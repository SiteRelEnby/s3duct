"""Tests for s3duct.encryption."""

import os
import subprocess
import pytest

from s3duct.encryption import (
    age_available, age_encrypt_file, age_decrypt_file, get_recipient_from_identity,
    age_encrypt_manifest, age_decrypt_manifest,
    aes_encrypt_file, aes_decrypt_file, parse_key,
)


skip_no_age = pytest.mark.skipif(not age_available(), reason="age CLI not installed")


def test_age_available_returns_bool():
    result = age_available()
    assert isinstance(result, bool)


@skip_no_age
def test_encrypt_decrypt_roundtrip(tmp_path):
    # Generate a keypair
    keygen = subprocess.run(
        ["age-keygen"], capture_output=True, text=True
    )
    assert keygen.returncode == 0
    identity_file = tmp_path / "key.txt"
    identity_file.write_text(keygen.stdout)

    # Extract recipient
    recipient = get_recipient_from_identity(str(identity_file))
    assert recipient.startswith("age1")

    # Create test file
    plaintext = b"secret data for encryption test"
    src = tmp_path / "plain.bin"
    src.write_bytes(plaintext)

    # Encrypt
    enc = tmp_path / "encrypted.age"
    age_encrypt_file(src, enc, recipient)
    assert enc.exists()
    assert enc.read_bytes() != plaintext

    # Decrypt
    dec = tmp_path / "decrypted.bin"
    age_decrypt_file(enc, dec, str(identity_file))
    assert dec.read_bytes() == plaintext


@skip_no_age
def test_encrypt_bad_recipient(tmp_path):
    src = tmp_path / "plain.bin"
    src.write_bytes(b"data")
    dest = tmp_path / "out.age"
    with pytest.raises(RuntimeError, match="age encrypt failed"):
        age_encrypt_file(src, dest, "not-a-valid-recipient")


@skip_no_age
def test_get_recipient_from_identity(tmp_path):
    keygen = subprocess.run(["age-keygen"], capture_output=True, text=True)
    identity_file = tmp_path / "key.txt"
    identity_file.write_text(keygen.stdout)
    recipient = get_recipient_from_identity(str(identity_file))
    assert recipient.startswith("age1")
    assert len(recipient) > 10


def test_age_encrypt_file_mocked(tmp_path, monkeypatch):
    """Test age_encrypt_file logic when age is not available."""
    src = tmp_path / "plain.bin"
    src.write_bytes(b"data")
    dest = tmp_path / "out.age"

    def mock_run(cmd, **kwargs):
        dest.write_bytes(b"encrypted")
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr("s3duct.encryption.subprocess.run", mock_run)
    age_encrypt_file(src, dest, "fake-recipient")
    assert dest.exists()


def test_age_decrypt_file_mocked_failure(tmp_path, monkeypatch):
    """Test age_decrypt_file error handling."""
    src = tmp_path / "enc.age"
    src.write_bytes(b"encrypted")
    dest = tmp_path / "dec.bin"

    def mock_run(cmd, **kwargs):
        return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="bad key")

    monkeypatch.setattr("s3duct.encryption.subprocess.run", mock_run)
    with pytest.raises(RuntimeError, match="age decrypt failed"):
        age_decrypt_file(src, dest, "/fake/identity")


@skip_no_age
def test_age_encrypt_decrypt_manifest(tmp_path):
    """Roundtrip manifest encryption/decryption with age via stdin/stdout."""
    identity = tmp_path / "identity.txt"
    subprocess.run(["age-keygen", "-o", str(identity)], check=True,
                   capture_output=True)
    recipient = get_recipient_from_identity(str(identity))

    plaintext = b'{"version": 1, "name": "test", "chunks": []}'
    encrypted = age_encrypt_manifest(plaintext, recipient)
    assert encrypted != plaintext
    assert len(encrypted) > 0

    decrypted = age_decrypt_manifest(encrypted, str(identity))
    assert decrypted == plaintext


# ---------------------------------------------------------------------------
# AES-256-GCM tests
# ---------------------------------------------------------------------------

def test_aes_encrypt_decrypt_roundtrip(tmp_path):
    key = os.urandom(32)
    plaintext = b"hello AES-256-GCM encryption"
    src = tmp_path / "plain.bin"
    src.write_bytes(plaintext)

    enc = tmp_path / "encrypted.enc"
    aes_encrypt_file(src, enc, key)
    assert enc.exists()
    assert enc.read_bytes() != plaintext
    # File should be nonce(12) + ciphertext + tag(16) = 12 + len + 16
    assert len(enc.read_bytes()) == 12 + len(plaintext) + 16

    dec = tmp_path / "decrypted.bin"
    aes_decrypt_file(enc, dec, key)
    assert dec.read_bytes() == plaintext


def test_aes_decrypt_wrong_key(tmp_path):
    key = os.urandom(32)
    wrong_key = os.urandom(32)
    src = tmp_path / "plain.bin"
    src.write_bytes(b"secret")

    enc = tmp_path / "encrypted.enc"
    aes_encrypt_file(src, enc, key)

    dec = tmp_path / "decrypted.bin"
    with pytest.raises(Exception):
        aes_decrypt_file(enc, dec, wrong_key)


def test_aes_decrypt_truncated(tmp_path):
    src = tmp_path / "short.enc"
    src.write_bytes(b"short")  # less than 12 bytes nonce
    dec = tmp_path / "dec.bin"
    with pytest.raises(RuntimeError, match="too short"):
        aes_decrypt_file(src, dec, os.urandom(32))


def test_aes_encrypt_large_chunk(tmp_path):
    """Test encrypting a larger chunk (simulating real chunk sizes)."""
    key = os.urandom(32)
    plaintext = os.urandom(1024 * 1024)  # 1MB
    src = tmp_path / "big.bin"
    src.write_bytes(plaintext)

    enc = tmp_path / "big.enc"
    aes_encrypt_file(src, enc, key)

    dec = tmp_path / "big.dec"
    aes_decrypt_file(enc, dec, key)
    assert dec.read_bytes() == plaintext


# ---------------------------------------------------------------------------
# parse_key tests
# ---------------------------------------------------------------------------

def test_parse_key_hex():
    hex_key = "aa" * 32  # 64 hex chars = 32 bytes
    key = parse_key(f"hex:{hex_key}")
    assert len(key) == 32
    assert key == bytes.fromhex(hex_key)


def test_parse_key_file(tmp_path):
    key_bytes = os.urandom(32)
    key_file = tmp_path / "key.bin"
    key_file.write_bytes(key_bytes)
    key = parse_key(f"file:{key_file}")
    assert key == key_bytes


def test_parse_key_env(monkeypatch):
    hex_key = "bb" * 32
    monkeypatch.setenv("TEST_S3DUCT_KEY", hex_key)
    key = parse_key("env:TEST_S3DUCT_KEY")
    assert key == bytes.fromhex(hex_key)


def test_parse_key_env_missing():
    with pytest.raises(ValueError, match="not set"):
        parse_key("env:NONEXISTENT_S3DUCT_KEY_12345")


def test_parse_key_invalid_format():
    with pytest.raises(ValueError, match="Invalid key format"):
        parse_key("plaintext-key")


def test_parse_key_wrong_length():
    with pytest.raises(ValueError, match="32 bytes"):
        parse_key("hex:aabb")  # only 2 bytes
