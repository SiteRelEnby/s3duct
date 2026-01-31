"""Chunk encryption and decryption (AES-256-GCM and age)."""

import os
import shutil
import subprocess
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

AES_NONCE_SIZE = 12  # 96 bits, recommended for GCM


# ---------------------------------------------------------------------------
# AES-256-GCM
# ---------------------------------------------------------------------------

def parse_key(key_spec: str) -> bytes:
    """Parse a key specification into 32 raw bytes.

    Formats:
        hex:AABBCC...   — 64 hex characters (32 bytes)
        file:/path       — raw 32-byte key file
        env:VAR_NAME     — environment variable containing hex key
    """
    if key_spec.startswith("hex:"):
        raw = bytes.fromhex(key_spec[4:])
    elif key_spec.startswith("file:"):
        raw = Path(key_spec[5:]).read_bytes()
    elif key_spec.startswith("env:"):
        var = key_spec[4:]
        value = os.environ.get(var)
        if value is None:
            raise ValueError(f"Environment variable {var!r} is not set")
        raw = bytes.fromhex(value)
    else:
        raise ValueError(
            f"Invalid key format: {key_spec!r}. "
            "Use hex:..., file:..., or env:..."
        )
    if len(raw) != 32:
        raise ValueError(f"Key must be exactly 32 bytes (got {len(raw)})")
    return raw


def aes_encrypt_file(source: Path, dest: Path, key: bytes) -> None:
    """Encrypt a file with AES-256-GCM.

    Output format: [12-byte nonce][ciphertext || 16-byte GCM tag]
    """
    plaintext = source.read_bytes()
    nonce = os.urandom(AES_NONCE_SIZE)
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, None)
    dest.write_bytes(nonce + ciphertext)


def aes_decrypt_file(source: Path, dest: Path, key: bytes) -> None:
    """Decrypt an AES-256-GCM encrypted file."""
    data = source.read_bytes()
    if len(data) < AES_NONCE_SIZE:
        raise RuntimeError("Encrypted file too short (missing nonce)")
    nonce = data[:AES_NONCE_SIZE]
    ciphertext = data[AES_NONCE_SIZE:]
    plaintext = AESGCM(key).decrypt(nonce, ciphertext, None)
    dest.write_bytes(plaintext)


def aes_encrypt_manifest(plaintext: bytes, key: bytes) -> bytes:
    """Encrypt manifest bytes with AES-256-GCM. Returns nonce + ciphertext."""
    nonce = os.urandom(AES_NONCE_SIZE)
    return nonce + AESGCM(key).encrypt(nonce, plaintext, None)


def aes_decrypt_manifest(data: bytes, key: bytes) -> bytes:
    """Decrypt manifest bytes. Raises on bad key (early validation)."""
    if len(data) < AES_NONCE_SIZE:
        raise RuntimeError("Encrypted manifest too short")
    nonce = data[:AES_NONCE_SIZE]
    return AESGCM(key).decrypt(nonce, data[AES_NONCE_SIZE:], None)


# ---------------------------------------------------------------------------
# Age (asymmetric)
# ---------------------------------------------------------------------------

def age_encrypt_manifest(plaintext: bytes, recipient: str) -> bytes:
    """Encrypt manifest bytes with age. Pipes via stdin/stdout."""
    try:
        result = subprocess.run(
            ["age", "-r", recipient],
            input=plaintext, capture_output=True,
        )
    except FileNotFoundError:
        raise RuntimeError("age not found. Install age: https://github.com/FiloSottile/age")
    if result.returncode != 0:
        raise RuntimeError(f"age encrypt failed: {result.stderr.decode().strip()}")
    return result.stdout


def age_decrypt_manifest(data: bytes, identity: str) -> bytes:
    """Decrypt manifest bytes with age. Pipes via stdin/stdout."""
    try:
        result = subprocess.run(
            ["age", "-d", "-i", identity],
            input=data, capture_output=True,
        )
    except FileNotFoundError:
        raise RuntimeError("age not found. Install age: https://github.com/FiloSottile/age")
    if result.returncode != 0:
        raise RuntimeError(f"age decrypt failed: {result.stderr.decode().strip()}")
    return result.stdout


def age_available() -> bool:
    """Check if the age CLI is installed."""
    return shutil.which("age") is not None


def _run_age(cmd: list[str], description: str) -> subprocess.CompletedProcess:
    """Run an age/age-keygen command with clear error messages."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
    except FileNotFoundError:
        raise RuntimeError(
            f"{cmd[0]} not found. Install age: https://github.com/FiloSottile/age"
        )
    if result.returncode != 0:
        raise RuntimeError(f"{description}: {result.stderr.strip()}")
    return result


def age_encrypt_file(source: Path, dest: Path, recipient: str) -> None:
    """Encrypt a file using age with a recipient public key."""
    _run_age(
        ["age", "-r", recipient, "-o", str(dest), str(source)],
        "age encrypt failed",
    )


def age_decrypt_file(source: Path, dest: Path, identity: str) -> None:
    """Decrypt a file using age with an identity (private key) file."""
    _run_age(
        ["age", "-d", "-i", identity, "-o", str(dest), str(source)],
        "age decrypt failed",
    )


def get_recipient_from_identity(identity_path: str) -> str:
    """Extract the public key (recipient) from an age identity file."""
    result = _run_age(
        ["age-keygen", "-y", identity_path],
        "age-keygen failed",
    )
    return result.stdout.strip()
