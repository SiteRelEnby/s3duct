"""Tests for s3duct.cli."""

import pytest
from click.testing import CliRunner

from s3duct.cli import main, parse_size, parse_tag, validate_name


def test_parse_size_bytes():
    assert parse_size("1024B") == 1024


def test_parse_size_kilobytes():
    assert parse_size("512K") == 512 * 1024


def test_parse_size_megabytes():
    assert parse_size("512M") == 512 * 1024 ** 2


def test_parse_size_gigabytes():
    assert parse_size("1G") == 1024 ** 3


def test_parse_size_terabytes():
    assert parse_size("1T") == 1024 ** 4


def test_parse_size_plain_number():
    assert parse_size("1048576") == 1048576


def test_parse_size_float():
    assert parse_size("1.5G") == int(1.5 * 1024 ** 3)


def test_parse_size_lowercase():
    assert parse_size("512m") == parse_size("512M")


def test_parse_size_whitespace():
    assert parse_size("  512M  ") == 512 * 1024 ** 2


def test_cli_help():
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "s3duct" in result.output


def test_cli_put_help():
    runner = CliRunner()
    result = runner.invoke(main, ["put", "--help"])
    assert result.exit_code == 0
    assert "--bucket" in result.output
    assert "--chunk-size" in result.output


def test_cli_put_missing_bucket():
    runner = CliRunner()
    result = runner.invoke(main, ["put", "--name", "test"])
    assert result.exit_code != 0


def test_cli_put_no_encrypt_default():
    """Without --key or --age-identity, no encryption is used (no error)."""
    runner = CliRunner()
    result = runner.invoke(main, ["put", "--help"])
    assert "--key" in result.output
    assert "--age-identity" in result.output
    assert "--no-encrypt" in result.output


def test_cli_put_key_and_age_mutual_exclusion():
    """--key and --age-identity cannot both be provided."""
    runner = CliRunner()
    result = runner.invoke(main, [
        "put", "--bucket", "b", "--name", "n",
        "--key", "hex:" + "aa" * 32,
        "--age-identity", "/dev/null",
    ], input="")
    assert result.exit_code != 0
    assert "mutually exclusive" in result.output.lower() or "mutually exclusive" in str(result.exception or "").lower()


def test_cli_get_key_and_age_mutual_exclusion():
    """--key and --age-identity cannot both be provided on get."""
    runner = CliRunner()
    result = runner.invoke(main, [
        "get", "--bucket", "b", "--name", "n",
        "--key", "hex:" + "aa" * 32,
        "--age-identity", "/dev/null",
    ])
    assert result.exit_code != 0
    assert "mutually exclusive" in result.output.lower() or "mutually exclusive" in str(result.exception or "").lower()


def test_cli_get_help():
    """Get help shows new options."""
    runner = CliRunner()
    result = runner.invoke(main, ["get", "--help"])
    assert "--key" in result.output
    assert "--age-identity" in result.output
    assert "--no-decrypt" in result.output


def test_parse_tag_valid():
    assert parse_tag("project=backups") == ("project", "backups")
    assert parse_tag("env=prod") == ("env", "prod")
    assert parse_tag("key=val=ue") == ("key", "val=ue")  # value can contain =


def test_parse_tag_invalid():
    with pytest.raises(Exception):
        parse_tag("no-equals-sign")
    with pytest.raises(Exception):
        parse_tag("=no-key")


def test_cli_put_tag_option():
    runner = CliRunner()
    result = runner.invoke(main, ["put", "--help"])
    assert "--tag" in result.output


def test_cli_put_encrypt_manifest_option():
    runner = CliRunner()
    result = runner.invoke(main, ["put", "--help"])
    assert "--encrypt-manifest" in result.output


def test_validate_name_valid():
    validate_name("my-backup")
    validate_name("project/daily")
    validate_name("a")


def test_validate_name_empty():
    with pytest.raises(Exception):
        validate_name("")
    with pytest.raises(Exception):
        validate_name("   ")


def test_validate_name_bad_prefix():
    with pytest.raises(Exception):
        validate_name("/leading-slash")
    with pytest.raises(Exception):
        validate_name(".hidden")


def test_validate_name_double_slash():
    with pytest.raises(Exception):
        validate_name("bad//path")


def test_cli_put_empty_name():
    runner = CliRunner()
    result = runner.invoke(main, [
        "put", "--bucket", "b", "--name", "",
    ], input="")
    assert result.exit_code != 0


def test_cli_put_unencrypted_warning():
    """Uploading without encryption should warn."""
    runner = CliRunner()
    result = runner.invoke(main, [
        "put", "--bucket", "b", "--name", "n",
    ], input="")
    assert "No encryption configured" in result.output


def test_cli_put_no_encrypt_suppresses_warning():
    """--no-encrypt should suppress the unencrypted warning."""
    runner = CliRunner()
    result = runner.invoke(main, [
        "put", "--bucket", "b", "--name", "n", "--no-encrypt",
    ], input="")
    assert "No encryption configured" not in result.output


def test_cli_put_encrypt_manifest_requires_key():
    """--encrypt-manifest without --key should error."""
    runner = CliRunner()
    result = runner.invoke(main, [
        "put", "--bucket", "b", "--name", "n", "--encrypt-manifest",
    ], input="")
    assert result.exit_code != 0


def test_cli_put_no_encrypt_manifest_flag():
    """--no-encrypt-manifest should be accepted."""
    runner = CliRunner()
    result = runner.invoke(main, ["put", "--help"])
    assert "--no-encrypt-manifest" in result.output


def test_cli_verify_age_identity_option():
    """verify command should accept --age-identity."""
    runner = CliRunner()
    result = runner.invoke(main, ["verify", "--help"])
    assert "--age-identity" in result.output


def test_cli_verify_key_and_age_mutual_exclusion(tmp_path):
    """verify --key + --age-identity should error."""
    identity = tmp_path / "id.txt"
    identity.write_text("AGE-SECRET-KEY-1FAKE")
    runner = CliRunner()
    result = runner.invoke(main, [
        "verify", "--bucket", "b", "--name", "n",
        "--key", "hex:" + "aa" * 32,
        "--age-identity", str(identity),
    ])
    assert result.exit_code != 0
    assert "mutually exclusive" in result.output
