"""Security tests — XSS payloads, oversized inputs, malformed data."""

import pytest

from datanika.services.naming import validate_name


class TestXSSPayloads:
    @pytest.mark.parametrize("payload", [
        "<script>alert(1)</script>",
        "<img src=x onerror=alert(1)>",
        "javascript:alert(1)",
        "<svg onload=alert(1)>",
        "'-alert(1)-'",
        '"><script>alert(document.cookie)</script>',
    ])
    def test_xss_in_name_rejected(self, payload):
        with pytest.raises(ValueError, match="alphanumeric"):
            validate_name(payload, "Entity")


class TestOversizedInputs:
    def test_very_long_name_accepted_if_alphanumeric(self):
        # Long but valid — naming doesn't enforce length
        long_name = "A" * 1000
        validate_name(long_name, "Entity")  # no raise

    def test_empty_name_rejected(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            validate_name("", "Entity")

    def test_whitespace_only_rejected(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            validate_name("   ", "Entity")


class TestSpecialCharacters:
    @pytest.mark.parametrize("char", [
        "\x00",  # null byte
        "\n",    # newline
        "\r",    # carriage return
        "\t",    # tab
        "\x1b",  # escape
        "\x7f",  # delete
    ])
    def test_control_characters_rejected(self, char):
        with pytest.raises(ValueError, match="alphanumeric"):
            validate_name(f"test{char}name", "Entity")

    def test_unicode_emoji_rejected(self):
        with pytest.raises(ValueError, match="alphanumeric"):
            validate_name("test 🎉 name", "Entity")

    @pytest.mark.parametrize("char", [
        "'", '"', "\\", "/", "|", "&", ";", "$", "`", "(", ")", "{", "}", "[", "]",
    ])
    def test_shell_metacharacters_rejected(self, char):
        with pytest.raises(ValueError, match="alphanumeric"):
            validate_name(f"test{char}name", "Entity")
