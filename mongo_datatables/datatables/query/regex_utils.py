"""Regex pattern validation to prevent ReDoS and NoSQL injection."""

import re
from typing import Optional

# Maximum allowed length for a user-supplied regex pattern
_MAX_PATTERN_LEN = 200

# Patterns that indicate catastrophic backtracking risk or injection attempts
_DANGEROUS_PATTERNS = re.compile(
    r"""
    (\(.*\*){2,}        |   # nested quantifiers: (a*)* style
    (\(.*\+){2,}        |   # nested quantifiers: (a+)+ style
    (\.\*){3,}          |   # excessive .* repetition
    \(\?[Ppi]           |   # inline flags or named groups that alter engine behaviour
    \(\?[<>!]           |   # lookahead / lookbehind (can be expensive)
    \{[0-9]{4,}\}           # very large fixed repetition counts
    """,
    re.VERBOSE,
)


def validate_regex(pattern: str) -> Optional[str]:
    """Validate a user-supplied regex pattern for safety.

    Checks length, compilability, and known ReDoS / injection constructs.

    pattern: The raw regex string from user input.
    Returns the pattern unchanged if valid.
    Raises ValueError with a descriptive message if the pattern is rejected.
    """
    if len(pattern) > _MAX_PATTERN_LEN:
        raise ValueError(
            f"Regex pattern exceeds maximum length of {_MAX_PATTERN_LEN} characters"
        )

    if _DANGEROUS_PATTERNS.search(pattern):
        raise ValueError("Regex pattern contains potentially unsafe constructs")

    try:
        re.compile(pattern)
    except re.error as exc:
        raise ValueError(f"Invalid regex pattern: {exc}") from exc

    return pattern


def safe_regex(pattern: str, is_user_regex: bool) -> str:
    """Return a safe regex pattern, escaping if not user-supplied.

    pattern: The input string.
    is_user_regex: If True, validate as a user regex; otherwise escape it.
    Returns a safe pattern string.
    Raises ValueError if is_user_regex is True and the pattern is unsafe.
    """
    if is_user_regex:
        return validate_regex(pattern)
    return re.escape(pattern)
