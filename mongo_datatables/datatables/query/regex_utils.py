"""Regex pattern validation to prevent ReDoS and NoSQL injection."""

import re
from typing import Optional

# Maximum allowed length for a user-supplied regex pattern
_MAX_PATTERN_LEN = 200

# Maximum allowed parenthesis nesting depth
_MAX_NESTING_DEPTH = 2

# Reject patterns that can cause catastrophic backtracking:
# - unescaped closing paren followed by any quantifier: (...)+ (...)* (...){n}
# - lookahead/behind, named groups, inline flags
# - stacked quantifiers: a++ a**
# - very large bounded repetition
_UNSAFE_PATTERNS = re.compile(
    r"""
    (?<!\\)\)[\*\+\?\{]  |  # group close + quantifier (including bounded): (...)+ (...){n}
    \(\?[Ppi<>!=]        |  # lookahead/behind, named groups, inline flags
    (?<!\\)[\*\+\?]{2,}  |  # stacked quantifiers: a++ a**
    \{[0-9]{4,}              # very large bounded repetition
    """,
    re.VERBOSE,
)


def validate_regex(pattern: str) -> Optional[str]:
    """Validate a user-supplied regex pattern for safety.

    Rejects patterns that can cause catastrophic backtracking in Python's
    re engine. Bans any quantifier (including bounded `{n}`) on groups,
    covering (a+)+, (a+){2}, and all similar constructs, as well as
    lookahead/behind, stacked quantifiers, and very large bounded repetitions.
    Also limits nesting depth.

    pattern: The raw regex string from user input.
    Returns the pattern unchanged if valid.
    Raises ValueError with a descriptive message if the pattern is rejected.
    """
    if len(pattern) > _MAX_PATTERN_LEN:
        raise ValueError(
            f"Regex pattern exceeds maximum length of {_MAX_PATTERN_LEN} characters"
        )

    if _UNSAFE_PATTERNS.search(pattern):
        raise ValueError("Regex pattern contains unsafe constructs")

    if _nesting_depth(pattern) > _MAX_NESTING_DEPTH:
        raise ValueError(f"Regex pattern nesting depth exceeds maximum of {_MAX_NESTING_DEPTH}")

    try:
        re.compile(pattern)
    except re.error as exc:
        raise ValueError(f"Invalid regex pattern: {exc}") from exc

    return pattern


def _nesting_depth(pattern: str) -> int:
    """Return the maximum parenthesis nesting depth in pattern.

    Correctly handles backslash-escaped characters.

    pattern: A regex string.
    Returns the maximum depth as an integer.
    """
    depth = max_depth = 0
    escaped = False
    for ch in pattern:
        if escaped:
            escaped = False
            continue
        if ch == "\\":
            escaped = True
        elif ch == "(":
            depth += 1
            max_depth = max(max_depth, depth)
        elif ch == ")":
            depth = max(0, depth - 1)
    return max_depth


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
