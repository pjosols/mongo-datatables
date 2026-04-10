"""Tests for regex_utils: validate_regex and safe_regex ReDoS protection."""
import pytest

from mongo_datatables.datatables.query.regex_utils import validate_regex, safe_regex, _MAX_PATTERN_LEN, _MAX_NESTING_DEPTH


# ---------------------------------------------------------------------------
# validate_regex — safe patterns accepted
# ---------------------------------------------------------------------------

class TestValidateRegexAccepted:
    def test_simple_literal(self):
        assert validate_regex("hello") == "hello"

    def test_anchors(self):
        assert validate_regex("^start") == "^start"
        assert validate_regex("end$") == "end$"

    def test_character_class(self):
        assert validate_regex("[a-z]+") == "[a-z]+"

    def test_dot_star(self):
        assert validate_regex("foo.*bar") == "foo.*bar"

    def test_simple_group_no_quantifier(self):
        assert validate_regex("(foo|bar)") == "(foo|bar)"

    def test_escaped_paren_with_quantifier(self):
        # Escaped paren followed by quantifier is fine — not a group close
        assert validate_regex("\\)+") == "\\)+"

    def test_small_bounded_repetition(self):
        assert validate_regex("a{3}") == "a{3}"
        assert validate_regex("a{10,100}") == "a{10,100}"

    def test_nesting_at_max_depth(self):
        # depth 2: ((a))
        assert validate_regex("((a))") == "((a))"

    def test_empty_string(self):
        assert validate_regex("") == ""

    def test_max_length_boundary(self):
        pattern = "a" * _MAX_PATTERN_LEN
        assert validate_regex(pattern) == pattern


# ---------------------------------------------------------------------------
# validate_regex — ReDoS patterns rejected
# ---------------------------------------------------------------------------

class TestValidateRegexReDoSRejected:
    def test_group_plus_quantifier(self):
        # (a+)+ — classic ReDoS
        with pytest.raises(ValueError, match="unsafe"):
            validate_regex("(a+)+")

    def test_group_star_quantifier(self):
        # (a*)* — catastrophic backtracking
        with pytest.raises(ValueError, match="unsafe"):
            validate_regex("(a*)*")

    def test_alternation_group_plus(self):
        # (a|a)+ — overlapping alternation with quantifier
        with pytest.raises(ValueError, match="unsafe"):
            validate_regex("(a|a)+")

    def test_nested_group_plus(self):
        # ((a|b)+)+ — deeply nested
        with pytest.raises(ValueError, match="unsafe"):
            validate_regex("((a|b)+)+")

    def test_stacked_quantifiers(self):
        with pytest.raises(ValueError, match="unsafe"):
            validate_regex("a++")

    def test_stacked_star_quantifiers(self):
        with pytest.raises(ValueError, match="unsafe"):
            validate_regex("a**")

    def test_stacked_question_quantifiers(self):
        with pytest.raises(ValueError, match="unsafe"):
            validate_regex("a??")

    def test_very_large_bounded_repetition(self):
        with pytest.raises(ValueError, match="unsafe"):
            validate_regex("a{1000}")

    def test_lookahead(self):
        with pytest.raises(ValueError, match="unsafe"):
            validate_regex("foo(?=bar)")

    def test_lookbehind(self):
        with pytest.raises(ValueError, match="unsafe"):
            validate_regex("(?<=foo)bar")

    def test_named_group(self):
        with pytest.raises(ValueError, match="unsafe"):
            validate_regex("(?P<name>foo)")

    def test_inline_flag(self):
        with pytest.raises(ValueError, match="unsafe"):
            validate_regex("(?i)foo")


# ---------------------------------------------------------------------------
# validate_regex — length and nesting limits
# ---------------------------------------------------------------------------

class TestValidateRegexLimits:
    def test_exceeds_max_length(self):
        with pytest.raises(ValueError, match="length"):
            validate_regex("a" * (_MAX_PATTERN_LEN + 1))

    def test_exceeds_nesting_depth(self):
        # depth 3: (((a)))
        with pytest.raises(ValueError, match="nesting"):
            validate_regex("(((a)))")

    def test_nesting_depth_with_escaped_parens_not_counted(self):
        # \( is escaped, should not count toward depth
        assert validate_regex("\\(\\(\\(a\\)\\)\\)") is not None

    def test_invalid_regex_syntax(self):
        with pytest.raises(ValueError, match="Invalid regex"):
            validate_regex("[unclosed")


# ---------------------------------------------------------------------------
# safe_regex
# ---------------------------------------------------------------------------

class TestSafeRegex:
    def test_user_regex_false_escapes(self):
        result = safe_regex("a.b+c", is_user_regex=False)
        assert result == "a\\.b\\+c"

    def test_user_regex_true_validates_and_returns(self):
        result = safe_regex("^foo.*bar$", is_user_regex=True)
        assert result == "^foo.*bar$"

    def test_user_regex_true_rejects_redos(self):
        with pytest.raises(ValueError):
            safe_regex("(a+)+", is_user_regex=True)

    def test_user_regex_false_does_not_validate(self):
        # Even a ReDoS-looking string is just escaped when is_user_regex=False
        result = safe_regex("(a+)+", is_user_regex=False)
        assert result == "\\(a\\+\\)\\+"
