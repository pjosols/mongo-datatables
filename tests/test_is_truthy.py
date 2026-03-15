import pytest
from mongo_datatables.utils import is_truthy


@pytest.mark.parametrize("value", [True, "true", "True", 1])
def test_truthy_values(value):
    assert is_truthy(value) is True


@pytest.mark.parametrize("value", [False, "false", "False", 0, None, "", "yes", "1", 2])
def test_falsy_values(value):
    assert is_truthy(value) is False
