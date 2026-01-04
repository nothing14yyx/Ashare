from ashare.utils.convert import to_float


def test_to_float_handles_none_and_empty():
    assert to_float(None) is None
    assert to_float("") is None
    assert to_float("-") is None
    assert to_float("None") is None


def test_to_float_parses_numbers():
    assert to_float("1,234.5") == 1234.5
    assert to_float(10) == 10.0
    assert to_float(3.5) == 3.5


def test_to_float_nan_inf():
    assert to_float(float("nan")) is None
    assert to_float(float("inf")) is None
