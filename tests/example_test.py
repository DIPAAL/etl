from etl.helper_functions import wrap_with_timings


def not_included():
    return True


def test_example_pass():
    assert not_included()


def test_wraps_function():
    operand1 = 2
    operand2 = 2
    expected = operand1 + operand2
    result = wrap_with_timings('test_function', lambda: operand1 + operand2)

    assert expected == result
