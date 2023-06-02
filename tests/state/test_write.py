from motion import Component

import pytest

C = Component("MyComponent")


@C.init_state
def setUp():
    return {"value": 0}


def test_update_state():
    c_instance = C()
    assert c_instance.read_state("value") == 0
    c_instance.update_state({"value": 1, "value2": 2})
    assert c_instance.read_state("value") == 1
    assert c_instance.read_state("value2") == 2

    # Test something that should fail
    with pytest.raises(TypeError):
        c_instance.update_state(1)

    with pytest.raises(TypeError):
        c_instance.update_state("Hello")

    # Should do nothing
    c_instance.update_state({})
