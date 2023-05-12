from motion import Component
import pytest
import pydantic


class PydanticModel(pydantic.BaseModel):
    number: int


def test_pydantic_infer():
    Counter = Component("Counter")

    @Counter.infer("number")
    def noop(state, value: PydanticModel):
        return value.number

    c = Counter()
    assert c.run(number={"number": 1}) == 1
    with pytest.raises(ValueError):
        c.run(number=2)


def test_wrong_args():
    c = Component("Counter")

    with pytest.raises(ValueError):

        @c.infer("number")
        def noop(state, value, something_else):
            return value.number

    with pytest.raises(ValueError):

        @c.fit("number")
        def noop(state, something_else):
            return 1
