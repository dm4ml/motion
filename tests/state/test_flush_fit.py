from motion import Component

Counter = Component("Counter")


@Counter.init_state
def setUp():
    return {"value": 0, "values": []}


@Counter.infer("number")
def noop(state, value):
    return value


# Arbitrarily large batch
@Counter.fit("number")
def increment(state, infer_result):
    values = state["values"]
    values.append(infer_result)

    if len(values) == 10:
        return {"values": [], "value": sum(values) + state["value"]}

    return {"values": values}


def test_flush_instance():
    counter = Counter()

    init_value = counter.read_state("value")

    for i in range(10):
        counter.run("number", kwargs={"value": i})

    # Flush instance
    counter.flush_fit("number")

    # Assert new state is different from old state
    assert counter.get_version() > 1
    assert counter.read_state("value") != init_value
    assert counter.read_state("value") == sum(range(10))

    # If I flush again, nothing should happen since
    # there are no elements in the fit queue
    counter.flush_fit("number")
    assert counter.get_version() > 1

    counter.shutdown()


def test_fit_daemon():
    counter = Counter()

    for i in range(10):
        counter.run("number", kwargs={"value": i})

    # Flush instance
    counter.flush_fit("number")

    # Assert new state is different from old state
    assert counter.get_version() > 1

    # Don't shutdown
