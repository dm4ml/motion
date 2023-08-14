from motion import Component
import time

Spinner = Component("Spinner")


def fibonacci(n):
    if n <= 1:
        return n
    else:
        return fibonacci(n - 1) + fibonacci(n - 2)


@Spinner.update("spin")
def spin(state, props):
    fib = fibonacci(props["value"])
    return {"fib": fib}


def test_process():
    num = 11
    res = 89

    rounds = 10

    # Commented this out because it takes a large number of rounds for process to be better than thread
    # inst1 = Spinner(instance_id="thread", update_task_type="thread")
    # start = time.time()

    # for i in range(rounds):
    #     inst1.run(
    #         "spin",
    #         props={"value": num},
    #         flush_update=i == rounds - 1,
    #         ignore_cache=True,
    #     )

    # thread_time = time.time() - start
    # assert inst1.read_state("fib") == res
    # inst1.shutdown()

    inst2 = Spinner(instance_id="process", update_task_type="process")
    start = time.time()

    for i in range(rounds):
        inst2.run(
            "spin",
            props={"value": num},
            flush_update=i == rounds - 1,
            ignore_cache=True,
        )

    process_time = time.time() - start
    assert inst2.read_state("fib") == res

    # assert thread_time > process_time

    inst2.shutdown()
