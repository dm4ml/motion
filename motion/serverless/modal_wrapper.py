from modal import Stub

stub = Stub()


@stub.cls
class ModalWrapper:
    def __enter__(self):
        pass
