from copy import deepcopy
from unittest import TestCase

from eventail.batch_processor import Batch, RawEvent


class MockService:

    def call_later(self, timeout, callback):
        self.timeout = timeout
        self.callback = callback
        return 1

    def cancel_timer(self, timer):
        assert timer == 1


# MagicMock from unittest does not make a deep copy of its arguments
# so, they are lost.
class MockCallback:
    def __init__(self, ret):
        self.ret = ret
        self.args = None
        self.kwargs = None

    def __call__(self, *args, **kwargs):
        self.args = deepcopy(args)
        self.kwargs = deepcopy(kwargs)

    def assert_not_called(self):
        assert self.args is None and self.kwargs is None, "mock has been called"

    def assert_called_with(self, *args, **kwargs):
        assert self.args == args and self.kwargs == kwargs, f"got {self.args} and {self.kwargs}"


class TestBatch(TestCase):

    def test_init(self):
        service = MockService()
        cb = MockCallback(None)
        Batch(service, 3, 3, cb)

    def test_push(self):
        service = MockService()
        cb = MockCallback(None)
        batch = Batch(service, 3, 3, cb)
        batch.push({"msg": 1}, "a", {})
        cb.assert_not_called()
        batch.push({"msg": 2}, "b", {})
        cb.assert_not_called()
        batch.push({"msg": 3}, "c", {})
        cb.assert_called_with([RawEvent({"msg": 1}, "a", {}), RawEvent({"msg": 2}, "b", {}), RawEvent({"msg": 3}, "c", {})])

    def test_expired(self):
        service = MockService()
        cb = MockCallback(None)
        batch = Batch(service, 4, 3, cb)
        batch.push({"msg": 1}, "a", {})
        cb.assert_not_called()
        batch.push({"msg": 2}, "b", {})
        cb.assert_not_called()
        batch.push({"msg": 3}, "c", {})
        cb.assert_not_called()
        batch.expire()
        cb.assert_called_with([RawEvent({"msg": 1}, "a", {}), RawEvent({"msg": 2}, "b", {}), RawEvent({"msg": 3}, "c", {})])
