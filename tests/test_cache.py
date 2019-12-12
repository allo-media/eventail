from unittest import TestCase

from eventail.tmp_store import STDataStore


class TestSTDataStore(TestCase):

    def setUp(self):
        self.tmp_store = STDataStore.sentinel_backend(["redis:6379"], "", 0, None, "test", 2)

    def test_tmp_store(self):
        self.tmp_store.set("key1", "value1")
        self.tmp_store.set("key2", "value2")

        self.assertEqual(self.tmp_store.pop("key2"), "value2")
        self.assertIsNone(self.tmp_store.pop("key2"))
        self.assertIsNone(self.tmp_store.mpop("key1", "key2"))
        self.assertEqual(self.tmp_store.peek("key1"), "value1")
        self.assertIsNone(self.tmp_store.peek("key2"))
        self.tmp_store.set("key2", "value2")
        self.assertEqual(self.tmp_store.mpop("key1", "key2"), ["value1", "value2"])
        self.assertIsNone(self.tmp_store.peek("key1"))
        self.assertIsNone(self.tmp_store.peek("key2"))
        self.tmp_store.mset(dict(key1=1, key2=2))
        self.assertEqual(self.tmp_store.pop("key1"), 1)
        self.assertEqual(self.tmp_store.pop("key2"), 2)

