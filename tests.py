#!/usr/bin/env python

"""
Unit tests for the kestrel library.
"""


import unittest

import kestrel


class Test(unittest.TestCase):
    servers = ['127.0.0.1:22133']

    def setUp(self):
        self.queue = kestrel.Client(servers=self.servers, queue='test')
        self.queue.flush()

    def test_add(self):
        self.assertTrue(self.queue.add('test', 100))

    def test_get(self):
        self.assertTrue(self.queue.add('test'))
        self.assertEquals('test', self.queue.get(timeout=10))

    def test_get(self):
        self.assertTrue(self.queue.add('test'))
        self.assertEquals('test', self.queue.get())
        self.assertEquals(None, self.queue.get())

    def test_peek(self):
        self.assertTrue(self.queue.add('item peek'))
        for i in range(5):
            self.assertEquals('item peek', self.queue.peek(5))

    def test_next_abort_finish(self):
        self.assertTrue(self.queue.add('test'))
        self.assertEquals('test', self.queue.next())
        self.assertEquals(True, self.queue.abort())
        self.assertEquals('test', self.queue.next())
        self.assertEquals(True, self.queue.finish())
        self.assertEquals(None, self.queue.next())

    def test_next(self):
        for i in range(5):
            item = 'test %d' % (i)
            self.assertTrue(self.queue.add(item))

        results = []
        while True:
            next = self.queue.next(timeout=1)
            if next is None:
                break
            results.append(next)

        for i in range(5):
            self.assertTrue('test %d' % (i) in results)

        self.assertEquals(None, self.queue.get())

    def test_delete(self):
        self.assertTrue(self.queue.add('test'))
        self.assertEquals(True, self.queue.delete())
        self.assertEquals(None, self.queue.get())

    def test_flush(self):
        self.assertTrue(self.queue.add('test'))
        self.assertEquals(True, self.queue.flush())
        self.assertEquals(None, self.queue.get())

    def test_flush_all(self):
        alt_queue = kestrel.Client(servers=self.servers, queue='test_alt')

        self.assertTrue(self.queue.add('test'))
        self.assertTrue(alt_queue.add('test_alt'))
        self.assertEquals(True, self.queue.flush_all())
        self.assertEquals(None, self.queue.get())
        self.assertEquals(None, alt_queue.get())

        alt_queue.delete()
        alt_queue.close()

    def test_reload(self):
        self.assertEquals(True, self.queue.reload())

    def test_stats(self):
        self.assertTrue(isinstance(self.queue.stats(), tuple))

    def test_raw_stats(self):
        self.assertTrue(len(self.queue.raw_stats()) > 0)
        self.assertTrue(self.queue.raw_stats(True).count('queue \'test\'') > 0)

    def test_version(self):
        self.assertTrue(self.queue.version().startswith('1.'))

    def tearDown(self):
        self.queue.close()
        self.queue = None


if __name__ == '__main__':
    unittest.main()