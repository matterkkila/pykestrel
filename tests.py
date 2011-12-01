#!/usr/bin/env python

"""
Unit tests for the kestrel library.
"""


import re
import unittest

import kestrel


class Test(unittest.TestCase):
    server = '10.106.126.12:22133'

    def setUp(self):
        self.queue = kestrel.Client(server=self.server)
        self.queue.flush('queue_test')

    def test_add(self):
        self.assertTrue(self.queue.add('queue_test', 'test', 100))

    def test_get(self):
        self.assertTrue(self.queue.add('queue_test', 'test'))
        self.assertEquals('test', self.queue.get('queue_test', timeout=10))

    def test_get(self):
        self.assertTrue(self.queue.add('queue_test', 'test'))
        self.assertEquals('test', self.queue.get('queue_test'))
        self.assertEquals(None, self.queue.get('queue_test'))

    def test_peek(self):
        self.assertTrue(self.queue.add('queue_test', 'item peek'))
        for i in range(5):
            self.assertEquals('item peek', self.queue.peek('queue_test', 5))

    def test_next_abort_finish(self):
        self.assertTrue(self.queue.add('queue_test', 'test'))
        self.assertEquals('test', self.queue.next('queue_test'))
        self.assertEquals(True, self.queue.abort('queue_test'))
        self.assertEquals('test', self.queue.next('queue_test'))
        self.assertEquals(True, self.queue.finish('queue_test'))
        self.assertEquals(None, self.queue.next('queue_test'))

    def test_next(self):
        for i in range(5):
            item = 'test %d' % (i)
            self.assertTrue(self.queue.add('queue_test', item))

        results = []
        while True:
            next = self.queue.next('queue_test', timeout=1)
            if next is None:
                break
            results.append(next)

        for i in range(5):
            self.assertTrue('test %d' % (i) in results)

        self.assertEquals(None, self.queue.get('queue_test'))

    def test_delete(self):
        self.assertTrue(self.queue.add('queue_test', 'test'))
        self.assertEquals(True, self.queue.delete('queue_test'))
        self.assertEquals(None, self.queue.get('queue_test'))

    def test_flush(self):
        self.assertTrue(self.queue.add('queue_test', 'test'))
        self.assertEquals(True, self.queue.flush('queue_test'))
        self.assertEquals(None, self.queue.get('queue_test'))

    def test_flush_all(self):
        self.assertTrue(self.queue.add('test_queue', 'test'))
        self.assertTrue(self.queue.add('test_alt', 'test_alt'))
        self.assertEquals(True, self.queue.flush_all())
        self.assertEquals(None, self.queue.get('test_alt'))
        self.assertEquals(None, self.queue.get('test_queue'))

    def test_reload(self):
        self.assertEquals(True, self.queue.reload())

    def test_stats(self):
        self.assertTrue(isinstance(self.queue.stats(), tuple))

    def test_raw_stats(self):
        self.assertTrue(len(self.queue.raw_stats()) > 0)
        self.assertTrue(self.queue.raw_stats(True).count('queue \'queue_test\'') > 0)

    def test_version(self):
        m = re.match(r'^[0-9]+\.[0-9]+.*', self.queue.version())
        self.assertTrue(m != None)

    def tearDown(self):
        self.queue.close()
        self.queue = None


if __name__ == '__main__':
    unittest.main()