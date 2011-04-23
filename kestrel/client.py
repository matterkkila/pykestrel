#!/usr/bin/env python

"""
A Kestrel client library.
"""

from collections import defaultdict
import re


import memcache


class Client(object):
    """Kestrel queue client."""

    def __init__(self, servers, queue):
        """Constructor.

        :param servers: The list of servers to connect to, really should only
            be one for a kestrel client;
        :type servers: list
        :param queue: The name of the key to work against
        :type queue: string

        """

        self.__memcache = KestrelMemcacheClient(servers=servers)
        self.queue = queue

    def add(self, data, expire=None):
        """Add a job onto the queue.

        WARNING:  You should only send strings through to the queue, if not
        the python-memcached library will serialize these objects and since
        kestrel ignores the flags supplied during a set operation, when the
        object is retrieved from the queue it will not be unserialized.

        :param data: The job itself
        :type data: mixed
        :param expire: The expiration time of the job, if a job doesn't get
            used in this amount of time, it will silently die away.
        :type expire: int
        :return: True/False
        :rtype: bool

        """

        if expire is None:
            expire = 0

        ret = self.__memcache.set(self.queue, data, expire)

        if ret == 0:
            return False

        return True

    def get(self, timeout=None):
        """Get a job off the queue. (unreliable)

        :param timeout: The time to wait for a job if none are on the queue
            when the initial request is made. (seconds)
        :type timeout: int
        :return: The job
        :rtype: mixed

        """

        cmd = '%s' % (self.queue)

        if timeout is not None:
            cmd = '%s/t=%d' % (cmd, timeout)

        return self.__memcache.get('%s' % (cmd))

    def next(self, timeout=None):
        """Marks the last job as compelete and gets the next one.

        :param timeout: The time to wait for a job if none are on the queue
            when the initial request is made. (seconds)
        :type timeout: int
        :return: The job
        :rtype: mixed

        """

        cmd = '%s/close' % (self.queue)

        if timeout is not None:
            cmd = '%s/t=%d' % (cmd, timeout)

        return self.__memcache.get('%s/open' % (cmd))

    def peek(self, timeout=None):
        """Copy a job from the queue, leaving the original in place.

        :param timeout: The time to wait for a job if none are on the queue
            when the initial request is made. (seconds)
        :type timeout: int
        :return: The job
        :rtype: mixed

        """

        cmd = '%s/peek' % (self.queue)

        if timeout is not None:
            cmd = '%s/t=%d' % (cmd, timeout)

        return self.__memcache.get(cmd)

    def abort(self):
        """Mark a job as incomplete, making it available to another client.

        :return: True on success
        :rtype: boolean

        """

        self.__memcache.get('%s/abort' % (self.queue))
        return True

    def finish(self):
        """Mark the last job read off the queue as complete on the server.

        :return: True on success
        :rtype: bool

        """

        self.__memcache.get('%s/close' % (self.queue))
        return True

    def delete(self):
        """Delete this queue from the kestrel server.

        :return: True on success, False on error
        :rtype: bool

        """

        ret = self.__memcache.delete(self.queue)

        if ret == 0:
            return False

        return True

    def close(self):
        """Force the client to disconnect from the server.

        :return: True
        :rtype: bool

        """

        self.__memcache.disconnect_all()
        return True

    def flush(self):
        """Clear out (remove all jobs) in the current queue.

        :return: True
        :rtype: bool

        """

        self.__memcache.flush(self.queue)
        return True

    def flush_all(self):
        """Clears out all jobs in all the queues on this kestrel server.

        :return: True
        :rtype: bool

        """

        self.__memcache.flush_all()
        return True

    def reload(self):
        """Forces the kestrel server to reload the config.

        :return: True
        :rtype: bool

        """

        self.__memcache.reload()
        return True

    def stats(self):
        """Get the stats from the server and parse the results into a python
           dict.

           {
               '127.0.0.1:22133': {
                   'stats': {
                       'cmd_get': 10,
                       ...
                   },
                   'queues': {
                       'queue_name': {
                           'age': 30,
                           ...
                       }
                   }
               }
           }
        """

        server = None
        _sstats = {}
        _qstats = {}

        for server, stats in self.raw_stats():
            server = server.split(' ', 1)[0]
            for name, stat in stats.iteritems():
                if not name.startswith('queue_'):
                    try:
                        _sstats[name] = long(stat)
                    except ValueError:
                        _sstats[name] = stat

        for name, stats in re.findall('queue \'(?P<name>.*?)\' \{(?P<stats>.*?)\}', self.raw_stats(True), re.DOTALL):
            _stats = {}
            for stat in [stat.strip() for stat in stats.split('\n')]:
                if stat.count('='):
                    (key, value) = stat.split('=')
                    _stats[key] = long(value)
            _qstats[name] = _stats

        return (server, dict([('server', _sstats), ('queues', _qstats)]))

    def raw_stats(self, pretty=None):
        """Get statistics in either the pretty (kestrel) format or the
        standard memcache format.

        :param pretty: Set to True to generate the stats in the kestrel/pretty
            format.
        :type pretty: bool
        :return: The stats text blob, or the structed format from the
            underlying memcache library
        :rtype: string

        """

        if pretty is True:
            return self.__memcache.pretty_stats()

        return self.__memcache.get_stats()

    def shutdown(self):
        """Shutdown the kestrel server gracefully.

        :return: None
        :rtype: None

        """

        return self.__memcache.shutdown()

    def version(self):
        """Get the version for the kestrel server.

        :return: The kestrel server version. e.g. 1.2.3
        :rtype: string

        """

        return self.__memcache.version()


class KestrelMemcacheClient(memcache.Client):
    """Kestrel Memcache Client.

    Since kestrel has a few commands that are not part of the memcached
    protocol we add functions to support them.

    Specifically: RELOAD, FLUSH, DUMP_STATS, DUMP_CONFIG, SHUTDOWN

    Also the memcache.Client doesn't have support for the VERSION command
    so we have added that function as well.

    """

    def reload(self):
        for s in self.servers:
            if not s.connect(): continue
            s.send_cmd('RELOAD')
            s.expect('OK')

    def flush(self, key):
        for s in self.servers:
            if not s.connect(): continue
            s.send_cmd('FLUSH %s' % (key))
            s.expect('OK')

    def pretty_stats(self):
        return self.__read_cmd('DUMP_STATS')

    def version(self):
        data = []
        for s in self.servers:
            if not s.connect(): continue
            s.send_cmd('VERSION')
            data.append(s.readline())

        return ('\n').join(data).split(' ', 1)[1]

    def shutdown(self):
        for s in self.servers:
            if not s.connect(): continue
            s.send_cmd('SHUTDOWN')

    def __read_cmd(self, cmd):
        data = []
        for s in self.servers:
            if not s.connect(): continue
            s.send_cmd(cmd)
            data.append(self.__read_string(s))

        return ('\n').join(data)

    def __read_string(self, s):
        data = []
        while True:
            line = s.readline()
            if not line or line.strip() == 'END': break
            data.append(line)

        return ('\n').join(data)
