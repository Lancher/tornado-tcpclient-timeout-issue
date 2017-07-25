from __future__ import absolute_import, division, print_function

from contextlib import closing
import os
import socket
import time

from tornado.concurrent import Future
from tornado.netutil import bind_sockets, Resolver
from tornado.queues import Queue
# from tornado.tcpclient import TCPClient, _Connector
from connector import _Connector
from tornado.tcpserver import TCPServer
from tornado.testing import AsyncTestCase, gen_test
from tornado.test.util import skipIfNoIPv6, unittest, refusing_port, skipIfNonUnix
from tornado.gen import TimeoutError


# Fake address families for testing.  Used in place of AF_INET
# and AF_INET6 because some installations do not have AF_INET6.
AF1, AF2 = 1, 2


class ConnectorTest(AsyncTestCase):
    class FakeStream(object):
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    def setUp(self):
        super(ConnectorTest, self).setUp()
        self.connect_futures = {}
        self.streams = {}
        self.addrinfo = [(AF1, 'a'), (AF1, 'b'),
                         (AF2, 'c'), (AF2, 'd')]

    def tearDown(self):
        # Unless explicitly checked (and popped) in the test, we shouldn't
        # be closing any streams
        for stream in self.streams.values():
            self.assertFalse(stream.closed)
        super(ConnectorTest, self).tearDown()

    def assert_connector_streams_closed(self, conn):
        for stream in conn.streams:
            self.assertTrue(stream.closed)

    def create_stream(self, af, addr):
        stream = ConnectorTest.FakeStream()
        self.streams[addr] = stream
        future = Future()
        self.connect_futures[(af, addr)] = future
        return stream, future

    def assert_pending(self, *keys):
        self.assertEqual(sorted(self.connect_futures.keys()), sorted(keys))

    def resolve_connect(self, af, addr, success):
        future = self.connect_futures.pop((af, addr))
        if success:
            future.set_result(self.streams[addr])
        else:
            self.streams.pop(addr)
            future.set_exception(IOError())

    def start_connect(self, addrinfo):
        conn = _Connector(addrinfo, self.create_stream)
        # Give it a huge timeout; we'll trigger timeouts manually.
        future = conn.start(3600, time.time()+3600)
        return conn, future

    def test_immediate_success(self):
        conn, future = self.start_connect(self.addrinfo)
        self.assertEqual(list(self.connect_futures.keys()),
                         [(AF1, 'a')])
        self.resolve_connect(AF1, 'a', True)
        self.assertEqual(future.result(), (AF1, 'a', self.streams['a']))

    def test_immediate_failure(self):
        # Fail with just one address.
        conn, future = self.start_connect([(AF1, 'a')])
        self.assert_pending((AF1, 'a'))
        self.resolve_connect(AF1, 'a', False)
        self.assertRaises(IOError, future.result)

    def test_one_family_second_try(self):
        conn, future = self.start_connect([(AF1, 'a'), (AF1, 'b')])
        self.assert_pending((AF1, 'a'))
        self.resolve_connect(AF1, 'a', False)
        self.assert_pending((AF1, 'b'))
        self.resolve_connect(AF1, 'b', True)
        self.assertEqual(future.result(), (AF1, 'b', self.streams['b']))

    def test_one_family_second_try_failure(self):
        conn, future = self.start_connect([(AF1, 'a'), (AF1, 'b')])
        self.assert_pending((AF1, 'a'))
        self.resolve_connect(AF1, 'a', False)
        self.assert_pending((AF1, 'b'))
        self.resolve_connect(AF1, 'b', False)
        self.assertRaises(IOError, future.result)

    def test_one_family_second_try_timeout(self):
        conn, future = self.start_connect([(AF1, 'a'), (AF1, 'b')])
        self.assert_pending((AF1, 'a'))
        # trigger the timeout while the first lookup is pending;
        # nothing happens.
        conn.on_timeout()
        self.assert_pending((AF1, 'a'))
        self.resolve_connect(AF1, 'a', False)
        self.assert_pending((AF1, 'b'))
        self.resolve_connect(AF1, 'b', True)
        self.assertEqual(future.result(), (AF1, 'b', self.streams['b']))

    def test_two_families_immediate_failure(self):
        conn, future = self.start_connect(self.addrinfo)
        self.assert_pending((AF1, 'a'))
        self.resolve_connect(AF1, 'a', False)
        self.assert_pending((AF1, 'b'), (AF2, 'c'))
        self.resolve_connect(AF1, 'b', False)
        self.resolve_connect(AF2, 'c', True)
        self.assertEqual(future.result(), (AF2, 'c', self.streams['c']))

    def test_two_families_timeout(self):
        conn, future = self.start_connect(self.addrinfo)
        self.assert_pending((AF1, 'a'))
        conn.on_timeout()
        self.assert_pending((AF1, 'a'), (AF2, 'c'))
        self.resolve_connect(AF2, 'c', True)
        self.assertEqual(future.result(), (AF2, 'c', self.streams['c']))
        # resolving 'a' after the connection has completed doesn't start 'b'
        self.resolve_connect(AF1, 'a', False)
        self.assert_pending()

    def test_success_after_timeout(self):
        conn, future = self.start_connect(self.addrinfo)
        self.assert_pending((AF1, 'a'))
        conn.on_timeout()
        self.assert_pending((AF1, 'a'), (AF2, 'c'))
        self.resolve_connect(AF1, 'a', True)
        self.assertEqual(future.result(), (AF1, 'a', self.streams['a']))
        # resolving 'c' after completion closes the connection.
        self.resolve_connect(AF2, 'c', True)
        self.assertTrue(self.streams.pop('c').closed)

    def test_all_fail(self):
        conn, future = self.start_connect(self.addrinfo)
        self.assert_pending((AF1, 'a'))
        conn.on_timeout()
        self.assert_pending((AF1, 'a'), (AF2, 'c'))
        self.resolve_connect(AF2, 'c', False)
        self.assert_pending((AF1, 'a'), (AF2, 'd'))
        self.resolve_connect(AF2, 'd', False)
        # one queue is now empty
        self.assert_pending((AF1, 'a'))
        self.resolve_connect(AF1, 'a', False)
        self.assert_pending((AF1, 'b'))
        self.assertFalse(future.done())
        self.resolve_connect(AF1, 'b', False)
        self.assertRaises(IOError, future.result)

    def test_one_family_timeout_after_connect_timeout(self):
        conn, future = self.start_connect([(AF1, 'a'), (AF1, 'b')])
        self.assert_pending((AF1, 'a'))
        conn.on_connect_timeout()
        # the connector will close all streams on connect timeout, we
        # should explicitly pop the connect_future.
        self.connect_futures.pop((AF1, 'a'))
        self.assertTrue(self.streams.pop('a').closed)
        conn.on_timeout()
        # if the future is set with TimeoutError, we will not iterate next
        # possible address.
        self.assert_pending()
        self.assertEqual(len(conn.streams), 1)
        self.assert_connector_streams_closed(conn)
        self.assertRaises(TimeoutError, future.result)

    def test_one_family_success_before_connect_timeout(self):
        conn, future = self.start_connect([(AF1, 'a'), (AF1, 'b')])
        self.assert_pending((AF1, 'a'))
        self.resolve_connect(AF1, 'a', True)
        conn.on_connect_timeout()
        self.assert_pending()
        self.assertEqual(self.streams['a'].closed, False)
        # success stream will be pop
        self.assertEqual(len(conn.streams), 0)
        # streams in connector should be closed after connect timeout
        self.assert_connector_streams_closed(conn)
        self.assertEqual(future.result(), (AF1, 'a', self.streams['a']))

    def test_one_family_second_try_after_connect_timeout(self):
        conn, future = self.start_connect([(AF1, 'a'), (AF1, 'b')])
        self.assert_pending((AF1, 'a'))
        self.resolve_connect(AF1, 'a', False)
        self.assert_pending((AF1, 'b'))
        conn.on_connect_timeout()
        self.connect_futures.pop((AF1, 'b'))
        self.assertTrue(self.streams.pop('b').closed)
        self.assert_pending()
        self.assertEqual(len(conn.streams), 2)
        self.assert_connector_streams_closed(conn)
        self.assertRaises(TimeoutError, future.result)

    def test_one_family_second_try_failure_before_connect_timeout(self):
        conn, future = self.start_connect([(AF1, 'a'), (AF1, 'b')])
        self.assert_pending((AF1, 'a'))
        self.resolve_connect(AF1, 'a', False)
        self.assert_pending((AF1, 'b'))
        self.resolve_connect(AF1, 'b', False)
        conn.on_connect_timeout()
        self.assert_pending()
        self.assertEqual(len(conn.streams), 2)
        self.assert_connector_streams_closed(conn)
        self.assertRaises(IOError, future.result)

    def test_two_family_timeout_before_connect_timeout(self):
        conn, future = self.start_connect(self.addrinfo)
        self.assert_pending((AF1, 'a'))
        conn.on_timeout()
        self.assert_pending((AF1, 'a'), (AF2, 'c'))
        conn.on_connect_timeout()
        self.connect_futures.pop((AF1, 'a'))
        self.assertTrue(self.streams.pop('a').closed)
        self.connect_futures.pop((AF2, 'c'))
        self.assertTrue(self.streams.pop('c').closed)
        self.assert_pending()
        self.assertEqual(len(conn.streams), 2)
        self.assert_connector_streams_closed(conn)
        self.assertRaises(TimeoutError, future.result)

    def test_two_family_success_after_timeout(self):
        conn, future = self.start_connect(self.addrinfo)
        self.assert_pending((AF1, 'a'))
        conn.on_timeout()
        self.assert_pending((AF1, 'a'), (AF2, 'c'))
        self.resolve_connect(AF1, 'a', True)
        # if one of streams succeed, connector will close all other streams
        self.connect_futures.pop((AF2, 'c'))
        self.assertTrue(self.streams.pop('c').closed)
        self.assert_pending()
        self.assertEqual(len(conn.streams), 1)
        self.assert_connector_streams_closed(conn)
        self.assertEqual(future.result(), (AF1, 'a', self.streams['a']))

    def test_two_family_timeout_after_connect_timeout(self):
        conn, future = self.start_connect(self.addrinfo)
        self.assert_pending((AF1, 'a'))
        conn.on_connect_timeout()
        self.connect_futures.pop((AF1, 'a'))
        self.assertTrue(self.streams.pop('a').closed)
        self.assert_pending()
        conn.on_timeout()
        # if the future is set with TimeoutError, connector will not
        # trigger secondary address.
        self.assert_pending()
        self.assertEqual(len(conn.streams), 1)
        self.assert_connector_streams_closed(conn)
        self.assertRaises(TimeoutError, future.result)

if __name__ == "__main__":
    unittest.main()
