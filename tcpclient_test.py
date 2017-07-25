#!/usr/bin/env python
#
# Copyright 2014 Facebook
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import, division, print_function

from contextlib import closing
import os
import socket

from tornado.concurrent import Future
from tornado.netutil import bind_sockets, Resolver
from tornado.queues import Queue
# from tornado.tcpclient import TCPClient, _Connector
from tcpclient import TCPClient
from tornado.tcpserver import TCPServer
from tornado.testing import AsyncTestCase, gen_test
from tornado.test.util import skipIfNoIPv6, unittest, refusing_port, skipIfNonUnix
from tornado.gen import TimeoutError


# Fake address families for testing.  Used in place of AF_INET
# and AF_INET6 because some installations do not have AF_INET6.
AF1, AF2 = 1, 2


class TestTCPServer(TCPServer):
    def __init__(self, family):
        super(TestTCPServer, self).__init__()
        self.streams = []
        self.queue = Queue()
        sockets = bind_sockets(None, 'localhost', family)
        self.add_sockets(sockets)
        self.port = sockets[0].getsockname()[1]

    def handle_stream(self, stream, address):
        self.streams.append(stream)
        self.queue.put(stream)

    def stop(self):
        super(TestTCPServer, self).stop()
        for stream in self.streams:
            stream.close()


class TCPClientTest(AsyncTestCase):
    def setUp(self):
        super(TCPClientTest, self).setUp()
        self.server = None
        self.client = TCPClient()

    def start_server(self, family):
        if family == socket.AF_UNSPEC and 'TRAVIS' in os.environ:
            self.skipTest("dual-stack servers often have port conflicts on travis")
        self.server = TestTCPServer(family)
        return self.server.port

    def stop_server(self):
        if self.server is not None:
            self.server.stop()
            self.server = None

    def tearDown(self):
        self.client.close()
        self.stop_server()
        super(TCPClientTest, self).tearDown()

    def skipIfLocalhostV4(self):
        # The port used here doesn't matter, but some systems require it
        # to be non-zero if we do not also pass AI_PASSIVE.
        Resolver().resolve('localhost', 80, callback=self.stop)
        addrinfo = self.wait()
        families = set(addr[0] for addr in addrinfo)
        if socket.AF_INET6 not in families:
            self.skipTest("localhost does not resolve to ipv6")

    @gen_test
    def do_test_connect(self, family, host, source_ip=None, source_port=None):
        port = self.start_server(family)
        stream = yield self.client.connect(host, port,
                                           source_ip=source_ip,
                                           source_port=source_port)
        server_stream = yield self.server.queue.get()
        with closing(stream):
            stream.write(b"hello")
            data = yield server_stream.read_bytes(5)
            self.assertEqual(data, b"hello")

    def test_connect_ipv4_ipv4(self):
        self.do_test_connect(socket.AF_INET, '127.0.0.1')

    def test_connect_ipv4_dual(self):
        self.do_test_connect(socket.AF_INET, 'localhost')

    @skipIfNoIPv6
    def test_connect_ipv6_ipv6(self):
        self.skipIfLocalhostV4()
        self.do_test_connect(socket.AF_INET6, '::1')

    @skipIfNoIPv6
    def test_connect_ipv6_dual(self):
        self.skipIfLocalhostV4()
        if Resolver.configured_class().__name__.endswith('TwistedResolver'):
            self.skipTest('TwistedResolver does not support multiple addresses')
        self.do_test_connect(socket.AF_INET6, 'localhost')

    def test_connect_unspec_ipv4(self):
        self.do_test_connect(socket.AF_UNSPEC, '127.0.0.1')

    @skipIfNoIPv6
    def test_connect_unspec_ipv6(self):
        self.skipIfLocalhostV4()
        self.do_test_connect(socket.AF_UNSPEC, '::1')

    def test_connect_unspec_dual(self):
        self.do_test_connect(socket.AF_UNSPEC, 'localhost')

    @gen_test
    def test_refused_ipv4(self):
        cleanup_func, port = refusing_port()
        self.addCleanup(cleanup_func)
        with self.assertRaises(IOError):
            yield self.client.connect('127.0.0.1', port)

    def test_source_ip_fail(self):
        '''
        Fail when trying to use the source IP Address '8.8.8.8'.
        '''
        self.assertRaises(socket.error,
                          self.do_test_connect,
                          socket.AF_INET,
                          '127.0.0.1',
                          source_ip='8.8.8.8')

    def test_source_ip_success(self):
        '''
        Success when trying to use the source IP Address '127.0.0.1'
        '''
        self.do_test_connect(socket.AF_INET, '127.0.0.1', source_ip='127.0.0.1')

    @skipIfNonUnix
    def test_source_port_fail(self):
        '''
        Fail when trying to use source port 1.
        '''
        self.assertRaises(socket.error,
                          self.do_test_connect,
                          socket.AF_INET,
                          '127.0.0.1',
                          source_port=1)

    @gen_test
    def test_connect_timeout(self):
        timeout = 0.05

        class TimeoutResolver(Resolver):
            def resolve(self, *args, **kwargs):
                return Future()  # never completes
        with self.assertRaises(TimeoutError):
            yield TCPClient(resolver=TimeoutResolver()).connect(
                '1.2.3.4', 12345, timeout=timeout)


if __name__ == "__main__":
    unittest.main()

