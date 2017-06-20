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

"""A non-blocking TCP connection factory.
"""
from __future__ import absolute_import, division, print_function

import functools
import socket
import logging
import numbers

from tornado.concurrent import Future
from tornado.ioloop import IOLoop
from tornado.iostream import IOStream
from tornado import gen
from tornado.netutil import Resolver
from tornado.platform.auto import set_close_exec
from tornado.gen import TimeoutError

_INITIAL_CONNECT_TIMEOUT = 0.3

logging.basicConfig(level=logging.INFO, format='[%(levelname).1s %(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


class _Connector(object):
    """A stateless implementation of the "Happy Eyeballs" algorithm.

    "Happy Eyeballs" is documented in RFC6555 as the recommended practice
    for when both IPv4 and IPv6 addresses are available.

    In this implementation, we partition the addresses by family, and
    make the first connection attempt to whichever address was
    returned first by ``getaddrinfo``.  If that connection fails or
    times out, we begin a connection in parallel to the first address
    of the other family.  If there are additional failures we retry
    with other addresses, keeping one connection attempt per family
    in flight at a time.

    http://tools.ietf.org/html/rfc6555

    """
    def __init__(self, addrinfo, connect):
        self.io_loop = IOLoop.current()
        self.connect = connect

        self.future = Future()
        self.timeout = None
        self.connect_timeout = None
        self.last_error = None
        self.remaining = len(addrinfo)
        self.primary_addrs, self.secondary_addrs = self.split(addrinfo)

    @staticmethod
    def split(addrinfo):
        """Partition the ``addrinfo`` list by address family.

        Returns two lists.  The first list contains the first entry from
        ``addrinfo`` and all others with the same family, and the
        second list contains all other addresses (normally one list will
        be AF_INET and the other AF_INET6, although non-standard resolvers
        may return additional families).
        """
        primary = []
        secondary = []
        primary_af = addrinfo[0][0]
        for af, addr in addrinfo:
            if af == primary_af:
                primary.append((af, addr))
            else:
                secondary.append((af, addr))
        return primary, secondary

    def start(self, timeout=_INITIAL_CONNECT_TIMEOUT, connect_timeout=None):
        self.try_connect(iter(self.primary_addrs))
        self.set_timeout(timeout)
        if connect_timeout is not None:
            self.set_connect_timeout(connect_timeout)
        return self.future

    def try_connect(self, addrs):
        try:
            af, addr = next(addrs)
        except StopIteration:
            # We've reached the end of our queue, but the other queue
            # might still be working.  Send a final error on the future
            # only when both queues are finished.
            if self.remaining == 0 and not self.future.done():
                self.future.set_exception(self.last_error or
                                          IOError("connection failed"))
            return
        future = self.connect(af, addr)
        future.add_done_callback(functools.partial(self.on_connect_done,
                                                   addrs, af, addr))

    def on_connect_done(self, addrs, af, addr, future):
        self.remaining -= 1
        try:
            stream = future.result()
        except Exception as e:
            if self.future.done():
                return
            # Error: try again (but remember what happened so we have an
            # error to raise in the end)
            self.last_error = e
            self.try_connect(addrs)
            if self.timeout is not None:
                # If the first attempt failed, don't wait for the
                # timeout to try an address from the secondary queue.
                self.io_loop.remove_timeout(self.timeout)
                self.on_timeout()
            return
        self.clear_timeout()
        self.clear_connect_timeout()
        if self.future.done():
            # This is a late arrival; just drop it.
            stream.close()
        else:
            self.future.set_result((af, addr, stream))

    def set_timeout(self, timeout):
        self.timeout = self.io_loop.add_timeout(self.io_loop.time() + timeout,
                                                self.on_timeout)

    def on_timeout(self):
        self.timeout = None
        if not self.future.done():
            self.try_connect(iter(self.secondary_addrs))

    def clear_timeout(self):
        if self.timeout is not None:
            self.io_loop.remove_timeout(self.timeout)

    def set_connect_timeout(self, connect_timeout):
        self.connect_timeout = self.io_loop.add_timeout(
            connect_timeout, self.on_connect_timeout)

    def on_connect_timeout(self):
        if not self.future.done():
            self.future.set_exception(TimeoutError())

    def clear_connect_timeout(self):
        if self.connect_timeout is not None:
            self.io_loop.remove_timeout(self.connect_timeout)


def create_stream(af, addr):
    stream = IOStream(socket.socket(af))
    return stream.connect(addr)


def fake_addrinfo_1():
    # private address
    return [
        (2, ('192.168.0.1', 80)),
        (2, ('192.168.0.2', 80)),
    ]


def fake_addrinfo_2():
    # google address
    return [
        (2, ('108.177.97.106', 80)),
        (2, ('108.177.97.147', 80)),
        (2, ('108.177.97.103', 80)),
        (2, ('108.177.97.99', 80)),
        (2, ('108.177.97.105', 80)),
        (2, ('108.177.97.104', 80)),
        (10, ('2404:6800:4008:c05::68', 80, 0, 0))
    ]


def fake_addrinfo_3():
    # gcp
    return [
        (2, ('104.199.255.152', 80)),

    ]


@gen.coroutine
def runner():
    # resolver = Resolver()
    # addrinfo = yield resolver.resolve('www.google.com', '80', socket.AF_UNSPEC)
    addrinfo = fake_addrinfo_1()

    logging.info(addrinfo)
    connector = _Connector(addrinfo, create_stream)
    try:
        af, addr, stream = yield connector.start(connect_timeout=IOLoop.current().time()+0.1)
    except TimeoutError as e:
        logging.info('fuck up timeout')


def main():
    runner()
    IOLoop.current().start()


if __name__ == '__main__':
    main()
