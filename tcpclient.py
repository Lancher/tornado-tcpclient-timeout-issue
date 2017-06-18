from __future__ import absolute_import, division, print_function

import functools
import socket
import numbers
import datetime
import time

from tornado.concurrent import Future
from tornado.ioloop import IOLoop
from tornado.iostream import IOStream
from tornado import gen
from tornado.netutil import Resolver
from tornado.platform.auto import set_close_exec
from connector import _Connector
from tornado.util import PY3, Configurable, errno_from_exception, timedelta_to_seconds
from tornado.gen import TimeoutError


class TCPClient(object):
    """A non-blocking TCP connection factory.

    .. versionchanged:: 5.0
       The ``io_loop`` argument (deprecated since version 4.1) has been removed.
    """
    def __init__(self, resolver=None):
        if resolver is not None:
            self.resolver = resolver
            self._own_resolver = False
        else:
            self.resolver = Resolver()
            self._own_resolver = True

    def close(self):
        if self._own_resolver:
            self.resolver.close()

    @gen.coroutine
    def connect(self, host, port, af=socket.AF_UNSPEC, ssl_options=None,
                max_buffer_size=None, source_ip=None, source_port=None,
                timeout=None):
        """Connect to the given host and port.

        Asynchronously returns an `.IOStream` (or `.SSLIOStream` if
        ``ssl_options`` is not None).

        Using the ``source_ip`` kwarg, one can specify the source
        IP address to use when establishing the connection.
        In case the user needs to resolve and
        use a specific interface, it has to be handled outside
        of Tornado as this depends very much on the platform.

        Similarly, when the user requires a certain source port, it can
        be specified using the ``source_port`` arg.

        .. versionchanged:: 4.5
           Added the ``source_ip`` and ``source_port`` arguments.
        """
        if timeout is not None:
            if isinstance(timeout, numbers.Real):
                timeout = time.time() + timeout
            elif isinstance(timeout, datetime.timedelta):
                timeout = time.time() + timedelta_to_seconds(timeout)
            else:
                raise TypeError("Unsupported timeout %r" % timeout)
        if timeout is not None:
            addrinfo = yield gen.with_timeout(
                timeout, self.resolver.resolve(host, port, af))
        else:
            addrinfo = yield self.resolver.resolve(host, port, af)
        connector = _Connector(
            addrinfo,
            functools.partial(self._create_stream, max_buffer_size,
                              source_ip=source_ip, source_port=source_port)
        )
        af, addr, stream = yield connector.start(connect_timeout=timeout)
        # TODO: For better performance we could cache the (af, addr)
        # information here and re-use it on subsequent connections to
        # the same host. (http://tools.ietf.org/html/rfc6555#section-4.2)
        if ssl_options is not None:
            if timeout is not None:
                stream = yield gen.with_timeout(timeout, stream.start_tls(
                    False, ssl_options=ssl_options, server_hostname=host))
            else:
                stream = yield stream.start_tls(False, ssl_options=ssl_options,
                                                server_hostname=host)
        raise gen.Return(stream)

    def _create_stream(self, max_buffer_size, af, addr, source_ip=None,
                       source_port=None):
        # Always connect in plaintext; we'll convert to ssl if necessary
        # after one connection has completed.
        source_port_bind = source_port if isinstance(source_port, int) else 0
        source_ip_bind = source_ip
        if source_port_bind and not source_ip:
            # User required a specific port, but did not specify
            # a certain source IP, will bind to the default loopback.
            source_ip_bind = '::1' if af == socket.AF_INET6 else '127.0.0.1'
            # Trying to use the same address family as the requested af socket:
            # - 127.0.0.1 for IPv4
            # - ::1 for IPv6
        socket_obj = socket.socket(af)
        set_close_exec(socket_obj.fileno())
        if source_port_bind or source_ip_bind:
            # If the user requires binding also to a specific IP/port.
            try:
                socket_obj.bind((source_ip_bind, source_port_bind))
            except socket.error:
                socket_obj.close()
                # Fail loudly if unable to use the IP/port.
                raise
        try:
            stream = IOStream(socket_obj,
                              max_buffer_size=max_buffer_size)
        except socket.error as e:
            fu = Future()
            fu.set_exception(e)
            return fu
        else:
            return stream.connect(addr)


def fake_addrinfo_1():
    # private address
    return [
        (2, ('192.168.0.1', 80)),
        (2, ('192.168.0.2', 80))
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
    # test precise timeout
    timeout = 0.01
    timeout_min, timeout_max = time.time() + 0.01, time.time() + 0.03
    try:
        stream = yield TCPClient().connect('192.168.0.1', '80', timeout=timeout)
    except TimeoutError:
        print(timeout_min, time.time(), timeout_max)
        print(timeout_min < time.time() < timeout_max)


def main():
    runner()
    IOLoop.current().start()


if __name__ == '__main__':
    main()
