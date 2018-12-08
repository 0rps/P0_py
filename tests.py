import unittest
import time
import uuid
import os
import socket
import queue
import server_impl as server


#
# class TestSimple(unittest.TestCase):
#
#     server_port = 12998
#     server_host = 'localhost'
#
#     def setUp(self):
#         self.kv_server = KeyValueServer()
#         self.kv_server.start(self.server_port)
#         time.sleep(0.1)
#
#         self.client = Client()
#         self.client.connect(self.server_host, self.server_port)
#
#     def tearDown(self):
#         self.kv_server.close()
#         time.sleep(0.5)
#
#     def test_simple_put(self):
#         self.client.set_value('1', '456')
#
#     def test_get_empty(self):
#         response = self.client.get_value('1')
#         self.assertEqual(len(response), 0)
#
#     def test_get_single(self):
#         self.client.set_value('1', '123')
#         response = self.client.get_value('1')
#         self.assertEqual(len(response), 0)
#
#     def test_get_multiple(self):
#         self.client.set_value('1', '123')
#         self.client.set_value('1', 'qwerty')
#         self.client.set_value('1', '123_qwerty')
#         self.client.set_value('1', 'qwerty')
#         response = self.client.get_value('1')
#
#         self.assertEqual(len(response), 3)
#         self.assertListEqual(response, ['123', 'qwerty', '123_qwerty'])
#
#     def test_delete_empty(self):
#         self.client.delete_key('1')
#
#     def test_delete_single(self):
#         self.client.set_value('1', '123')
#         self.client.delete_key('1')
#         response = self.client.get_value('1')
#
#         self.assertEqual(len(response), 0)
#
#     def test_delete_multiple(self):
#         self.client.set_value('1', '123')
#         self.client.set_value('1', '456')
#         self.client.set_value('1', 'abc')
#         self.client.delete_key('1')
#
#         response = self.client.get_value('1')
#         self.assertEqual(len(response), 0)


class ClientException(Exception):
    pass


# class Client:
#     def __init__(self, is_slow=False):
#         self._socket = None
#         self._is_slow = is_slow
#
#     def connect(self, host, port) -> bool:
#         if self._socket is not None:
#             raise ClientException('Client already connected')
#
#         self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#         self._socket.connect((host, port))
#
#     def set_value(self, key, value):
#         message = "put,{key},{value}\n".format(key=key, value=value)
#         self._send(message)
#
#     def get_value(self, key):
#         message = "get,{key}".format(key=key)
#         self._send(message)
#         raw_values = self._receive().split('\n')
#         values = [value[value.find(','):] for value in raw_values]
#         return values
#
#     def delete_key(self, key):
#         message = "delete,{key}".format(key=key)
#         self._send(message)
#
#     def _send(self, message):
#         message = message.encode('utf-8')
#         total_sent = 0
#         while total_sent < len(message):
#             sent = self._socket.send(message[total_sent:])
#             if sent == 0:
#                 raise RuntimeError('Connection closed')
#             total_sent += sent
#
#     def _receive(self):
#         buffer = b''
#         while True:
#             chunk = self._socket.recv(1024)
#             if chunk == b'':
#                 break
#                 # raise RuntimeError('Connection closed')
#             buffer += chunk
#             if len(buffer) > 1 and buffer[-1] == b'\n' and buffer[-2] == b'\n':
#                 buffer = buffer[:-2]
#                 break
#
#             if len(buffer) == 1 and buffer[0] == b'\n':
#                 buffer = b''
#                 break
#
#         return buffer.decode('utf-8')
#

class TestWorker(unittest.TestCase):
    def setUp(self):
        server.kv.init_db()
        self._socket, self._client = socket.socketpair()
        self._queue = queue.Queue()
        self._thread = server.WorkerThread(self._client, self._queue)
        self._thread.start()

    def test_start_close(self):
        cmd = self._queue.get()
        self.assertEqual(cmd.cmd, server.CMD_NEW)

        w_uuid, w_sock = cmd.params
        self.assertEqual(len(w_uuid) > 0, True)

        w_sock.send(str(server.CMD_CLOSE_ALL).encode())
        item = self._queue.get(timeout=2)

        self.assertEqual(item.cmd, server.CMD_CLOSED)
        self.assertEqual(item.params[0], w_uuid)

    def test_put(self):
        self._socket.send(b'put,key1,v1\n')
        self._socket.send(b'get,key1\n')

        msg = self._socket.recv(1024)
        self.assertEqual(msg, b'key1,v1\n\n')

        self._socket.send(b'get,key3\n')
        msg = self._socket.recv(1024)
        self.assertEqual(msg, b'\n')

    def test_multiple_put(self):
        self._socket.send(b'put,key1,v1\n')
        self._socket.send(b'put,key2,v2\n')
        self._socket.send(b'put,key3,v3\n')
        self._socket.send(b'get,key2\n')

        msg = self._socket.recv(1024)
        self.assertEqual(msg, b'key2,v2\n\n')

        self._socket.send(b'put,key2,v22\n')
        self._socket.send(b'put,key2,v22\n')
        self._socket.send(b'get,key2\n')
        msg = self._socket.recv(1024)
        msg = msg.decode('utf-8')
        splitted_msg = [x for x in msg.split('\n') if len(x) > 0]

        splitted_msg = ",".join(splitted_msg).split(',')

        self.assertEqual(len(splitted_msg), 4)
        self.assertEqual('key2', splitted_msg[0])
        self.assertEqual('key2', splitted_msg[2])

        self.assertListEqual(sorted(['v22', 'v2']), sorted([splitted_msg[1], splitted_msg[3]]))

        self._socket.send(b'put,key2,v23\n')
        self._socket.send(b'get,key2\n')
        msg = self._socket.recv(1024)
        msg = msg.decode('utf-8')
        splitted_msg = ','.join([x for x in msg.split('\n') if len(x) > 0]).split(',')

        self.assertEqual(len(splitted_msg), 6)
        self.assertListEqual(sorted(['v22', 'v2', 'v23']), sorted([splitted_msg[1], splitted_msg[3], splitted_msg[5]]))

    def test_del(self):
        self._socket.send(b'put,key2,v2\n')
        self._socket.send(b'put,key3,v3\n')
        self._socket.send(b'get,key3\n')

        msg = self._socket.recv(1024)
        self.assertEqual(msg, b'key3,v3\n\n')

        self._socket.send(b'del,key2\n')
        self._socket.send(b'get,key3\n')
        msg = self._socket.recv(1024)
        self.assertEqual(msg, b'key3,v3\n\n')

        self._socket.send(b'get,key2\n')
        msg = self._socket.recv(1024)
        self.assertEqual(msg, b'\n')

    def tearDown(self):
        self._socket.close()
        self._thread.join(2)
        if self._thread.is_alive():
            raise Exception("Thread is not dead")


class TestListenerStart(unittest.TestCase):

    PORT = 18781

    def test_start_finish_listener(self):
        control_queue = queue.Queue()
        listener = server.ListenerThread(self.PORT, control_queue)
        listener.start()

        cmd = control_queue.get(timeout=1)
        self.assertEqual(cmd.cmd, server.CMD_NEW_LISTENER)

        listener_uuid, sock = cmd.params
        sock.send(str(server.CMD_CLOSE_ALL).encode('utf-8'))

        cmd = control_queue.get(timeout=1)

        self.assertEqual(cmd.cmd, server.CMD_CLOSED)
        self.assertEqual(listener_uuid, cmd.params[0])

        listener.join(timeout=1)


class TestListener(unittest.TestCase):

    PORT = 18781

    def setUp(self):
        self._queue = queue.Queue()
        self._listener = server.ListenerThread(self.PORT, self._queue)
        self._listener.start()
        cmd = self._queue.get(timeout=1)
        self._control_socket = cmd.params[1]

    def test_start_close_client(self):
        client = socket.socket()
        client.connect((socket.gethostname(), self.PORT))
        new_cmd = self._queue.get(timeout=1)
        self.assertEqual(new_cmd.cmd, server.CMD_NEW)

        client.close()
        closed_cmd = self._queue.get(timeout=1)
        self.assertEqual(closed_cmd.cmd, server.CMD_CLOSED)

    def tearDown(self):
        self._control_socket.send(str(server.CMD_CLOSE_ALL).encode('utf-8'))
        self._listener.join(timeout=1)


class TestStartStopControlManager(unittest.TestCase):

    def stop_stop_with_no_clients(self):
        self._in = queue.Queue()
        self._out = queue.Queue()
        self._thread = server.ControlThread(self._in, self._out)
        self._thread.start()
        self._in.put(server.ControlCommand(server.CMD_CLOSE_ALL, None))
        self._thread.join(timeout=1)

    def stop_with_clients(self):
        self._in = queue.Queue()
        self._out = queue.Queue()
        self._thread = server.ControlThread(self._in, self._out)
        self._thread.start()

        ssock, csock= socket.socketpair()
        cuuid = uuid.uuid4().hex
        self._in.put(server.ControlCommand(server.CMD_NEW, (cuuid, ssock)))
        self._in.put(server.ControlCommand(server.CMD_CLOSE_ALL, None))

        data = csock.recv(1024)
        self.assertEqual(data, str(server.CMD_CLOSE_ALL).encode('utf-8'))

        self._in.put(server.ControlCommand(server.CMD_CLOSED, (cuuid, )))
        self._thread.join(timeout=1)


class TestControlManager(unittest.TestCase):

    def setUp(self):
        self._in = queue.Queue()
        self._out = queue.Queue()
        self._thread = server.ControlThread(self._in, self._out)
        self._thread.start()

    def _open_client(self):
        client_uuid = uuid.uuid4().hex
        csock, ssock = socket.socketpair()
        self._in.put(server.ControlCommand(server.CMD_NEW, (client_uuid, ssock)))
        return client_uuid, csock

    def _close_client(self, client_uuid):
        self._in.put(server.ControlCommand(server.CMD_CLOSED, (client_uuid, )))

    def test_count_active(self):
        self._in.put(server.ControlCommand(server.CMD_COUNT_ACTIVE, None))
        result = self._out.get(timeout=1)
        self.assertEqual(result, 0)

        client1, sock1 = self._open_client()
        client2, sock2 = self._open_client()
        client3, sock3 = self._open_client()

        self._in.put(server.ControlCommand(server.CMD_COUNT_ACTIVE, None))
        result = self._out.get(timeout=1)
        self.assertEqual(result, 3)

        client4, sock4 = self._open_client()
        client5, sock5 = self._open_client()

        self._in.put(server.ControlCommand(server.CMD_COUNT_ACTIVE, None))
        result = self._out.get(timeout=1)
        self.assertEqual(result, 5)

        self._close_client(client1)
        self._close_client(client2)
        self._close_client(client3)
        self._close_client(client4)
        self._close_client(client5)

    def test_count_dropped(self):
        client1, sock1 = self._open_client()
        client2, sock2 = self._open_client()
        client3, sock3 = self._open_client()

        self._in.put(server.ControlCommand(server.CMD_COUNT_ACTIVE, None))
        result = self._out.get(timeout=1)
        self.assertEqual(result, 3)

        self._close_client(client1)
        self._close_client(client2)

        self._in.put(server.ControlCommand(server.CMD_COUNT_ACTIVE, None))
        result = self._out.get(timeout=1)
        self.assertEqual(result, 1)

        client4, sock4 = self._open_client()
        self._close_client(client3)

        self._in.put(server.ControlCommand(server.CMD_COUNT_DROPPED, None))
        result = self._out.get(timeout=1)
        self.assertEqual(result, 3)

        self._close_client(client4)
        self._in.put(server.ControlCommand(server.CMD_COUNT_ACTIVE, None))
        result = self._out.get(timeout=1)
        self.assertEqual(result, 0)

    def test_close_clients(self):
        client_1, sock1 = self._open_client()
        client_2, sock2 = self._open_client()
        self._in.put(server.ControlCommand(server.CMD_CLOSE_ALL, None))

        close_all = str(server.CMD_CLOSE_ALL).encode('utf-8')

        data1 = sock1.recv(1024)
        self.assertEqual(data1, close_all)
        data2 = sock2.recv(1024)
        self.assertEqual(data2, close_all)

        self._in.put(server.ControlCommand(server.CMD_CLOSED, (client_1,)))
        self._in.put(server.ControlCommand(server.CMD_CLOSED, (client_2,)))

    def tearDown(self):
        self._in.put(server.ControlCommand(server.CMD_CLOSE_ALL, None))
        self._thread.join(timeout=1)


class TestStartStopServer(unittest.TestCase):

    PORT = 18781

    def test(self):
        self._server = server.KeyValueServer()
        self._server.start(self.PORT)
        self._server.close()
        self._server._control.join(timeout=2)


class TestServer(unittest.TestCase):

    PORT = 18781

    def setUp(self):
        self._server = server.KeyValueServer()
        self._server.start(self.PORT)
        self._socks = []

    def _new_client(self):
        sock = socket.socket()
        sock.connect((socket.gethostname(), self.PORT))
        self._socks.append(sock)
        return sock

    def test_active_client(self):
        count = self._server.count_active()
        self.assertEqual(count, 0)

        sock1 = self._new_client()
        time.sleep(0.5)
        count = self._server.count_active()
        self.assertEqual(count, 1)

        sock2 = self._new_client()
        time.sleep(0.1)
        count = self._server.count_active()
        self.assertEqual(count, 2)

        sock2.close()
        time.sleep(0.1)
        count = self._server.count_active()
        self.assertEqual(count, 1)

    def test_dropped_client(self):
        sock1 = self._new_client()
        sock2 = self._new_client()
        sock3 = self._new_client()

        count = self._server.count_dropped()
        self.assertEqual(count, 0)

        sock1.close()
        sock2.close()

        time.sleep(0.1)
        count = self._server.count_dropped()
        self.assertEqual(count, 2)

        sock3.close()
        time.sleep(0.1)
        count = self._server.count_dropped()
        self.assertEqual(count, 3)

    def tearDown(self):
        self._server.close()
        self._server._control.join(timeout=2)


if __name__ == '__main__':
    unittest.main()
