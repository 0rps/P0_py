import unittest
import time
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


class Client:
    def __init__(self, is_slow=False):
        self._socket = None
        self._is_slow = is_slow

    def connect(self, host, port) -> bool:
        if self._socket is not None:
            raise ClientException('Client already connected')

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((host, port))

    def set_value(self, key, value):
        message = "put,{key},{value}\n".format(key=key, value=value)
        self._send(message)

    def get_value(self, key):
        message = "get,{key}".format(key=key)
        self._send(message)
        raw_values = self._receive().split('\n')
        values = [value[value.find(','):] for value in raw_values]
        return values

    def delete_key(self, key):
        message = "delete,{key}".format(key=key)
        self._send(message)

    def _send(self, message):
        message = message.encode('utf-8')
        total_sent = 0
        while total_sent < len(message):
            sent = self._socket.send(message[total_sent:])
            if sent == 0:
                raise RuntimeError('Connection closed')
            total_sent += sent

    def _receive(self):
        buffer = b''
        while True:
            chunk = self._socket.recv(1024)
            if chunk == b'':
                break
                # raise RuntimeError('Connection closed')
            buffer += chunk
            if len(buffer) > 1 and buffer[-1] == b'\n' and buffer[-2] == b'\n':
                buffer = buffer[:-2]
                break

            if len(buffer) == 1 and buffer[0] == b'\n':
                buffer = b''
                break

        return buffer.decode('utf-8')


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
        self.assertEqual(cmd.cmd, server.CMD_NEW)

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


class TestControlManager(unittest.TestCase):

    def setUp(self):
        self._in = queue.Queue()
        self._out = queue.Queue()
        self._thread = server.ControlThread(self._in, self._out)
        self._thread.start()

    def test_count_active(self):
        pass

    def test_count_dropped(self):
        pass

    def test_close_clients(self):
        pass

    def tearDown(self):
        self._in.put(server.ControlCommand(server.CMD_CLOSE_ALL, None))
        self._thread.join(timeout=1)

#
# class TestServer(unittest.TestCase):
#     pass


def main():
    server_socket, client_socket = socket.socketpair()
    c_queue = queue.Queue()
    thread = server.WorkerThread(client_socket, c_queue)
    thread.start()

    cmd = c_queue.get()

    w_uuid, ssock = cmd.params
    print("uuid: ", w_uuid)

    ssock.send(str(server.CMD_CLOSE_ALL).encode())
    time.sleep(5)
    item = c_queue.get(timeout=2)
    print(item)


if __name__ == '__main__':
    #main()
    unittest.main()
