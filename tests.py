import unittest
import time

import socket
from server_impl import KeyValueServer


# class ClientException(Exception):
#     pass
#
#
# class Client:
#     def __init__(self):
#         self._socket = None
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
#
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

class TestClient(unittest.TestCase):
    pass


class TestListener(unittest.TestCase):
    pass


class TestControlManager(unittest.TestCase):
    pass


class TestServer(unittest.TestCase):
    pass

  if __name__ == '__main__':
    unittest.main()