import socket
import select
import os
import uuid
from threading import Thread, Lock
from queue import Queue
from collections import namedtuple
import kv_impl as kv


LISTEN_BACKLOG = 25

CMD_CLOSE_ALL = 1
CMD_CLOSED = 2
CMD_NEW = 3
CMD_COUNT_ACTIVE = 4
CMD_COUNT_DROPPED = 5

ControlCommand = namedtuple('ControlCommand', ['cmd', 'params'])


kv_lock = Lock()


class KVServerException(Exception):
    pass


class ControlThread(Thread):

    def __init__(self, in_queue: Queue, out_queue: Queue):
        super().__init__()
        self.dropped = 0
        self.__control_channels = {}
        self.__in_queue = in_queue
        self.__out_queue = out_queue
        self.__is_close = False

    def run(self):
        while True:
            control_cmd = self.__in_queue.get()
            cmd, params = control_cmd.cmd, control_cmd.params

            if cmd == CMD_NEW:
                client_uuid, client_pipe = params[0], params[1]
                self.__control_channels[client_uuid] = client_pipe
            elif cmd == CMD_CLOSED:
                client_uuid = params[0]
                del self.__control_channels[client_uuid]
                if self.__is_close and len(self.__control_channels) == 0:
                    self.__out_queue.put_nowait(ControlCommand(CMD_CLOSED, None))
                    break
            elif cmd == CMD_COUNT_ACTIVE:
                self.__out_queue.put_nowait(len(self.__control_channels))
            elif cmd == CMD_COUNT_DROPPED:
                self.__out_queue.put_nowait(self.dropped)
            elif cmd == CMD_CLOSE_ALL:
                for pipe in self.__control_channels.values():
                    pipe.write(cmd)
                if not len(self.__control_channels):
                    self.__out_queue.put_nowait(ControlCommand(CMD_CLOSED, None))
                    break


class ListenerThread(Thread):

    def __init__(self, port: int, result_queue: Queue):
        super().__init__()
        self._uuid = uuid.uuid4().hex
        self._control_queue = result_queue
        self._control_pipe, write_pipe = os.pipe2(os.O_NONBLOCK)

        cmd = ControlCommand(CMD_NEW, (self._uuid, write_pipe))
        self._control_queue.put_nowait(cmd)

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setblocking(False)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((socket.gethostname(), port))
        self._socket.listen(LISTEN_BACKLOG)

    def run(self):
        inputs = [self._control_pipe, self._socket]
        while True:
            readable, _, exceptional = select.select(inputs, [], inputs)
            for s in readable:
                if s == self._control_pipe:
                    self._control_queue.put_nowait(ControlCommand(CMD_CLOSED, (self._uuid, )))
                    self._control_pipe.close()
                    self._socket.close()
                    return
                elif s == self._socket:
                    client_socket, _ = s.accept()
                    WorkerThread(client_socket, self._control_queue).start()


class WorkerThread(Thread):

    def __init__(self, client_socket, result_queue):
        super().__init__()
        self._uuid = uuid.uuid4().hex
        self._socket = client_socket
        self._socket.setblocking(False)
        self._control_queue = result_queue
        self._control_pipe, write_pipe = os.pipe2(os.O_NONBLOCK)

        cmd = ControlCommand(CMD_NEW, (self._uuid, write_pipe))
        self._control_queue.put_nowait(cmd)

        self._recv_buffer = b''
        self._send_buffer = []

    def run(self):
        inputs = [self._control_pipe, self._socket]
        while True:
            outputs = []
            if len(self._send_buffer) > 0:
                outputs = [self._socket]
            readable, writable, exceptional = select.select(inputs, outputs, inputs)

            for sock in exceptional:
                pass

            for sock in readable:
                if sock == self._control_pipe:
                    # NOTE: unused data
                    self._control_pipe.read()

                    self._control_pipe.close()
                    self._socket.close()
                    return
                elif sock == self._socket:
                    pass

            for sock in writable:
                send_data = self._send_buffer[0]
                sent_count = sock.send(send_data)
                if len(send_data) == sent_count:
                    self._send_buffer = self._send_buffer[1:]
                else:
                    self._send_buffer[0] = send_data[sent_count:]

    def _del_command(self, data):
        key = data
        with kv_lock:
            kv.clear(key)

    def _get_command(self, data):
        key = data
        with kv_lock:
            values = kv.get(key)

        msg = ''
        for value in values:
            msg += "{},{}\n".format(key, value)
        msg += '\n'
        self._write_message(msg)

    def _put_command(self, data):
        key, value = data.partition(',')
        with kv_lock:
            kv.put(key, value)

    def _parse_buffer(self):
        while True:
            raw_cmd, middle, tail = self._recv_buffer.partition(b'\n')
            if len(middle) == 0:
                return

            cmd, _, data = raw_cmd.partition(b',')
            data = data.decode('utf-8')
            if cmd == b'get':
                self._get_command(data)
            elif cmd == b'put':
                self._put_command(data)
            elif cmd == b'del':
                self._del_command(data)

    def _write_message(self, data):
        self._send_buffer.append(data.encode('utf-8'))


class KeyValueServer:

    S_UNKNOWN = -1
    S_CLOSED = 0
    S_STARTING = 1
    S_RUNNING = 2

    def __init__(self):
        kv.init_db()
        self._query_queue = Queue()
        self._result_queue = Queue()
        self._status = self.S_UNKNOWN

    def start(self, port: int):
        if self._status == self.S_CLOSED:
            raise KVServerException('Server already closed')

        self._status = self.S_STARTING
        control_thread = ControlThread(self._query_queue, self._result_queue)
        control_thread.start()

        listener_thread = ListenerThread(port, self._query_queue)
        listener_thread.start()

        self._status = self.S_RUNNING

    def close(self):
        if not self.S_RUNNING:
            raise KVServerException('Server is not running')
        self._send_recv(CMD_CLOSE_ALL)
        self._status = self.S_CLOSED

    def count_active(self) -> int:
        return self._send_recv(CMD_COUNT_ACTIVE)

    def count_dropped(self) -> int:
        return self._send_recv(CMD_COUNT_DROPPED)

    def _send_recv(self, cmd):
        cmd = ControlCommand(cmd, None)
        self._query_queue.put_nowait(cmd)
        return self._result_queue.get()