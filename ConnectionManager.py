import socket
import threading

from concurrent.futures import ThreadPoolExecutor
from message_manager import MessageManager
from .core_node_list import CoreNodeList

PING_INTERVAL = 1800

class ConnectionManager:
    def __init__(self, host, my_port):
        print('Initializing ConnecitonManager')
        self.host = host
        self.port = my_port
        self.core_node_set = CoreNodeList()
        self.__add_peer((host, my_port))
        self.mm = MessageManager()
    
    def start(self):
        t = threading.Thread(target=self.__wait_for_access)
        t.start()

        self.ping_timer = threading.Timer(PING_INTERVAL, self.__check_peers_connection)
        self.ping_timer.start()

    def join_network(self, host, port):
        self.my_c_host = host
        self.my_c_port = port
        self.__connect_to_P2PNW(host, port)

    def __connect_to_P2PNW(self, host, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, port))
        msg = self.mm.build(MSG_ADD, self.port)
        s.sendall(msg.encode('utf-8'))
        s.close()

    def connection_close(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.host, self.port))
        self.socket.close()
        s.close()
        self.ping_timer.cancel()
        msg = self.mm.build(MSG_REMOVE, self.port)
        self.send_msg((self.my_c_host, self.my_c_port), msg)

    def send_msg(self, peer, msg):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((peer))
            s.sendall(msg.encode('utf-8'))
            s.close()
        except OSError:
            print('Connection failed for peer: ', peer)
            self.__remove_peer(peer)

    def send_msg_to_all_peer(self, msg):
        print('send msg_to_all_peer was called')
        current_list = self.core_node_set.get_list()
        for peer in current_list:
            if peer != (self.host, self.port):
                print('message will be send to ...', peer)
                self.send_msg(peer, msg)

    

    def __handle_message(self, params):
        soc, addr, data_sum = params

        while True:
            data = soc.recv(1024)
            data_sum = data_sum + data.decode('utf-8')

            if not data:
                break
        
        if not data_sum:
            return

        result, reason, cmd, peer_port, payload = self.mm.parse(data_sum)
        print(result, reason, cmd, peer_port, payload)
        status = (result, reason)

        if status == ('error', ERR_PROTOCOL_UNMATCH):
            print('Error: Protocol name is not matched')
            return
        elif status == ('error', ERR_VERSION_UNMATCH):
            print('Error: Protocol version is not matched')
            return
        elif status == ('ok', OK_WITHOUT_PAYLOAD):
            if cmd == MSG_ADD:
                print('ADD request was received')
                self.__add_peer((addr[0], peer_port))
                if (addr[0], peer_port) == (self.host, self.port):
                    return
                else:
                    cl = pickle.dumps(self.core_node_set.get_list(), 0).decode()
                    msg = self.mm.build(MSG_CORE_LIST, self.port, cl)
                    self.send_msg_to_all_peer(msg)
            elif cmd == MSG_REMOVE:
                print('REMOVE request was received. from ', addr[0], peer_port)
                self.__remove_peer((addr[0], peer_port))
                cl = pickle.dumps(self.core_node_set.get_list(), 0).decode()
                msg = self.mm.build(MSG_CORE_LIST, self.port, cl)
                self.send_msg_to_all_peer(msg)
            elif cmd == MSG_PING:
                return
            elif cmd == MSG_REQUEST_CORE_LIST:
                print('List for Core nodes was requested')
                cl = picle.dumps(self.core_node_set.get_list(), 0).decode()
                msg = self.mm.build(MSG_CORE_LIST, self.port, cl)
                self.send_msg((addr[0], peer_port), msg)
            else:
                print('received unkown command', cmd)
                return
        elif status == ('ok', OK_WITH_PAYLOAD):
            if cmd == MSG_CORE_LIST:
                print('Refresh the core node list...')
                new_core_set = pickle.loads(payload.encode('utf-8'))
                print('latest core node list: ' new_core_set)
                self.core_node_set.overwrite(new_core_set)
            else:
                print('Unexpected status ', status)

    def __add_peer(self):
        print('Adding peer: ', peer)
        self.core_node_set.add((peer))

    def __remove_peer(self):
        print('Removing peer: ', peer)
        self.core_node_set.remove(peer)
        print('Current Core list: ' , self.core_node_set.get_list())

    def __wait_for_access(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCKET_STREAM)
        self.socket.bind((self.host, self.port))
        self.socket.listen(0)

        executor = ThreadPoolExecutor(max_workers=10)

        while True:
            print('Waiting for the connection...')
            soc, addr = self.socket.accept()
            print('Connected by ', addr)
            data_sum = ''
            params = (soc, addr, data_sum)
            executor.submit(self.__handle_message, params)

    def __check_peers_connection(self):
        print('check_peers_connection was called')
        current_list = self.core_node_set.get_list()
        changed = False
        dead_c_node_set = list(filter(lambda p: not self.__is_alive(p), current_list))

        if dead_c_node_set:
            changed = True
            print('Removing ', dead_c_node_set)
            current_list = current_list - set(dead_c_node_set)
            self.core_node_set.overwrite(current_list)

        print('current core noe list: ' self.core_node_set.get_list())

        if changed:
            cl = pickle.dumps(current_list, 0).decode()
            msg = self.mm.build(MSG_CORE_LIST, self.port, cl)
            self.send_msg_to_all_peer(msg)

        self.ping_timer = threading.Timer(PING_INTERVAL, self.__check_peers_connection)
        self.ping_timer.start()

    def __is_alive(self, target):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((target))
            msg_type = MSG_PING
            msg = self.mm.build(msg_type)
            s.sendall(msg.encode('utf-8'))
            s.close()
            return True
        except OSError:
            return False
