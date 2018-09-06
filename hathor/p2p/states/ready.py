# encoding: utf-8

from twisted.internet.task import LoopingCall

from hathor.transaction.storage.exceptions import TransactionDoesNotExist
from hathor.p2p.states.base import BaseState
from hathor.transaction import Transaction, Block
from hathor.p2p.peer_id import PeerId

import json
import base64
import time


class ReadyState(BaseState):
    def __init__(self, protocol):
        super().__init__(protocol)
        self.cmd_map.update({
            # p2p control messages
            self.ProtocolCommand.PING: self.handle_ping,
            self.ProtocolCommand.PONG: self.handle_pong,
            self.ProtocolCommand.GET_PEERS: self.handle_get_peers,
            self.ProtocolCommand.PEERS: self.handle_peers,
            self.ProtocolCommand.ERROR: self.handle_error,

            # hathor messages
            self.ProtocolCommand.GET_DATA: self.handle_get_data,
            self.ProtocolCommand.DATA: self.handle_data,
            self.ProtocolCommand.GET_BLOCKS: self.handle_get_blocks,
            self.ProtocolCommand.BLOCKS: self.handle_blocks,
            self.ProtocolCommand.GET_BEST_HEIGHT: self.handle_get_best_height,
            self.ProtocolCommand.BEST_HEIGHT: self.handle_best_height,
        })

    def on_enter(self):
        protocol = self.protocol
        protocol.manager.on_peer_connect(protocol)
        protocol.lc_ping = LoopingCall(self.send_ping_if_necessary)
        protocol.lc_ping.start(1)

        self.send_get_peers()
        self.send_get_best_height()

    def on_exit(self):
        protocol = self.protocol
        protocol.manager.on_peer_disconnect(protocol)
        protocol.lc_ping.stop()

    def send_get_data(self, hash_hex):
        """ Send a GET-DATA message, requesting the data of a given hash.
        """
        print('send_get_data', hash_hex)
        self.send_message(self.ProtocolCommand.GET_DATA, hash_hex)

    def handle_get_data(self, payload):
        hash_hex = payload
        print('handle_get_data', hash_hex)
        try:
            tx = self.protocol.manager.tx_storage.get_transaction_by_hash(hash_hex)
            self.send_data(tx)
        except TransactionDoesNotExist:
            # TODO Send NOT-FOUND?
            self.send_data('')
        except Exception as e:
            print(e)

    def send_data(self, tx):
        payload_type = 'tx' if not tx.is_block else 'block'
        payload = base64.b64encode(tx.get_struct()).decode('ascii')
        self.send_message(self.ProtocolCommand.DATA, '{}:{}'.format(payload_type, payload))

    def handle_data(self, payload):
        if not payload:
            return
        payload_type, _, payload = payload.partition(':')
        data = base64.b64decode(payload)
        if payload_type == 'tx':
            tx = Transaction.create_from_struct(data)
        elif payload_type == 'block':
            tx = Block.create_from_struct(data)
        else:
            raise ValueError('Unknown payload load')

        if self.protocol.manager.tx_storage.get_genesis_by_hash_bytes(tx.hash):
            print('!!! WE JUST GOT A GENESIS')
            return
        self.protocol.manager.on_new_tx(tx, conn=self.protocol)

    def send_get_best_height(self):
        self.send_message(self.ProtocolCommand.GET_BEST_HEIGHT)

    def handle_get_best_height(self, unused_payload):
        print('handle_get_best_height')
        payload = self.protocol.manager.tx_storage.get_best_height()
        self.send_message(self.ProtocolCommand.BEST_HEIGHT, str(payload))

    def handle_best_height(self, payload):
        print('handle_best_height:', payload)
        best_height = int(payload)
        self.protocol.manager.on_best_height(best_height, conn=self.protocol)

    def send_get_blocks(self):
        payload = self.protocol.manager.tx_storage.get_latest_block().hash_hex
        self.send_message(self.ProtocolCommand.GET_BLOCKS, payload)

    def handle_get_blocks(self, payload):
        print('handle_get_blocks;', payload)
        block_hashes = self.protocol.manager.tx_storage.get_block_hashes_after(payload)
        block_hashes_hex = [x.hex() for x in block_hashes]
        output_payload = json.dumps(block_hashes_hex)
        self.send_message(self.ProtocolCommand.BLOCKS, output_payload)

    def handle_blocks(self, payload):
        print('handle_blocks')
        block_hashes = json.loads(payload)
        self.protocol.manager.on_block_hashes_received(block_hashes, conn=self.protocol)

    def send_get_peers(self):
        """ Send a GET-PEERS command, requesting a list of nodes.
        """
        self.send_message(self.ProtocolCommand.GET_PEERS)

    def handle_get_peers(self, payload):
        """ Executed when a GET-PEERS command is received. It just responds with
        a list of all known peers.
        """
        self.send_peers()

    def send_peers(self):
        """ Send a PEERS command with a list of all known peers.
        """
        peers = []
        for conn in self.protocol.manager.connected_peers.values():
            peers.append({
                'id': conn.peer.id,
                'entrypoints': conn.peer.entrypoints,
                'last_message': conn.last_message,
            })
        self.send_message(self.ProtocolCommand.PEERS, json.dumps(peers))
        print('Peers: %s' % str(peers))

    def handle_peers(self, payload):
        """ Executed when a PEERS command is received. It updates the list
        of known peers (and tries to connect to new ones).
        """
        received_peers = json.loads(payload)
        for data in received_peers:
            peer = PeerId.create_from_json(data)
            peer.validate()
            self.protocol.manager.update_peer(peer)
        remote = self.protocol.transport.getPeer()
        print(remote, 'PEERS', payload)

    def send_ping_if_necessary(self):
        """ Send a PING command if the connection has been idle for 3 seconds or more.
        """
        dt = time.time() - self.protocol.last_message
        if dt > 3:
            self.send_ping()

    def send_ping(self):
        """ Send a PING command. Usually you would use `send_ping_if_necessary` to
        prevent wasting bandwidth.
        """
        self.send_message(self.ProtocolCommand.PING)

    def send_pong(self):
        """ Send a PONG command as a response to a PING command.
        """
        self.send_message(self.ProtocolCommand.PONG)

    def handle_ping(self, payload):
        """ Executed when a PING command is received. It responds with a
        PONG message.
        """
        self.send_pong()

    def handle_pong(self, payload):
        """ Executed when a PONG message is received. It only updates
        the last time a message has been received by this peer.
        """
        self.protocol.last_message = time.time()
