from hathor.conf import HathorSettings
from hathor.p2p.messages import ProtocolMessages
from hathor.p2p.states import ReadyState
from hathor.p2p.states.ready import BlockInfo
from hathor.simulator import FakeConnection
from hathor.simulator.trigger import StopAfterNMinedBlocks
from tests import unittest
from tests.simulation.base import SimulatorTestCase

settings = HathorSettings()


class BaseGetBestBlockchainTestCase(SimulatorTestCase):

    def _send_cmd(self, proto, cmd, payload=None):
        if not payload:
            line = '{}\r\n'.format(cmd)
        else:
            line = '{} {}\r\n'.format(cmd, payload)

        if isinstance(line, str):
            line = line.encode('utf-8')

        return proto.dataReceived(line)

    def test_get_best_blockchain(self):
        manager1 = self.create_peer()
        manager2 = self.create_peer()
        conn12 = FakeConnection(manager1, manager2, latency=0.05)
        self.simulator.add_connection(conn12)
        self.simulator.run(3600)

        connected_peers1 = list(manager1.connections.connected_peers.values())
        connected_peers2 = list(manager2.connections.connected_peers.values())
        self.assertEqual(1, len(connected_peers1))
        self.assertEqual(1, len(connected_peers2))

        # assert the protocol has capabilities
        # HelloState is responsible to transmite to protocol the capabilities
        protocol1 = connected_peers2[0]
        protocol2 = connected_peers1[0]
        self.assertIsNotNone(protocol1.capabilities)
        self.assertIsNotNone(protocol2.capabilities)

        # assert the protocol has the GET_BEST_BLOCKCHAIN capability
        self.assertIn(settings.CAPABILITY_GET_BEST_BLOCKCHAIN, protocol1.capabilities)
        self.assertIn(settings.CAPABILITY_GET_BEST_BLOCKCHAIN, protocol2.capabilities)

        # assert the protocol is in ReadyState
        state1 = protocol1.state
        state2 = protocol2.state
        self.assertIsInstance(state1, ReadyState)
        self.assertIsInstance(state2, ReadyState)

        # assert ReadyState commands
        self.assertIn(ProtocolMessages.GET_BEST_BLOCKCHAIN, state1.cmd_map)
        self.assertIn(ProtocolMessages.BEST_BLOCKCHAIN, state1.cmd_map)
        self.assertIn(ProtocolMessages.GET_BEST_BLOCKCHAIN, state2.cmd_map)
        self.assertIn(ProtocolMessages.BEST_BLOCKCHAIN, state2.cmd_map)

        # assert best blockchain
        self.assertIsNotNone(state1.best_blockchain)
        self.assertIsNotNone(state2.best_blockchain)

        # mine 100 blocks
        miner = self.simulator.create_miner(manager1, hashpower=1e6)
        miner.start()
        trigger = StopAfterNMinedBlocks(miner, quantity=100)
        self.assertTrue(self.simulator.run(7200, trigger=trigger))
        miner.stop()

        # assert best blockchain exchange
        state1.send_get_best_blockchain()
        state2.send_get_best_blockchain()
        self.simulator.run(60)
        self.assertEqual(10, len(state1.best_blockchain))
        self.assertEqual(10, len(state2.best_blockchain))

        self.assertIsInstance(state1.best_blockchain[0], BlockInfo)
        self.assertIsInstance(state2.best_blockchain[0], BlockInfo)

    def test_handle_get_best_blockchain(self):
        manager1 = self.create_peer()
        manager2 = self.create_peer()
        conn12 = FakeConnection(manager1, manager2, latency=0.05)
        self.simulator.add_connection(conn12)

        # mine 100 blocks
        miner = self.simulator.create_miner(manager1, hashpower=1e6)
        miner.start()
        trigger = StopAfterNMinedBlocks(miner, quantity=100)
        self.assertTrue(self.simulator.run(7200, trigger=trigger))
        miner.stop()

        connected_peers1 = list(manager1.connections.connected_peers.values())
        self.assertEqual(1, len(connected_peers1))
        protocol2 = connected_peers1[0]
        state2 = protocol2.state
        self.assertIsInstance(state2, ReadyState)

        connected_peers2 = list(manager2.connections.connected_peers.values())
        self.assertEqual(1, len(connected_peers2))
        protocol1 = connected_peers2[0]
        state1 = protocol1.state
        self.assertIsInstance(state1, ReadyState)

        # assert compliance with N blocks inside the boundaries
        state1.send_get_best_blockchain(nBlocks='1')
        self.simulator.run(60)
        self.assertFalse(conn12.tr1.disconnecting)

        state2.send_get_best_blockchain(nBlocks='100')
        self.simulator.run(60)
        self.assertFalse(conn12.tr2.disconnecting)

        # assert compliance with N blocks under lower boundary
        state1.send_get_best_blockchain(nBlocks='0')
        self.simulator.run(60)
        self.assertTrue(conn12.tr1.disconnecting)

        # assert compliance with N blocks beyond upper boundary
        state2.send_get_best_blockchain(nBlocks='101')
        self.simulator.run(60)
        self.assertTrue(conn12.tr2.disconnecting)


class SyncV1GetBestBlockchainTestCase(unittest.SyncV1Params, BaseGetBestBlockchainTestCase):
    __test__ = True


class SyncV2GetBestBlockchainTestCase(unittest.SyncV2Params, BaseGetBestBlockchainTestCase):
    __test__ = True


# sync-bridge should behave like sync-v2
class SyncBridgeGetBestBlockchainTestCase(unittest.SyncBridgeParams, BaseGetBestBlockchainTestCase):
    __test__ = True
