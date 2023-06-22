# Copyright 2021 Hathor Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
import json
import math
import struct
from collections import OrderedDict
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Generator, NamedTuple, Optional, cast

from structlog import get_logger
from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.internet.task import LoopingCall

from hathor.conf import HathorSettings
from hathor.p2p.messages import ProtocolMessages
from hathor.p2p.sync_manager import SyncManager
from hathor.p2p.sync_v2.mempool import SyncMempoolManager
from hathor.p2p.sync_v2.streamers import DEFAULT_STREAMING_LIMIT, BlockchainStreaming, StreamEnd, TransactionsStreaming
from hathor.transaction import BaseTransaction, Block, Transaction
from hathor.transaction.base_transaction import tx_or_block_from_bytes
from hathor.transaction.exceptions import HathorError
from hathor.transaction.storage.exceptions import TransactionDoesNotExist
from hathor.types import VertexId
from hathor.util import Reactor

if TYPE_CHECKING:
    from hathor.p2p.protocol import HathorProtocol

settings = HathorSettings()
logger = get_logger()


class BlockInfo(NamedTuple):
    id: VertexId
    height: int

    def __repr__(self):
        return f'BlockInfo({self.height}, {self.id.hex()})'


class PeerState(Enum):
    ERROR = 'error'
    UNKNOWN = 'unknown'
    SYNCING_BLOCKS = 'syncing-blocks'
    SYNCING_TRANSACTIONS = 'syncing-transactions'
    SYNCING_MEMPOOL = 'syncing-mempool'


class NodeBlockSync(SyncManager):
    """ An algorithm to sync the Blockchain between two peers.
    """
    name: str = 'node-block-sync'

    def __init__(self, protocol: 'HathorProtocol', reactor: Optional[Reactor] = None) -> None:
        """
        :param protocol: Protocol of the connection.
        :type protocol: HathorProtocol

        :param reactor: Reactor to schedule later calls. (default=twisted.internet.reactor)
        :type reactor: Reactor
        """
        self.protocol = protocol
        self.manager = protocol.node
        self.tx_storage = protocol.node.tx_storage
        self.state = PeerState.UNKNOWN

        self.DEFAULT_STREAMING_LIMIT = DEFAULT_STREAMING_LIMIT

        if reactor is None:
            from hathor.util import reactor as twisted_reactor
            reactor = twisted_reactor
        assert reactor is not None
        self.reactor: Reactor = reactor
        self._is_streaming: bool = False

        # Create logger with context
        self.log = logger.new(peer=self.protocol.get_short_peer_id())

        # Extra
        self._blk_size = 0
        self._blk_end_hash = settings.GENESIS_BLOCK_HASH
        self._blk_max_quantity = 0

        # indicates whether we're receiving a stream from the peer
        self.receiving_stream = False

        # highest block where we are synced
        self.synced_block: Optional[BlockInfo] = None

        # highest block peer has
        self.peer_best_block: Optional[BlockInfo] = None

        # Latest deferred waiting for a reply.
        self.deferred_by_key: dict[str, Deferred] = {}

        # When syncing blocks we start streaming with all peers
        # so the moment I get some repeated blocks, I stop the download
        # because it's probably a streaming that I've just received
        self.max_repeated_blocks = 10

        # Streaming objects
        self.blockchain_streaming: Optional[BlockchainStreaming] = None
        self.transactions_streaming: Optional[TransactionsStreaming] = None

        # Whether the peers are synced, i.e. our best height and best block are the same
        self._synced = False

        # Indicate whether the sync manager has been started.
        self._started: bool = False

        # Saves the last received block from the block streaming # this is useful to be used when running the sync of
        # transactions in the case when I am downloading a side chain. Starts at the genesis, which is common to all
        # peers on the network
        self._last_received_block: Optional[Block] = None

        # Saves if I am in the middle of a mempool sync
        # we don't execute any sync while in the middle of it
        self.mempool_manager = SyncMempoolManager(self)
        self._receiving_tips: Optional[list[bytes]] = None

        # Cache for get_tx calls
        self._get_tx_cache: OrderedDict[bytes, BaseTransaction] = OrderedDict()
        self._get_tx_cache_maxsize = 1000

        # Looping call of the main method
        self._lc_run = LoopingCall(self.run_sync)
        self._lc_run.clock = self.reactor
        self._is_running = False

        # Whether we propagate transactions or not
        self._is_relaying = False

        # This stores the final height that we expect the last "get blocks" stream to end on
        self._blk_end_height: Optional[int] = None

        # Whether to sync with this peer
        self._is_enabled: bool = False

    def get_status(self) -> dict[str, Any]:
        """ Return the status of the sync.
        """
        res = {
            'is_enabled': self.is_sync_enabled(),
            'peer_best_block': self.peer_best_block,
            'synced_block': self.synced_block,
            'synced': self._synced,
            'state': self.state.value,
        }
        return res

    def is_synced(self) -> bool:
        return self._synced

    def is_errored(self) -> bool:
        return self.state is PeerState.ERROR

    def is_sync_enabled(self) -> bool:
        return self._is_enabled

    def enable_sync(self) -> None:
        self._is_enabled = True

    def disable_sync(self) -> None:
        self._is_enabled = False

    def send_tx_to_peer_if_possible(self, tx: BaseTransaction) -> None:
        if not self._is_enabled:
            self.log.debug('sync is disabled')
            return
        if not self.is_synced():
            # XXX Should we accept any tx while I am not synced?
            return

        # XXX When we start having many txs/s this become a performance issue
        # Then we could change this to be a streaming of real time data with
        # blocks as priorities to help miners get the blocks as fast as we can
        # We decided not to implement this right now because we already have some producers
        # being used in the sync algorithm and the code was becoming a bit too complex
        if self._is_relaying:
            self.send_data(tx)

    def is_started(self) -> bool:
        return self._started

    def start(self) -> None:
        """ Start sync.
        """
        if self._started:
            raise Exception('NodeSyncBlock is already running')
        self._started = True
        self._lc_run.start(5)

    def stop(self) -> None:
        """ Stop sync.
        """
        if not self._started:
            raise Exception('NodeSyncBlock is already stopped')
        self._started = False
        self._lc_run.stop()

    def get_cmd_dict(self) -> dict[ProtocolMessages, Callable[[str], None]]:
        """ Return a dict of messages of the plugin.

        For further information about each message, see the RFC.
        Link: https://github.com/HathorNetwork/rfcs/blob/master/text/0025-p2p-sync-v2.md#p2p-sync-protocol-messages
        """
        return {
            ProtocolMessages.GET_NEXT_BLOCKS: self.handle_get_next_blocks,
            ProtocolMessages.GET_PREV_BLOCKS: self.handle_get_prev_blocks,
            ProtocolMessages.BLOCKS: self.handle_blocks,
            ProtocolMessages.BLOCKS_END: self.handle_blocks_end,
            ProtocolMessages.GET_BEST_BLOCK: self.handle_get_best_block,
            ProtocolMessages.BEST_BLOCK: self.handle_best_block,
            ProtocolMessages.GET_BLOCK_TXS: self.handle_get_block_txs,
            ProtocolMessages.GET_TRANSACTIONS_BFS: self.handle_get_transactions_bfs,
            ProtocolMessages.TRANSACTION: self.handle_transaction,
            ProtocolMessages.TRANSACTIONS_END: self.handle_transactions_end,
            ProtocolMessages.GET_PEER_BLOCK_HASHES: self.handle_get_peer_block_hashes,
            ProtocolMessages.PEER_BLOCK_HASHES: self.handle_peer_block_hashes,
            ProtocolMessages.STOP_BLOCK_STREAMING: self.handle_stop_block_streaming,
            ProtocolMessages.GET_TIPS: self.handle_get_tips,
            ProtocolMessages.TIPS: self.handle_tips,
            ProtocolMessages.TIPS_END: self.handle_tips_end,
            # XXX: overriding ReadyState.handle_error
            ProtocolMessages.ERROR: self.handle_error,
            ProtocolMessages.GET_DATA: self.handle_get_data,
            ProtocolMessages.DATA: self.handle_data,
            ProtocolMessages.RELAY: self.handle_relay,
            ProtocolMessages.NOT_FOUND: self.handle_not_found,
        }

    def handle_not_found(self, payload: str) -> None:
        """ Handle a received NOT-FOUND message.
        """
        # XXX: NOT_FOUND is a valid message, but we shouldn't ever receive it unless the other peer is running with a
        #                modified code or if there is a bug
        self.log.warn('not found? close connection', payload=payload)
        self.protocol.send_error_and_close_connection('Unexpected NOT_FOUND')

    def handle_error(self, payload: str) -> None:
        """ Override protocols original handle_error so we can recover a sync in progress.
        """
        assert self.protocol.connections is not None
        # forward message to overloaded handle_error:
        self.protocol.handle_error(payload)

    def update_synced(self, synced: bool) -> None:
        self._synced = synced

    @inlineCallbacks
    def run_sync(self) -> Generator[Any, Any, None]:
        if not self._is_enabled:
            self.log.debug('sync is disabled')
            return
        if self._is_running:
            # Already running...
            self.log.debug('already running')
            return
        self._is_running = True
        try:
            yield self._run_sync()
        finally:
            self._is_running = False

    @inlineCallbacks
    def _run_sync(self) -> Generator[Any, Any, None]:
        """Run sync. This is the entrypoint for the sync.
        It is always safe to call this method.
        """
        yield self.run_sync_blocks()
        return

        if self.receiving_stream:
            # If we're receiving a stream, wait for it to finish before running sync.
            # If we're sending a stream, do the sync to update the peer's synced block
            self.log.debug('receiving stream, try again later')
            return

        if self.mempool_manager.is_running():
            # It's running a mempool sync, so we wait until it finishes
            self.log.debug('running mempool sync, try again later')
            return

        bestblock = self.tx_storage.get_best_block()
        meta = bestblock.get_metadata()

        self.log.debug('run sync', height=meta.height)

        assert self.protocol.connections is not None
        assert self.tx_storage.indexes is not None
        assert self.tx_storage.indexes.deps is not None

        if self.tx_storage.indexes.deps.has_needed_tx():
            self.log.debug('needed tx exist, sync transactions')
            self.update_synced(False)
            # TODO: find out whether we can sync transactions from this peer to speed things up
            self.run_sync_transactions()
        else:
            # I am already in sync with all checkpoints, sync next blocks
            pass

    def run_sync_transactions(self) -> None:
        self.state = PeerState.SYNCING_TRANSACTIONS

        assert self.protocol.connections is not None
        assert self.tx_storage.indexes is not None
        assert self.tx_storage.indexes.deps is not None

        start_hash = self.tx_storage.indexes.deps.get_next_needed_tx()

        # Start with the last received block and find the best block full validated in its chain
        block = self._last_received_block
        if block is None:
            block = cast(Block, self.tx_storage.get_genesis(settings.GENESIS_BLOCK_HASH))
        else:
            with self.tx_storage.allow_partially_validated_context():
                while not block.get_metadata().validation.is_valid():
                    block = block.get_block_parent()
        assert block is not None
        assert block.hash is not None
        block_height = block.get_height()

        self.log.info('run sync transactions', start=start_hash.hex(), end_block_hash=block.hash.hex(),
                      end_block_height=block_height)
        self.send_get_transactions_bfs([start_hash], block.hash)

    @inlineCallbacks
    def run_sync_blocks(self) -> Generator[Any, Any, None]:
        assert self.tx_storage.indexes is not None
        self.state = PeerState.SYNCING_BLOCKS

        # Find peer's best block
        self.peer_best_block = yield self.get_peer_best_block()
        assert self.peer_best_block is not None

        # My height
        bestblock = self.tx_storage.get_best_block()
        assert bestblock.hash is not None
        meta = bestblock.get_metadata()
        assert not meta.voided_by
        assert meta.validation.is_fully_connected()
        my_best_block = BlockInfo(bestblock.hash, bestblock.get_height())

        # Are we synced?
        if self.peer_best_block == my_best_block:
            # Yes, we are synced! \o/
            self.log.info('blocks are synced', best_block=my_best_block)
            self.update_synced(True)
            self.send_relay(enable=True)
            self.synced_block = my_best_block
            return

        if self.peer_best_block.height <= my_best_block.height:
            # Is peer behind me at the same blockchain?
            common_block_bytes = self.tx_storage.indexes.height.get(self.peer_best_block.height)
            if common_block_bytes == self.peer_best_block.id:
                # Nothing to sync from this peer.
                self.log.info('nothing to sync because peer is behind me at the same best blockchain',
                              my_best_block=my_best_block, peer_best_block=self.peer_best_block)
                self.update_synced(True)
                self.send_relay(enable=True)
                self.synced_block = self.peer_best_block
                return
        # TODO: validate if this is when we should disable relaying
        elif self._is_relaying:
            self.send_relay(enable=False)

        # If we reach this point, we need to sync.
        self.update_synced(False)

        self.log.debug('syncing blocks',
                       my_best_block=my_best_block,
                       peer_best_block=self.peer_best_block)

        # Find best common block
        self.synced_block = yield self.find_best_common_block(my_best_block, self.peer_best_block)
        assert self.synced_block is not None
        self.log.debug('sync blocks',
                       my_best_block=my_best_block,
                       peer_best_block=self.peer_best_block,
                       synced_block=self.synced_block)

        self.run_block_sync(
            self.synced_block.id, self.synced_block.height,
            self.peer_best_block.id, self.peer_best_block.height
        )

        return

        # if self.synced_height < self.peer_best_height:
        #     # sync from common block
        #     peer_block_at_height = yield self.get_peer_block_hashes([self.synced_height])
        #     self.run_block_sync(peer_block_at_height[0][1], self.synced_height, peer_best_block, peer_best_height)
        # elif my_height == self.synced_height == self.peer_best_height:
        #     # we're synced and on the same height, get their mempool
        #     self.state = PeerState.SYNCING_MEMPOOL
        #     self.mempool_manager.run()
        # else:
        #     # we got all the peer's blocks but aren't on the same height, nothing to do
        #     pass

    # --------------------------------------------
    # BEGIN: GET_TIPS/TIPS/TIPS_END implementation
    # --------------------------------------------

    def get_tips(self) -> Deferred[list[bytes]]:
        """Async method to request the tips, returned hashes guaranteed to be new"""
        key = 'tips'
        deferred = self.deferred_by_key.get(key, None)
        if deferred is None:
            deferred = self.deferred_by_key[key] = Deferred()
            self.send_get_tips()
        else:
            assert self._receiving_tips is not None
        return deferred

    def send_get_tips(self) -> None:
        self.log.debug('get tips')
        self.send_message(ProtocolMessages.GET_TIPS)
        self._receiving_tips = []

    def handle_get_tips(self, payload: str) -> None:
        """Handle a received GET_TIPS message."""
        assert self.tx_storage.indexes is not None
        assert self.tx_storage.indexes.mempool_tips is not None
        if self._is_streaming:
            self.log.warn('can\'t send while streaming')  # XXX: or can we?
            self.send_message(ProtocolMessages.MEMPOOL_END)
            return
        self.log.debug('handle_get_tips')
        # TODO Use a streaming of tips
        for txid in self.tx_storage.indexes.mempool_tips.get():
            self.send_tips(txid)
        self.send_message(ProtocolMessages.TIPS_END)

    def send_tips(self, tx_id: bytes) -> None:
        """Send a TIPS message."""
        self.send_message(ProtocolMessages.TIPS, json.dumps([tx_id.hex()]))

    def handle_tips(self, payload: str) -> None:
        """Handle a received TIPS message."""
        self.log.debug('tips', receiving_tips=self._receiving_tips)
        if self._receiving_tips is None:
            self.protocol.send_error_and_close_connection('TIPS not expected')
            return
        data = json.loads(payload)
        data = [bytes.fromhex(x) for x in data]
        # filter-out txs we already have
        self._receiving_tips.extend(tx_id for tx_id in data if not self.partial_vertex_exists(tx_id))

    def handle_tips_end(self, payload: str) -> None:
        """Handle a received TIPS-END message."""
        assert self._receiving_tips is not None
        key = 'tips'
        deferred = self.deferred_by_key.pop(key, None)
        if deferred is None:
            self.protocol.send_error_and_close_connection('TIPS-END not expected')
            return
        deferred.callback(self._receiving_tips)
        self._receiving_tips = None

    # ------------------------------------------
    # END: GET_TIPS/TIPS/TIPS_END implementation
    # ------------------------------------------

    def send_relay(self, *, enable: bool = True) -> None:
        self.log.debug('send_relay', enable=enable)
        self.send_message(ProtocolMessages.RELAY, json.dumps(enable))

    def handle_relay(self, payload: str) -> None:
        """Handle a received RELAY message."""
        if not payload:
            # XXX: "legacy" nothing means enable
            self._is_relaying = True
        else:
            val = json.loads(payload)
            if isinstance(val, bool):
                self._is_relaying = val
            else:
                self.protocol.send_error_and_close_connection('RELAY: invalid value')
                return

    def _setup_block_streaming(self, start_hash: bytes, start_height: int, end_hash: bytes, end_height: int,
                               reverse: bool) -> None:
        self._blk_start_hash = start_hash
        self._blk_start_height = start_height
        self._blk_end_hash = end_hash
        self._blk_end_height = end_height
        self._blk_received = 0
        self._blk_repeated = 0
        raw_quantity = end_height - start_height + 1
        self._blk_max_quantity = -raw_quantity if reverse else raw_quantity
        self._blk_prev_hash: Optional[bytes] = None
        self._blk_stream_reverse = reverse
        self._last_received_block = None

    def run_block_sync(self, start_hash: bytes, start_height: int, end_hash: bytes, end_height: int) -> None:
        """Called when the bestblock is after all checkpoints.
        It must syncs to the left until it reaches the remote's best block or the max stream limit.
        """
        self._setup_block_streaming(start_hash, start_height, end_hash, end_height, False)
        quantity = end_height - start_height
        self.log.info('get next blocks', start_height=start_height, end_height=end_height, quantity=quantity,
                      start_hash=start_hash.hex(), end_hash=end_hash.hex())
        self.send_get_next_blocks(start_hash, end_hash)

    def send_message(self, cmd: ProtocolMessages, payload: Optional[str] = None) -> None:
        """ Helper to send a message.
        """
        assert self.protocol.state is not None
        self.protocol.state.send_message(cmd, payload)

    def partial_vertex_exists(self, vertex_id: VertexId) -> bool:
        """Return true if the vertex exists no matter its validation state."""
        with self.tx_storage.allow_partially_validated_context():
            return self.tx_storage.transaction_exists(vertex_id)

    @inlineCallbacks
    def find_best_common_block(self, my_best_block: BlockInfo, peer_best_block: BlockInfo
                               ) -> Generator[Any, Any, BlockInfo]:
        """ Search for the highest block/height where we're synced.
        """
        assert self.tx_storage.indexes is not None

        # Run an n-ary search in the interval [lo, hi).
        # `lo` is always a height we are synced.
        # `hi` is always a height where sync state is unknown.
        lo = self.synced_block.height if self.synced_block else 0
        hi = min(my_best_block.height, peer_best_block.height)
        if hi == 0:
            hi = 1
        assert hi > lo

        common_block: BlockInfo

        while True:
            step = math.ceil((hi - lo) / 10)
            heights = list(range(lo, hi, step))
            heights.append(hi)
            self.log.debug('n-ary search query', lo=lo, hi=hi, heights=heights)

            block_list = yield self.get_peer_block_hashes(heights)
            block_list.sort(key=lambda x: x.height, reverse=True)
            self.log.debug('n-ary search answer', block_list)
            for info in block_list:
                try:
                    # We must check only fully validated transactions.
                    blk = self.tx_storage.get_transaction(info.id)
                except TransactionDoesNotExist:
                    hi = info.height
                else:
                    assert isinstance(blk, Block)
                    assert blk.get_metadata().validation.is_fully_connected()
                    assert info.height == blk.get_height()
                    lo = info.height
                    common_block = info
                    break
            else:
                assert False, 'should never reach here'

            if hi - lo <= 1:
                break

        assert hi - lo == 1
        self.log.debug('find_best_common_block', lo=lo, hi=hi, common_block=common_block)
        return common_block

    def get_peer_block_hashes(self, heights: list[int]) -> Deferred[list[BlockInfo]]:
        """ Returns the peer's block hashes in the given heights.
        """
        key = 'peer-block-hashes'
        if self.deferred_by_key.get(key, None) is not None:
            raise Exception('latest_deferred is not None')
        self.send_get_peer_block_hashes(heights)
        deferred: Deferred[list[BlockInfo]] = Deferred()
        self.deferred_by_key[key] = deferred
        return deferred

    def send_get_peer_block_hashes(self, heights: list[int]) -> None:
        payload = json.dumps(heights)
        self.send_message(ProtocolMessages.GET_PEER_BLOCK_HASHES, payload)

    def handle_get_peer_block_hashes(self, payload: str) -> None:
        assert self.tx_storage.indexes is not None
        heights = json.loads(payload)
        if len(heights) > 20:
            self.protocol.send_error_and_close_connection('GET-PEER-BLOCK-HASHES: too many heights')
            return
        data = []
        for h in heights:
            blk_hash = self.tx_storage.indexes.height.get(h)
            if blk_hash is None:
                break
            blk = self.tx_storage.get_transaction(blk_hash)
            if blk.get_metadata().voided_by:
                # The height index might have voided blocks when there is a draw.
                # Let's try again soon.
                self.reactor.callLater(3, self.handle_get_peer_block_hashes, payload)
                return
            data.append((h, blk_hash.hex()))
        payload = json.dumps(data)
        self.send_message(ProtocolMessages.PEER_BLOCK_HASHES, payload)

    def handle_peer_block_hashes(self, payload: str) -> None:
        data = json.loads(payload)
        data = [BlockInfo(height=h, id=bytes.fromhex(block_hash)) for (h, block_hash) in data]
        key = 'peer-block-hashes'
        deferred = self.deferred_by_key.pop(key, None)
        if deferred:
            deferred.callback(data)

    def send_get_next_blocks(self, start_hash: bytes, end_hash: bytes) -> None:
        payload = json.dumps(dict(
            start_hash=start_hash.hex(),
            end_hash=end_hash.hex(),
        ))
        self.send_message(ProtocolMessages.GET_NEXT_BLOCKS, payload)
        self.receiving_stream = True

    def handle_get_next_blocks(self, payload: str) -> None:
        self.log.debug('handle GET-NEXT-BLOCKS')
        if self._is_streaming:
            self.protocol.send_error_and_close_connection('GET-NEXT-BLOCKS received before previous one finished')
            return
        data = json.loads(payload)
        self.send_next_blocks(
            start_hash=bytes.fromhex(data['start_hash']),
            end_hash=bytes.fromhex(data['end_hash']),
        )

    def send_next_blocks(self, start_hash: bytes, end_hash: bytes) -> None:
        self.log.debug('start GET-NEXT-BLOCKS stream response')
        # XXX If I don't have this block it will raise TransactionDoesNotExist error. Should I handle this?
        blk = self.tx_storage.get_transaction(start_hash)
        assert isinstance(blk, Block)
        assert blk.hash is not None
        assert not blk.get_metadata().voided_by, f'{blk.hash.hex()}'
        if self.blockchain_streaming is not None and self.blockchain_streaming.is_running:
            self.blockchain_streaming.stop()
        self.blockchain_streaming = BlockchainStreaming(self, blk, end_hash, limit=self.DEFAULT_STREAMING_LIMIT)
        self.blockchain_streaming.start()

    def send_get_prev_blocks(self, start_hash: bytes, end_hash: bytes) -> None:
        payload = json.dumps(dict(
            start_hash=start_hash.hex(),
            end_hash=end_hash.hex(),
        ))
        self.send_message(ProtocolMessages.GET_PREV_BLOCKS, payload)
        self.receiving_stream = True

    def handle_get_prev_blocks(self, payload: str) -> None:
        self.log.debug('handle GET-PREV-BLOCKS')
        if self._is_streaming:
            self.protocol.send_error_and_close_connection('GET-PREV-BLOCKS received before previous one finished')
            return
        data = json.loads(payload)
        self.send_prev_blocks(
            start_hash=bytes.fromhex(data['start_hash']),
            end_hash=bytes.fromhex(data['end_hash']),
        )

    def send_prev_blocks(self, start_hash: bytes, end_hash: bytes) -> None:
        self.log.debug('start GET-PREV-BLOCKS stream response')
        # XXX If I don't have this block it will raise TransactionDoesNotExist error. Should I handle this?
        # TODO
        blk = self.tx_storage.get_transaction(start_hash)
        assert isinstance(blk, Block)
        if self.blockchain_streaming is not None and self.blockchain_streaming.is_running:
            self.blockchain_streaming.stop()
        self.blockchain_streaming = BlockchainStreaming(self, blk, end_hash, reverse=True,
                                                        limit=self.DEFAULT_STREAMING_LIMIT)
        self.blockchain_streaming.start()

    def send_blocks(self, blk: Block) -> None:
        """Send a BLOCK message."""
        # Uncomment the following line to improve debugging:
        # self.log.debug('sending block to peer', block=blk.hash_hex)
        payload = base64.b64encode(bytes(blk)).decode('ascii')
        self.send_message(ProtocolMessages.BLOCKS, payload)

    def send_blocks_end(self, response_code: StreamEnd) -> None:
        payload = str(int(response_code))
        self.log.debug('send BLOCKS-END', payload=payload)
        self.send_message(ProtocolMessages.BLOCKS_END, payload)

    def handle_blocks_end(self, payload: str) -> None:
        self.log.debug('recv BLOCKS-END', payload=payload, size=self._blk_size)

        response_code = StreamEnd(int(payload))
        self.receiving_stream = False
        assert self.protocol.connections is not None

        if self.state is not PeerState.SYNCING_BLOCKS:
            self.log.error('unexpected BLOCKS-END', state=self.state)
            self.protocol.send_error_and_close_connection('Not expecting to receive BLOCKS-END message')
            return

        self.log.debug('block streaming ended', reason=str(response_code))

    def handle_blocks(self, payload: str) -> None:
        """Handle a received BLOCK message."""
        if self.state is not PeerState.SYNCING_BLOCKS:
            self.log.error('unexpected BLOCK', state=self.state)
            self.protocol.send_error_and_close_connection('Not expecting to receive BLOCK message')
            return

        assert self.protocol.connections is not None

        blk_bytes = base64.b64decode(payload)
        blk = tx_or_block_from_bytes(blk_bytes)
        if not isinstance(blk, Block):
            # Not a block. Punish peer?
            return
        blk.storage = self.tx_storage

        assert blk.hash is not None

        self._blk_received += 1
        if self._blk_received > self._blk_max_quantity + 1:
            self.log.warn('too many blocks received', last_block=blk.hash_hex)
            # Too many blocks. Punish peer?
            self.state = PeerState.ERROR
            return

        if self.partial_vertex_exists(blk.hash):
            # We reached a block we already have. Skip it.
            self._blk_prev_hash = blk.hash
            self._blk_repeated += 1
            if self.receiving_stream and self._blk_repeated > self.max_repeated_blocks:
                self.log.debug('repeated block received', total_repeated=self._blk_repeated)
                self.handle_many_repeated_blocks()

        # basic linearity validation, crucial for correctly predicting the next block's height
        if self._blk_stream_reverse:
            if self._last_received_block and blk.hash != self._last_received_block.get_block_parent_hash():
                self.handle_invalid_block('received block is not parent of previous block')
                return
        else:
            if self._last_received_block and blk.get_block_parent_hash() != self._last_received_block.hash:
                self.handle_invalid_block('received block is not child of previous block')
                return

        try:
            # this methods takes care of checking if the block already exists,
            # it will take care of doing at least a basic validation
            # self.log.debug('add new block', block=blk.hash_hex)
            if self.partial_vertex_exists(blk.hash):
                # XXX: early terminate?
                self.log.debug('block early terminate?', blk_id=blk.hash.hex())
            else:
                self.log.debug('block received', blk_id=blk.hash.hex())
                self.on_new_tx(blk, propagate_to_peers=False, quiet=True)
        except HathorError:
            self.handle_invalid_block(exc_info=True)
            return
        else:
            self._last_received_block = blk
            self._blk_repeated = 0
            # XXX: debugging log, maybe add timing info
            if self._blk_received % 500 == 0:
                self.log.debug('block streaming in progress', blocks_received=self._blk_received)

    def handle_invalid_block(self, msg: Optional[str] = None, *, exc_info: bool = False) -> None:
        """ Call this method when receiving an invalid block.
        """
        kwargs: dict[str, Any] = {}
        if msg is not None:
            kwargs['error'] = msg
        if exc_info:
            kwargs['exc_info'] = True
        self.log.warn('invalid new block', **kwargs)
        # Invalid block?!
        self.state = PeerState.ERROR

    def handle_many_repeated_blocks(self) -> None:
        """ Method called when a block stream received many repeated blocks
            so I must stop the stream and reschedule to continue the sync with this peer later
        """
        self.send_stop_block_streaming()
        self.receiving_stream = False

    def send_stop_block_streaming(self) -> None:
        self.send_message(ProtocolMessages.STOP_BLOCK_STREAMING)

    def handle_stop_block_streaming(self, payload: str) -> None:
        if not self.blockchain_streaming or not self._is_streaming:
            self.log.debug('got stop streaming message with no streaming running')
            return

        self.log.debug('got stop streaming message')
        self.blockchain_streaming.stop()
        self.blockchain_streaming = None

    def get_peer_best_block(self) -> Deferred[BlockInfo]:
        key = 'best-block'
        deferred = self.deferred_by_key.pop(key, None)
        if self.deferred_by_key.get(key, None) is not None:
            raise Exception('latest_deferred is not None')

        self.send_get_best_block()
        deferred = Deferred()
        self.deferred_by_key[key] = deferred
        return deferred

    def send_get_best_block(self) -> None:
        self.send_message(ProtocolMessages.GET_BEST_BLOCK)

    def handle_get_best_block(self, payload: str) -> None:
        best_block = self.tx_storage.get_best_block()
        meta = best_block.get_metadata()
        assert meta.validation.is_fully_connected()
        assert not meta.voided_by
        data = {'block': best_block.hash_hex, 'height': meta.height}
        self.send_message(ProtocolMessages.BEST_BLOCK, json.dumps(data))

    def handle_best_block(self, payload: str) -> None:
        data = json.loads(payload)

        _id = bytes.fromhex(data['block'])
        height = data['height']
        best_block = BlockInfo(id=_id, height=height)

        key = 'best-block'
        deferred = self.deferred_by_key.pop(key, None)
        if deferred:
            deferred.callback(best_block)

    def _setup_tx_streaming(self):
        self._tx_received = 0
        self._tx_max_quantity = DEFAULT_STREAMING_LIMIT  # XXX: maybe this is redundant
        # XXX: what else can we add for checking if everything is going well?

    # XXX/TODO: BEGIN DEPRECATED SECTION

    def send_get_block_txs(self, child_hash: bytes, last_block_hash: bytes) -> None:
        """ Request a BFS of all transactions that parent of CHILD, up to the ones first comfirmed by LAST-BLOCK.

        Note that CHILD can either be a block or a transaction. But LAST-BLOCK is always a block.
        """
        self._setup_tx_streaming()
        self.log.debug('send_get_block_txs', child=child_hash.hex(), last_block=last_block_hash.hex())
        payload = json.dumps(dict(
            child=child_hash.hex(),
            last_block=last_block_hash.hex(),
        ))
        self.send_message(ProtocolMessages.GET_BLOCK_TXS, payload)
        self.receiving_stream = True

    def handle_get_block_txs(self, payload: str) -> None:
        if self._is_streaming:
            self.log.warn('ignore GET-BLOCK-TXS, already streaming')
            return
        data = json.loads(payload)
        self.log.debug('handle_get_block_txs', **data)
        child_hash = bytes.fromhex(data['child'])
        last_block_hash = bytes.fromhex(data['last_block'])
        self.send_block_txs(child_hash, last_block_hash)

    def send_block_txs(self, child_hash: bytes, last_block_hash: bytes) -> None:
        try:
            tx = self.tx_storage.get_transaction(child_hash)
        except TransactionDoesNotExist:
            # In case the tx does not exist we send a NOT-FOUND message
            self.log.debug('requested child_hash not found', child_hash=child_hash.hex())
            self.send_message(ProtocolMessages.NOT_FOUND, child_hash.hex())
            return
        if not self.partial_vertex_exists(last_block_hash):
            # In case the tx does not exist we send a NOT-FOUND message
            self.log.debug('requested last_block_hash not found', last_block_hash=last_block_hash.hex())
            self.send_message(ProtocolMessages.NOT_FOUND, last_block_hash.hex())
            return
        if self.transactions_streaming is not None and self.transactions_streaming.is_running:
            self.transactions_streaming.stop()
        self.transactions_streaming = TransactionsStreaming(self, [tx], last_block_hash,
                                                            limit=self.DEFAULT_STREAMING_LIMIT)
        self.transactions_streaming.start()

    # XXX/TODO: END DEPRECATED SECTION

    def send_get_transactions_bfs(self, start_from: list[bytes], until_first_block: bytes) -> None:
        """ Request a BFS of all transactions starting from start_from list and walking back into parents/inputs.

        The start_from list can contain blocks, but they won't be sent. For example if a block B1 has T1 and T2 as
        transaction parents, start_from=[B1] and start_from=[T1, T2] will have the same result.

        The stop condition is reaching transactions/inputs that have a first_block of height less or equan than the
        height of until_first_block. The other peer will return an empty response if it doesn't have any of the
        stransactions in start_from or if it doesn't have the until_first_block block.
        """
        self._setup_tx_streaming()
        start_from_hexlist = [tx.hex() for tx in start_from]
        until_first_block_hex = until_first_block.hex()
        self.log.debug('send_get_transactions_bfs', start_from=start_from_hexlist, last_block=until_first_block_hex)
        payload = json.dumps(dict(
            start_from=start_from_hexlist,
            until_first_block=until_first_block_hex,
        ))
        self.send_message(ProtocolMessages.GET_TRANSACTIONS_BFS, payload)
        self.receiving_stream = True

    def handle_get_transactions_bfs(self, payload: str) -> None:
        if self._is_streaming:
            self.log.warn('ignore GET-TRANSACTIONS-BFS, already streaming')
            return
        data = json.loads(payload)
        self.log.debug('handle_get_transactions_bfs', **data)
        start_from = [bytes.fromhex(tx_hash_hex) for tx_hash_hex in data['start_from']]
        until_first_block = bytes.fromhex(data['until_first_block'])
        self.send_transactions_bfs(start_from, until_first_block)

    def send_transactions_bfs(self, start_from: list[bytes], until_first_block: bytes) -> None:
        start_from_txs = []
        for start_from_hash in start_from:
            try:
                start_from_txs.append(self.tx_storage.get_transaction(start_from_hash))
            except TransactionDoesNotExist:
                # In case the tx does not exist we send a NOT-FOUND message
                self.log.debug('requested start_from_hash not found', start_from_hash=start_from_hash.hex())
                self.send_message(ProtocolMessages.NOT_FOUND, start_from_hash.hex())
                return
        if not self.tx_storage.transaction_exists(until_first_block):
            # In case the tx does not exist we send a NOT-FOUND message
            self.log.debug('requested until_first_block not found', until_first_block=until_first_block.hex())
            self.send_message(ProtocolMessages.NOT_FOUND, until_first_block.hex())
            return
        if self.transactions_streaming is not None and self.transactions_streaming.is_running:
            self.transactions_streaming.stop()
        self.transactions_streaming = TransactionsStreaming(self, start_from_txs, until_first_block,
                                                            limit=self.DEFAULT_STREAMING_LIMIT)
        self.transactions_streaming.start()

    def send_transaction(self, tx: Transaction) -> None:
        """Send a TRANSACTION message."""
        # payload = bytes(tx).hex()  # fails for big transactions
        payload = base64.b64encode(bytes(tx)).decode('ascii')
        self.send_message(ProtocolMessages.TRANSACTION, payload)

    def send_transactions_end(self, response_code: StreamEnd) -> None:
        payload = str(int(response_code))
        self.log.debug('send TRANSACTIONS-END', payload=payload)
        self.send_message(ProtocolMessages.TRANSACTIONS_END, payload)

    def handle_transactions_end(self, payload: str) -> None:
        self.log.debug('recv TRANSACTIONS-END', payload=payload, size=self._blk_size)

        response_code = StreamEnd(int(payload))
        self.receiving_stream = False
        assert self.protocol.connections is not None

        if self.state is not PeerState.SYNCING_TRANSACTIONS:
            self.log.error('unexpected TRANSACTIONS-END', state=self.state)
            self.protocol.send_error_and_close_connection('Not expecting to receive TRANSACTIONS-END message')
            return

        self.log.debug('transaction streaming ended', reason=str(response_code))

    def handle_transaction(self, payload: str) -> None:
        """Handle a received TRANSACTION message."""
        assert self.protocol.connections is not None

        # tx_bytes = bytes.fromhex(payload)
        tx_bytes = base64.b64decode(payload)
        tx = tx_or_block_from_bytes(tx_bytes)
        assert tx.hash is not None
        if not isinstance(tx, Transaction):
            self.log.warn('not a transaction', hash=tx.hash_hex)
            # Not a transaction. Punish peer?
            return

        self._tx_received += 1
        if self._tx_received > self._tx_max_quantity + 1:
            self.log.warn('too many txs received')
            self.state = PeerState.ERROR
            return

        try:
            # this methods takes care of checking if the tx already exists, it will take care of doing at least
            # a basic validation
            # self.log.debug('add new tx', tx=tx.hash_hex)
            if self.partial_vertex_exists(tx.hash):
                # XXX: early terminate?
                self.log.debug('tx early terminate?', tx_id=tx.hash.hex())
            else:
                self.log.debug('tx received', tx_id=tx.hash.hex())
                self.on_new_tx(tx, propagate_to_peers=False, quiet=True, reject_locked_reward=True)
        except HathorError:
            self.log.warn('invalid new tx', exc_info=True)
            # Invalid block?!
            # Invalid transaction?!
            # Maybe stop syncing and punish peer.
            self.state = PeerState.ERROR
            return
        else:
            # XXX: debugging log, maybe add timing info
            if self._tx_received % 100 == 0:
                self.log.debug('tx streaming in progress', txs_received=self._tx_received)

    # -----------------------------------
    # BEGIN: GET_DATA/DATA implementation
    # -----------------------------------

    @inlineCallbacks
    def get_tx(self, tx_id: bytes) -> Generator[Deferred, Any, BaseTransaction]:
        """Async method to get a transaction from the db/cache or to download it."""
        tx = self._get_tx_cache.get(tx_id)
        if tx is not None:
            self.log.debug('tx in cache', tx=tx_id.hex())
            return tx
        try:
            tx = self.tx_storage.get_transaction(tx_id)
        except TransactionDoesNotExist:
            tx = yield self.get_data(tx_id, 'mempool')
            if tx is None:
                self.log.error('failed to get tx', tx_id=tx_id.hex())
                self.protocol.send_error_and_close_connection(f'DATA mempool {tx_id.hex()} not found')
                raise
            if tx.hash != tx_id:
                self.protocol.send_error_and_close_connection(f'DATA mempool {tx_id.hex()} hash mismatch')
                raise
        return tx

    def get_data(self, tx_id: bytes, origin: str) -> Deferred:
        """Async method to request a tx by id"""
        # TODO: deal with stale `get_data` calls
        if origin != 'mempool':
            raise ValueError(f'origin={origin} not supported, only origin=mempool is supported')
        key = f'{origin}:{tx_id.hex()}'
        deferred = self.deferred_by_key.get(key, None)
        if deferred is None:
            deferred = self.deferred_by_key[key] = Deferred()
            self.send_get_data(tx_id, origin=origin)
            self.log.debug('get_data of new tx_id', deferred=deferred, key=key)
        else:
            # XXX: can we re-use deferred objects like this?
            self.log.debug('get_data of same tx_id, reusing deferred', deferred=deferred, key=key)
        return deferred

    def _on_get_data(self, tx: BaseTransaction, origin: str) -> None:
        """Called when a requested tx is received."""
        assert tx.hash is not None
        key = f'{origin}:{tx.hash_hex}'
        deferred = self.deferred_by_key.pop(key, None)
        if deferred is None:
            # Peer sent the wrong transaction?!
            # XXX: ban peer?
            self.protocol.send_error_and_close_connection(f'DATA {origin}: with tx that was not requested')
            return
        self.log.debug('get_data fulfilled', deferred=deferred, key=key)
        self._get_tx_cache[tx.hash] = tx
        if len(self._get_tx_cache) > self._get_tx_cache_maxsize:
            self._get_tx_cache.popitem(last=False)
        deferred.callback(tx)

    def send_data(self, tx: BaseTransaction, *, origin: str = '') -> None:
        """ Send a DATA message.
        """
        self.log.debug('send tx', tx=tx.hash_hex)
        tx_payload = base64.b64encode(tx.get_struct()).decode('ascii')
        if not origin:
            payload = tx_payload
        else:
            payload = ' '.join([origin, tx_payload])
        self.send_message(ProtocolMessages.DATA, payload)

    def send_get_data(self, txid: bytes, *, origin: Optional[str] = None) -> None:
        """Send a GET-DATA message for a given txid."""
        data = {
            'txid': txid.hex(),
        }
        if origin is not None:
            data['origin'] = origin
        payload = json.dumps(data)
        self.send_message(ProtocolMessages.GET_DATA, payload)

    def handle_get_data(self, payload: str) -> None:
        """Handle a received GET-DATA message."""
        data = json.loads(payload)
        txid_hex = data['txid']
        origin = data.get('origin', '')
        # self.log.debug('handle_get_data', payload=hash_hex)
        try:
            tx = self.protocol.node.tx_storage.get_transaction(bytes.fromhex(txid_hex))
            self.send_data(tx, origin=origin)
        except TransactionDoesNotExist:
            # In case the tx does not exist we send a NOT-FOUND message
            self.send_message(ProtocolMessages.NOT_FOUND, txid_hex)

    def handle_data(self, payload: str) -> None:
        """ Handle a received DATA message.
        """
        if not payload:
            return
        part1, _, part2 = payload.partition(' ')
        if not part2:
            origin = None
            data = base64.b64decode(part1)
        else:
            origin = part1
            data = base64.b64decode(part2)

        try:
            tx = tx_or_block_from_bytes(data)
        except struct.error:
            # Invalid data for tx decode
            return

        if origin:
            if origin != 'mempool':
                # XXX: ban peer?
                self.protocol.send_error_and_close_connection(f'DATA {origin}: unsupported origin')
                return
            assert tx is not None
            self._on_get_data(tx, origin)
            return

        assert tx is not None
        assert tx.hash is not None
        if self.protocol.node.tx_storage.get_genesis(tx.hash):
            # We just got the data of a genesis tx/block. What should we do?
            # Will it reduce peer reputation score?
            return

        tx.storage = self.protocol.node.tx_storage
        assert tx.hash is not None

        if self.partial_vertex_exists(tx.hash):
            # transaction already added to the storage, ignore it
            # XXX: maybe we could add a hash blacklist and punish peers propagating known bad txs
            self.manager.tx_storage.compare_bytes_with_local_tx(tx)
            return
        else:
            # If we have not requested the data, it is a new transaction being propagated
            # in the network, thus, we propagate it as well.
            if tx.can_validate_full():
                self.log.info('tx received in real time from peer', tx=tx.hash_hex, peer=self.protocol.get_peer_id())
                self.on_new_tx(tx, conn=self.protocol, propagate_to_peers=True)
            else:
                self.log.info('skipping tx received in real time from peer',
                              tx=tx.hash_hex, peer=self.protocol.get_peer_id())

    def on_new_tx(self, tx: BaseTransaction, *, conn: Optional['HathorProtocol'] = None,
                  quiet: bool = False, propagate_to_peers: bool = True,
                  skip_block_weight_verification: bool = False, sync_checkpoints: bool = False,
                  reject_locked_reward: bool = True) -> bool:

        assert self.tx_storage.indexes is not None
        assert tx.hash is not None

        tx.storage = self.tx_storage

        with self.tx_storage.allow_partially_validated_context():
            metadata = tx.get_metadata()

        if metadata.validation.is_fully_connected() or tx.can_validate_full():
            if not self.manager.on_new_tx(tx):
                return False
        else:
            with self.tx_storage.allow_partially_validated_context():
                if isinstance(tx, Block) and not tx.has_basic_block_parent():
                    self.log.warn('on_new_tx(): block parent needs to be at least basic-valid', tx=tx.hash_hex)
                    return False
                if not tx.validate_basic():
                    self.log.warn('on_new_tx(): basic validation failed', tx=tx.hash_hex)
                    return False

                # The method below adds the tx as a child of the parents
                # This needs to be called right before the save because we were adding the children
                # in the tx parents even if the tx was invalid (failing the verifications above)
                # then I would have a children that was not in the storage
                self.tx_storage.save_transaction(tx)
            self.manager.log_new_object(tx, 'new {} partially accepted', quiet=quiet)

        return True
