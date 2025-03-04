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

from enum import Enum
from typing import Any, NamedTuple, Optional

from structlog import get_logger

from hathor.checkpoint import Checkpoint
from hathor.conf import HathorSettings
from hathor.conf.settings import HathorSettings as HathorSettingsType
from hathor.consensus import ConsensusAlgorithm
from hathor.event import EventManager
from hathor.event.storage import EventMemoryStorage, EventRocksDBStorage, EventStorage
from hathor.event.websocket import EventWebsocketFactory
from hathor.feature_activation.feature_service import FeatureService
from hathor.indexes import IndexesManager, MemoryIndexesManager, RocksDBIndexesManager
from hathor.manager import HathorManager
from hathor.p2p.manager import ConnectionsManager
from hathor.p2p.peer_id import PeerId
from hathor.pubsub import PubSubManager
from hathor.storage import RocksDBStorage
from hathor.stratum import StratumFactory
from hathor.transaction.storage import (
    TransactionCacheStorage,
    TransactionMemoryStorage,
    TransactionRocksDBStorage,
    TransactionStorage,
)
from hathor.util import Random, Reactor, get_environment_info
from hathor.wallet import BaseWallet, Wallet

logger = get_logger()


class StorageType(Enum):
    MEMORY = 'memory'
    ROCKSDB = 'rocksdb'


class BuildArtifacts(NamedTuple):
    """Artifacts created by a builder."""
    peer_id: PeerId
    settings: HathorSettingsType
    rng: Random
    reactor: Reactor
    manager: HathorManager
    p2p_manager: ConnectionsManager
    pubsub: PubSubManager
    consensus: ConsensusAlgorithm
    tx_storage: TransactionStorage
    indexes: Optional[IndexesManager]
    wallet: Optional[BaseWallet]
    rocksdb_storage: Optional[RocksDBStorage]
    stratum_factory: Optional[StratumFactory]
    feature_service: FeatureService


class Builder:
    """Builder builds the core objects to run a full node.

    Example:

        builder = Builder()
        builder.use_memory()
        artifacts = builder.build()
    """
    def __init__(self) -> None:
        self.log = logger.new()
        self.artifacts: Optional[BuildArtifacts] = None

        self._settings: HathorSettingsType = HathorSettings()
        self._rng: Random = Random()
        self._checkpoints: Optional[list[Checkpoint]] = None
        self._capabilities: Optional[list[str]] = None

        self._peer_id: Optional[PeerId] = None
        self._network: Optional[str] = None
        self._cmdline: str = ''

        self._storage_type: StorageType = StorageType.MEMORY
        self._force_memory_index: bool = False

        self._event_manager: Optional[EventManager] = None
        self._enable_event_queue: Optional[bool] = None

        self._rocksdb_path: Optional[str] = None
        self._rocksdb_storage: Optional[RocksDBStorage] = None
        self._rocksdb_cache_capacity: Optional[int] = None

        self._tx_storage_cache: bool = False
        self._tx_storage_cache_capacity: Optional[int] = None

        self._indexes_manager: Optional[IndexesManager] = None
        self._tx_storage: Optional[TransactionStorage] = None
        self._event_storage: Optional[EventStorage] = None

        self._reactor: Optional[Reactor] = None
        self._pubsub: Optional[PubSubManager] = None

        self._wallet: Optional[BaseWallet] = None
        self._wallet_directory: Optional[str] = None
        self._wallet_unlock: Optional[bytes] = None

        self._enable_address_index: bool = False
        self._enable_tokens_index: bool = False
        self._enable_utxo_index: bool = False

        self._enable_sync_v1: bool = False
        self._enable_sync_v1_1: bool = True
        self._enable_sync_v2: bool = False

        self._enable_stratum_server: Optional[bool] = None

        self._full_verification: Optional[bool] = None

        self._soft_voided_tx_ids: Optional[set[bytes]] = None

    def build(self) -> BuildArtifacts:
        if self.artifacts is not None:
            raise ValueError('cannot call build twice')

        if self._network is None:
            raise TypeError('you must set a network')

        settings = self._get_settings()
        reactor = self._get_reactor()
        pubsub = self._get_or_create_pubsub()

        peer_id = self._get_peer_id()

        soft_voided_tx_ids = self._get_soft_voided_tx_ids()
        consensus_algorithm = ConsensusAlgorithm(soft_voided_tx_ids, pubsub)

        p2p_manager = self._get_p2p_manager()

        wallet = self._get_or_create_wallet()
        event_manager = self._get_or_create_event_manager()
        indexes = self._get_or_create_indexes_manager()
        tx_storage = self._get_or_create_tx_storage(indexes)

        if self._enable_address_index:
            indexes.enable_address_index(pubsub)

        if self._enable_tokens_index:
            indexes.enable_tokens_index()

        if self._enable_utxo_index:
            indexes.enable_utxo_index()

        kwargs: dict[str, Any] = {}

        if self._full_verification is not None:
            kwargs['full_verification'] = self._full_verification

        if self._enable_event_queue is not None:
            kwargs['enable_event_queue'] = self._enable_event_queue

        manager = HathorManager(
            reactor,
            network=self._network,
            pubsub=pubsub,
            consensus_algorithm=consensus_algorithm,
            peer_id=peer_id,
            tx_storage=tx_storage,
            p2p_manager=p2p_manager,
            event_manager=event_manager,
            wallet=wallet,
            rng=self._rng,
            checkpoints=self._checkpoints,
            capabilities=self._capabilities,
            environment_info=get_environment_info(self._cmdline, peer_id.id),
            **kwargs
        )

        p2p_manager.set_manager(manager)

        stratum_factory: Optional[StratumFactory] = None
        if self._enable_stratum_server:
            stratum_factory = self._create_stratum_server(manager)

        feature_service = self._create_feature_service(tx_storage)

        self.artifacts = BuildArtifacts(
            peer_id=peer_id,
            settings=settings,
            rng=self._rng,
            reactor=reactor,
            manager=manager,
            p2p_manager=p2p_manager,
            pubsub=pubsub,
            consensus=consensus_algorithm,
            tx_storage=tx_storage,
            indexes=indexes,
            wallet=wallet,
            rocksdb_storage=self._rocksdb_storage,
            stratum_factory=stratum_factory,
            feature_service=feature_service
        )

        return self.artifacts

    def check_if_can_modify(self) -> None:
        if self.artifacts is not None:
            raise ValueError('cannot modify after build() is called')

    def set_event_manager(self, event_manager: EventManager) -> 'Builder':
        self.check_if_can_modify()
        self._event_manager = event_manager
        return self

    def set_rng(self, rng: Random) -> 'Builder':
        self.check_if_can_modify()
        self._rng = rng
        return self

    def set_checkpoints(self, checkpoints: list[Checkpoint]) -> 'Builder':
        self.check_if_can_modify()
        self._checkpoints = checkpoints
        return self

    def set_capabilities(self, capabilities: list[str]) -> 'Builder':
        self.check_if_can_modify()
        self._capabilities = capabilities
        return self

    def set_peer_id(self, peer_id: PeerId) -> 'Builder':
        self.check_if_can_modify()
        self._peer_id = peer_id
        return self

    def _get_settings(self) -> HathorSettingsType:
        return self._settings

    def _get_reactor(self) -> Reactor:
        if self._reactor is not None:
            return self._reactor
        raise ValueError('reactor not set')

    def _get_soft_voided_tx_ids(self) -> set[bytes]:
        if self._soft_voided_tx_ids is not None:
            return self._soft_voided_tx_ids

        settings = self._get_settings()

        return set(settings.SOFT_VOIDED_TX_IDS)

    def _get_peer_id(self) -> PeerId:
        if self._peer_id is not None:
            return self._peer_id
        raise ValueError('peer_id not set')

    def _get_or_create_pubsub(self) -> PubSubManager:
        if self._pubsub is None:
            self._pubsub = PubSubManager(self._get_reactor())
        return self._pubsub

    def _create_stratum_server(self, manager: HathorManager) -> StratumFactory:
        stratum_factory = StratumFactory(manager=manager)
        manager.stratum_factory = stratum_factory
        manager.metrics.stratum_factory = stratum_factory
        return stratum_factory

    def _create_feature_service(self, tx_storage: TransactionStorage) -> FeatureService:
        return FeatureService(
            feature_settings=self._settings.FEATURE_ACTIVATION,
            tx_storage=tx_storage
        )

    def _get_or_create_rocksdb_storage(self) -> RocksDBStorage:
        assert self._rocksdb_path is not None

        if self._rocksdb_storage is not None:
            return self._rocksdb_storage

        kwargs = {}
        if self._rocksdb_cache_capacity is not None:
            kwargs = dict(cache_capacity=self._rocksdb_cache_capacity)

        self._rocksdb_storage = RocksDBStorage(
            path=self._rocksdb_path,
            **kwargs
        )

        return self._rocksdb_storage

    def _get_p2p_manager(self) -> ConnectionsManager:
        enable_ssl = True
        reactor = self._get_reactor()
        my_peer = self._get_peer_id()

        assert self._network is not None

        p2p_manager = ConnectionsManager(
            reactor,
            network=self._network,
            my_peer=my_peer,
            pubsub=self._get_or_create_pubsub(),
            ssl=enable_ssl,
            whitelist_only=False,
            rng=self._rng,
            enable_sync_v1=self._enable_sync_v1,
            enable_sync_v1_1=self._enable_sync_v1_1,
            enable_sync_v2=self._enable_sync_v2,
        )
        return p2p_manager

    def _get_or_create_indexes_manager(self) -> IndexesManager:
        if self._indexes_manager is not None:
            return self._indexes_manager

        if self._force_memory_index or self._storage_type == StorageType.MEMORY:
            self._indexes_manager = MemoryIndexesManager()

        elif self._storage_type == StorageType.ROCKSDB:
            rocksdb_storage = self._get_or_create_rocksdb_storage()
            self._indexes_manager = RocksDBIndexesManager(rocksdb_storage)

        else:
            raise NotImplementedError

        return self._indexes_manager

    def _get_or_create_tx_storage(self, indexes: IndexesManager) -> TransactionStorage:
        if self._tx_storage is not None:
            # If a tx storage is provided, set the indexes manager to it.
            self._tx_storage.indexes = indexes
            return self._tx_storage

        store_indexes: Optional[IndexesManager] = indexes
        if self._tx_storage_cache:
            store_indexes = None

        if self._storage_type == StorageType.MEMORY:
            self._tx_storage = TransactionMemoryStorage(indexes=store_indexes)

        elif self._storage_type == StorageType.ROCKSDB:
            rocksdb_storage = self._get_or_create_rocksdb_storage()
            self._tx_storage = TransactionRocksDBStorage(rocksdb_storage, indexes=store_indexes)

        else:
            raise NotImplementedError

        if self._tx_storage_cache:
            reactor = self._get_reactor()
            kwargs: dict[str, Any] = {}
            if self._tx_storage_cache_capacity is not None:
                kwargs['capacity'] = self._tx_storage_cache_capacity
            self._tx_storage = TransactionCacheStorage(self._tx_storage, reactor, indexes=indexes, **kwargs)

        return self._tx_storage

    def _get_or_create_event_storage(self) -> EventStorage:
        if self._event_storage is not None:
            pass
        elif self._storage_type == StorageType.MEMORY:
            self._event_storage = EventMemoryStorage()
        elif self._storage_type == StorageType.ROCKSDB:
            rocksdb_storage = self._get_or_create_rocksdb_storage()
            self._event_storage = EventRocksDBStorage(rocksdb_storage)
        else:
            raise NotImplementedError

        return self._event_storage

    def _get_or_create_event_manager(self) -> EventManager:
        if self._event_manager is None:
            reactor = self._get_reactor()
            storage = self._get_or_create_event_storage()
            factory = EventWebsocketFactory(reactor, storage)
            self._event_manager = EventManager(
                reactor=reactor,
                pubsub=self._get_or_create_pubsub(),
                event_storage=storage,
                event_ws_factory=factory
            )

        return self._event_manager

    def use_memory(self) -> 'Builder':
        self.check_if_can_modify()
        self._storage_type = StorageType.MEMORY
        return self

    def use_rocksdb(
        self,
        path: str,
        cache_capacity: Optional[int] = None
    ) -> 'Builder':
        self.check_if_can_modify()
        self._storage_type = StorageType.ROCKSDB
        self._rocksdb_path = path
        self._rocksdb_cache_capacity = cache_capacity
        return self

    def use_tx_storage_cache(self, capacity: Optional[int] = None) -> 'Builder':
        self.check_if_can_modify()
        self._tx_storage_cache = True
        self._tx_storage_cache_capacity = capacity
        return self

    def force_memory_index(self) -> 'Builder':
        self.check_if_can_modify()
        self._force_memory_index = True
        return self

    def _get_or_create_wallet(self) -> Optional[BaseWallet]:
        if self._wallet is not None:
            assert self._wallet_directory is None
            assert self._wallet_unlock is None
            return self._wallet

        if self._wallet_directory is None:
            return None
        wallet = Wallet(directory=self._wallet_directory)
        if self._wallet_unlock is not None:
            wallet.unlock(self._wallet_unlock)
        return wallet

    def set_wallet(self, wallet: BaseWallet) -> 'Builder':
        self.check_if_can_modify()
        self._wallet = wallet
        return self

    def enable_keypair_wallet(self, directory: str, *, unlock: Optional[bytes] = None) -> 'Builder':
        self.check_if_can_modify()
        self._wallet_directory = directory
        self._wallet_unlock = unlock
        return self

    def enable_stratum_server(self) -> 'Builder':
        self.check_if_can_modify()
        self._enable_stratum_server = True
        return self

    def enable_address_index(self) -> 'Builder':
        self.check_if_can_modify()
        self._enable_address_index = True
        return self

    def enable_tokens_index(self) -> 'Builder':
        self.check_if_can_modify()
        self._enable_tokens_index = True
        return self

    def enable_utxo_index(self) -> 'Builder':
        self.check_if_can_modify()
        self._enable_utxo_index = True
        return self

    def enable_wallet_index(self) -> 'Builder':
        self.check_if_can_modify()
        self.enable_address_index()
        self.enable_tokens_index()
        return self

    def enable_event_queue(self) -> 'Builder':
        self.check_if_can_modify()
        self._enable_event_queue = True
        return self

    def set_tx_storage(self, tx_storage: TransactionStorage) -> 'Builder':
        self.check_if_can_modify()
        self._tx_storage = tx_storage
        return self

    def set_event_storage(self, event_storage: EventStorage) -> 'Builder':
        self.check_if_can_modify()
        self._event_storage = event_storage
        return self

    def set_reactor(self, reactor: Reactor) -> 'Builder':
        self.check_if_can_modify()
        self._reactor = reactor
        return self

    def set_pubsub(self, pubsub: PubSubManager) -> 'Builder':
        self.check_if_can_modify()
        self._pubsub = pubsub
        return self

    def set_network(self, network: str) -> 'Builder':
        self.check_if_can_modify()
        self._network = network
        return self

    def set_enable_sync_v1(self, enable_sync_v1: bool) -> 'Builder':
        self.check_if_can_modify()
        self._enable_sync_v1 = enable_sync_v1
        return self

    def set_enable_sync_v1_1(self, enable_sync_v1_1: bool) -> 'Builder':
        self.check_if_can_modify()
        self._enable_sync_v1_1 = enable_sync_v1_1
        return self

    def set_enable_sync_v2(self, enable_sync_v2: bool) -> 'Builder':
        self.check_if_can_modify()
        self._enable_sync_v2 = enable_sync_v2
        return self

    def enable_sync_v1(self) -> 'Builder':
        self.check_if_can_modify()
        self._enable_sync_v1 = True
        return self

    def disable_sync_v1(self) -> 'Builder':
        self.check_if_can_modify()
        self._enable_sync_v1 = False
        return self

    def enable_sync_v1_1(self) -> 'Builder':
        self.check_if_can_modify()
        self._enable_sync_v1_1 = True
        return self

    def disable_sync_v1_1(self) -> 'Builder':
        self.check_if_can_modify()
        self._enable_sync_v1_1 = False
        return self

    def enable_sync_v2(self) -> 'Builder':
        self.check_if_can_modify()
        self._enable_sync_v2 = True
        return self

    def disable_sync_v2(self) -> 'Builder':
        self.check_if_can_modify()
        self._enable_sync_v2 = False
        return self

    def set_full_verification(self, full_verification: bool) -> 'Builder':
        self.check_if_can_modify()
        self._full_verification = full_verification
        return self

    def enable_full_verification(self) -> 'Builder':
        self.check_if_can_modify()
        self._full_verification = True
        return self

    def disable_full_verification(self) -> 'Builder':
        self.check_if_can_modify()
        self._full_verification = False
        return self

    def set_soft_voided_tx_ids(self, soft_voided_tx_ids: set[bytes]) -> 'Builder':
        self.check_if_can_modify()
        self._soft_voided_tx_ids = soft_voided_tx_ids
        return self
