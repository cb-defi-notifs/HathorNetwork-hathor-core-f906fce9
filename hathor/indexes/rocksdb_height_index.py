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

from typing import TYPE_CHECKING, Optional

from structlog import get_logger

from hathor.conf import HathorSettings
from hathor.indexes.height_index import BLOCK_GENESIS_ENTRY, HeightIndex, IndexEntry
from hathor.indexes.rocksdb_utils import RocksDBIndexUtils

if TYPE_CHECKING:  # pragma: no cover
    import rocksdb

settings = HathorSettings()
logger = get_logger()

_CF_NAME_HEIGHT_INDEX = b'height-index'
_DB_NAME: str = 'height'


class RocksDBHeightIndex(HeightIndex, RocksDBIndexUtils):
    """ Index of blocks by height.

    This index uses the following key/value format:

        key = [height]
              |--4b--|

        value = [tx.hash][tx.timestamp]
                |--32b--||--4 bytes---|

    It works nicely because rocksdb uses a tree sorted by key under the hood.
    """

    def __init__(self, db: 'rocksdb.DB', *, cf_name: Optional[bytes] = None) -> None:
        self.log = logger.new()
        RocksDBIndexUtils.__init__(self, db, cf_name or _CF_NAME_HEIGHT_INDEX)

    def get_db_name(self) -> Optional[str]:
        # XXX: we don't need it to be parametrizable, so this is fine
        return _DB_NAME

    def force_clear(self) -> None:
        self.clear()

    def _init_db(self) -> None:
        """ Initialize the database with the genesis entry."""
        key_genesis = self._to_key(0)
        value_genesis = self._to_value(BLOCK_GENESIS_ENTRY)
        self._db.put((self._cf, key_genesis), value_genesis)

    def _to_key(self, height: int) -> bytes:
        """ Serialize height to key used internally"""
        import struct
        key = struct.pack('>I', height)
        assert len(key) == 4
        return key

    def _from_key(self, key: bytes) -> int:
        """ Parse internal key to the height"""
        import struct
        assert len(key) == 4
        (height,) = struct.unpack('>I', key)
        return height

    def _to_value(self, entry: IndexEntry) -> bytes:
        """ Serialize entry to the value used internally"""
        import struct
        assert len(entry.hash) == 32
        value = bytearray(entry.hash)
        value.extend(struct.pack('>I', entry.timestamp))
        assert len(value) == 32 + 4
        return bytes(value)

    def _from_value(self, value: bytes) -> IndexEntry:
        """ Parse internal value to the entry"""
        import struct
        assert len(value) == 32 + 4
        hash = value[:32]
        (timestamp,) = struct.unpack('>I', value[32:])
        return IndexEntry(hash, timestamp)

    def _del_from_height(self, height: int) -> None:
        """ Delete all entries starting from the given height up."""
        import rocksdb
        batch = rocksdb.WriteBatch()
        it = self._db.iterkeys(self._cf)
        it.seek(self._to_key(height))
        for _, key in it:
            batch.delete((self._cf, key))
        self._db.write(batch)

    def _add(self, height: int, entry: IndexEntry, *, can_reorg: bool) -> None:
        """ Internal implementation of how to add an entry while expecting a re-org or not."""
        cur_height, cur_tip = self.get_height_tip()
        key = self._to_key(height)
        value = self._to_value(entry)
        if height > cur_height + 1:
            raise ValueError(f'parent hash required (current height: {cur_height}, new height: {height})')
        elif height == cur_height + 1:
            self._db.put((self._cf, key), value)
        elif cur_tip != entry.hash:
            if can_reorg:
                self._del_from_height(height)
                self._db.put((self._cf, key), value)
            else:
                raise ValueError('adding would cause a re-org, use can_reorg=True to accept re-orgs')
        else:
            # nothing to do (there are more blocks, but the block at height currently matches the added block)
            assert cur_tip == entry.hash

    def add_new(self, height: int, block_hash: bytes, timestamp: int) -> None:
        self._add(height, IndexEntry(block_hash, timestamp), can_reorg=False)

    def add_reorg(self, height: int, block_hash: bytes, timestamp: int) -> None:
        self._add(height, IndexEntry(block_hash, timestamp), can_reorg=True)

    def get(self, height: int) -> Optional[bytes]:
        key = self._to_key(height)
        value = self._db.get((self._cf, key))
        if not value:
            return None
        return self._from_value(value).hash

    def get_tip(self) -> bytes:
        it = self._db.itervalues(self._cf)
        it.seek_to_last()
        value = it.get()
        assert value is not None  # must never be empty, at least genesis has been added
        return self._from_value(value).hash

    def get_height_tip(self) -> tuple[int, bytes]:
        it = self._db.iteritems(self._cf)
        it.seek_to_last()
        (_, key), value = it.get()
        assert key is not None and value is not None  # must never be empty, at least genesis has been added
        height = self._from_key(key)
        entry = self._from_value(value)
        return height, entry.hash
