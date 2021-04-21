# Copyright 2021 Hathor Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from hathor.p2p.netfilter.chain import NetfilterChain
from hathor.p2p.netfilter.table import NetfilterTable
from hathor.p2p.netfilter.targets import NetfilterAccept

filter_table = NetfilterTable('filter')
filter_table.add_chain(NetfilterChain('pre_conn', policy=NetfilterAccept()))
filter_table.add_chain(NetfilterChain('post_hello', policy=NetfilterAccept()))
filter_table.add_chain(NetfilterChain('post_peerid', policy=NetfilterAccept()))

tables = {
    'filter': filter_table,
}


def get_table(name):
    """Get table `name` of the netfilter."""
    if name not in tables:
        raise KeyError('Table {} does not exists'.format(name))
    return tables[name]
