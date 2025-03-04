# Copyright 2023 Hathor Labs
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

from typing import TYPE_CHECKING

from twisted.internet.protocol import Factory

from hathor.sysctl.protocol import SysctlProtocol

if TYPE_CHECKING:
    from hathor.sysctl.sysctl import Sysctl


class SysctlFactory(Factory):
    def __init__(self, root: 'Sysctl') -> None:
        self.root = root

    def buildProtocol(self, addr):
        return SysctlProtocol(self.root)
