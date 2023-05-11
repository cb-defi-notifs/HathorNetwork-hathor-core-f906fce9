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

from typing import TYPE_CHECKING, Tuple

from hathor.sysctl.sysctl import Sysctl

if TYPE_CHECKING:
    from hathor.manager import HathorManager


class HathorManagerSysctl(Sysctl):
    def __init__(self, manager: 'HathorManager') -> None:
        super().__init__()

        self.manager = manager
        self.register(
            'profiler',
            self.get_profiler,
            None,
        )
        self.register(
            'profiler.start',
            None,
            self.start_profiler,
        )
        self.register(
            'profiler.stop',
            None,
            self.stop_profiler,
        )

    def get_profiler(self) -> Tuple[int, float]:
        is_running = self.manager.is_profiler_running
        if not is_running:
            return (0, 0)
        now = self.manager.reactor.seconds()
        duration = now - self.manager.profiler_last_start_time
        return (1, duration)

    def start_profiler(self, reset_flag: int) -> None:
        reset = (reset_flag != 0)
        self.manager.start_profiler(reset=reset)

    def stop_profiler(self, save_to: str) -> None:
        self.manager.stop_profiler(save_to)
