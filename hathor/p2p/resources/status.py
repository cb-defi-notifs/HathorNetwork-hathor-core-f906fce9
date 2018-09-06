
from twisted.web import resource
from hathor.api_util import set_cors

import json
import time


class StatusResource(resource.Resource):
    """ Implements an status web server API, which responds with a summary
    of the node state.

    You must run with option `--status <PORT>`.
    """
    isLeaf = True

    def __init__(self, manager):
        self.manager = manager

    def render_GET(self, request):
        request.setHeader(b'content-type', b'application/json; charset=utf-8')
        set_cors(request, 'GET')

        connected_peers = []
        for conn in self.manager.connected_peers.values():
            remote = conn.transport.getPeer()
            connected_peers.append({
                'id': conn.peer.id,
                'address': '{}:{}'.format(remote.host, remote.port),
                'received_bytes': conn.received_bytes,
                'last_message': time.time() - conn.last_message,
            })

        known_peers = []
        for peer in self.manager.peer_storage.values():
            known_peers.append({
                'id': peer.id,
                'entrypoints': peer.entrypoints,
            })

        data = {
            'server': {
                'uptime': time.time() - self.manager.start_time,
                'id': self.manager.my_peer.id,
                'entrypoints': self.manager.my_peer.entrypoints,
            },
            'known_peers': known_peers,
            'connected_peers': connected_peers,
        }
        return json.dumps(data, indent=4).encode('utf-8')
