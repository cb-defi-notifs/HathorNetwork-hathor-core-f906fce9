from twisted.internet.task import Clock

from tests import unittest
from tests.utils import add_new_blocks, add_new_transactions
from hathor.cli.multisig_signature import create_parser, execute
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes

from hathor.wallet import Wallet

from io import StringIO
from contextlib import redirect_stdout
import tempfile
import hashlib
import time


class SignatureTest(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.clock = Clock()
        self.clock.advance(time.time())
        self.network = 'testnet'
        self.manager = self.create_peer(self.network, unlock_wallet=True)

        tmpdir = tempfile.mkdtemp()
        self.wallet = Wallet(directory=tmpdir)
        self.wallet.unlock(b'123')

    def test_generate_signature(self):
        add_new_blocks(self.manager, 1, advance_clock=1)
        tx = add_new_transactions(self.manager, 1, advance_clock=1)[0]

        address = self.wallet.get_unused_address()
        keypair = self.wallet.keys[address]
        private_key_hex = keypair.private_key_bytes.hex()

        private_key = keypair.get_private_key(b'123')
        public_key = private_key.public_key()

        parser = create_parser()

        # Generate signature to validate
        args = parser.parse_args([tx.get_struct().hex(), private_key_hex])
        f = StringIO()
        with redirect_stdout(f):
            execute(args, '123')
        # Transforming prints str in array
        output = f.getvalue().split('\n')
        # Last element is always empty string
        output.pop()

        signature = bytes.fromhex(output[0].split(':')[1].strip())

        # Now we validate that the signature is correct
        data_to_sign = tx.get_sighash_all()
        hashed_data = hashlib.sha256(data_to_sign).digest()
        self.assertIsNone(public_key.verify(signature, hashed_data, ec.ECDSA(hashes.SHA256())))
