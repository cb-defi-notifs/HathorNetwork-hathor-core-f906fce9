from typing import NamedTuple

DECIMAL_PLACES = 2

GENESIS_TOKEN_UNITS = 2 * (10**9)  # 2B
GENESIS_TOKENS = GENESIS_TOKEN_UNITS * (10**DECIMAL_PLACES)  # 200B


class HathorSettings(NamedTuple):
    # Version byte of the address in P2PKH
    P2PKH_VERSION_BYTE: bytes

    # Version byte of the address in MultiSig
    MULTISIG_VERSION_BYTE: bytes

    DECIMAL_PLACES: int = DECIMAL_PLACES

    # Genesis pre-mined tokens
    GENESIS_TOKEN_UNITS: int = GENESIS_TOKEN_UNITS

    GENESIS_TOKENS: int = GENESIS_TOKENS

    TOKENS_PER_BLOCK: int = 20

    # Genesis pre-mined outputs
    # P2PKH HMcJymyctyhnWsWTXqhP9txDwgNZaMWf42
    #
    # To generate a new P2PKH script, run:
    # >>> from hathor.transaction.scripts import P2PKH
    # >>> import base58
    # >>> address = base58.b58decode('HMcJymyctyhnWsWTXqhP9txDwgNZaMWf42')
    # >>> P2PKH.create_output_script(address=address).hex()
    GENESIS_OUTPUT_SCRIPT: bytes = bytes.fromhex('76a914a584cf48b161e4a49223ed220df30037ab740e0088ac')

    # Weight of genesis and minimum weight of a tx/block
    MIN_BLOCK_WEIGHT: int = 14
    MIN_TX_WEIGHT: int = 14

    HATHOR_TOKEN_UID: bytes = b'\x00'

    # Maximum distance between two consecutive blocks (in seconds), except for genesis.
    # This prevent some DoS attacks exploiting the calculation of the score of a side chain.
    MAX_DISTANCE_BETWEEN_BLOCKS: int = 30*64  # P(t > T) = 1/e^30 = 9.35e-14

    # Number of blocks to be found with the same hash algorithm as `block`.
    # The bigger it is, the smaller the variance of the hash rate estimator is.
    BLOCK_DIFFICULTY_N_BLOCKS: int = 20

    # Maximum change in difficulty between consecutive blocks.
    #
    # The variance of the hash rate estimator is high when the hash rate is increasing
    # or decreasing. Many times it will overreact and increase/decrease the weight too
    # much. This limit is used to make the weight change more smooth.
    #
    # [msbrogli]
    # Why 0.25? I have some arguments in favor of 0.25 based on the models I've been studying.
    # But my arguments are not very solid. They may be good to compare 0.25 with 5.0 or higher values, but not to 0.50.
    # My best answer for now is that it will be rare to reach this limit due to the variance of the hash rate estimator
    # So, it will be reached only when the hash rate has really changed (increased or decreased). It also reduces
    # significantly the ripple effect overreacting to changes in the hash rate. For example, during my simulations
    # without a max_dw, when the hash rate increased from 2^20 to 2^30, the weight change was too big, and it took more
    # than 10 minutes to find the next block. After, it took so long that the weight change was reduced too much.
    # This ripple was amortized over time reaching the right value. Applying a max_dw, the ripple has been reduced.
    # Maybe 0.50 or 1.0 are good values as well.
    BLOCK_DIFFICULTY_MAX_DW: float = 0.25

    # Size limit in bytes for Block data field
    BLOCK_DATA_MAX_SIZE: int = 100

    # Number of subfolders in the storage folder (used in JSONStorage and CompactStorage)
    STORAGE_SUBFOLDERS: int = 256

    # Maximum level of the neighborhood graph generated by graphviz
    MAX_GRAPH_LEVEL: int = 3

    # Maximum difference between our latest timestamp and a peer's synced timestamp to consider
    # that the peer is synced (in seconds).
    P2P_SYNC_THRESHOLD: int = 60

    # Maximum number of opened threads that are solving POW for send tokens
    MAX_POW_THREADS: int = 5

    # The error tolerance, to allow small rounding errors in Python, when comparing weights,
    # accumulated weights, and scores
    # How to use:
    # if abs(w1 - w2) < WEIGHT_TOL:
    #     print('w1 and w2 are equal')

    # if w1 < w2 - WEIGHT_TOL:
    #     print('w1 is smaller than w2')

    # if w1 <= w2 + WEIGHT_TOL:
    #     print('w1 is smaller than or equal to w2')

    # if w1 > w2 + WEIGHT_TOL:
    #     print('w1 is greater than w2')

    # if w1 >= w2 - WEIGHT_TOL:
    #     print('w1 is greater than or equal to w2')
    WEIGHT_TOL: int = 1e-10
