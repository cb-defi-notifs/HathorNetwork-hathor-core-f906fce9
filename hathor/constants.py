TOKENS_PER_BLOCK = 20

DECIMAL_PLACES = 2

GENESIS_TOKEN_UNITS = 2 * (10**9)  # 2B

GENESIS_TOKENS = GENESIS_TOKEN_UNITS * (10**DECIMAL_PLACES)  # 200B

# Version byte of the address in P2PKH
P2PKH_VERSION_BYTE = b'\x00'

# Version byte of the address in MultiSig
MULTISIG_VERSION_BYTE = b'\x05'

# Weight of genesis and minimum weight of a tx/block
MIN_BLOCK_WEIGHT = 14
MIN_TX_WEIGHT = 14

HATHOR_TOKEN_UID = b'\x00'

# Average time between blocks.
AVG_TIME_BETWEEN_BLOCKS = 64

# Maximum distance between two consecutive blocks (in seconds), except for genesis.
# This prevent some DoS attacks exploiting the calculation of the score of a side chain.
# P(t > T) = 1/e^30 = 9.35e-14
MAX_DISTANCE_BETWEEN_BLOCKS = 30 * AVG_TIME_BETWEEN_BLOCKS

# Number of blocks to be found with the same hash algorithm as `block`.
# The bigger it is, the smaller the variance of the hash rate estimator is.
BLOCK_DIFFICULTY_N_BLOCKS = 120

# Maximum change in difficulty between consecutive blocks.
#
# The variance of the hash rate estimator is high when the hash rate is increasing
# or decreasing. Many times it will overreact and increase/decrease the weight too
# much. This limit is used to make the weight change more smooth.
#
# [msbrogli]
# Why 0.25? I have some arguments in favor of 0.25 based on the models I've been studying.
# But my arguments are not very solid. They may be good to compare 0.25 with 5.0 or higher values, but not to 0.50.
# My best answer for now is that it will be rare to reach this limit due to the variance of the hash rate estimator.
# So, it will be reached only when the hash rate has really changed (increased or decreased). It also reduces
# significantly the ripple effect overreacting to changes in the hash rate. For example, during my simulations without
# a max_dw, when the hash rate increased from 2^20 to 2^30, the weight change was too big, and it took more than
# 10 minutes to find the next block. After, it took so long that the weight change was reduced too much.
# This ripple was amortized over time reaching the right value. Applying a max_dw, the ripple has been reduced.
# Maybe 0.50 or 1.0 are good values as well.
BLOCK_DIFFICULTY_MAX_DW = 0.25

# Maximum depth looking for blocks with the same hash algorithm.
# The probability of finding N blocks when the max depth is K*N is:
# P(x >= N) = 1 - norm.cdf((A - K) * sqrt(N) / sqrt(K * (A - 1)))
# Where A is the number of hash algorithms available.
#
# For N=120 and A=2, we have that K=3 implies P(x >= N) = 0.9999999998730186,
# while K = 2 implies P(x >= N) = 0.5.
#
# Thus, K=3 is the least value to find all the 120 most of the times.
BLOCK_DIFFICULTY_MAX_DEPTH = 3 * BLOCK_DIFFICULTY_N_BLOCKS

# Number of bytes that each transaction type nonce have
BLOCK_NONCE_BYTES = 16
TX_NONCE_BYTES = 4

# Size limit in bytes for Block data field
BLOCK_DATA_MAX_SIZE = 100

# number of subfolders in the storage folder (used in JSONStorage and CompactStorage)
STORAGE_SUBFOLDERS = 256
