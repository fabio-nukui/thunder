from chains.cosmos.terra import UST

MAX_SINGLE_ARBITRAGE_AMOUNT = UST.to_amount(50_000)
MAX_N_REPEATS = 20
MIN_PROFIT_UST = UST.to_amount(20)
MIN_START_AMOUNT = UST.to_amount(200)
OPTIMIZATION_TOLERANCE = UST.to_amount(1)
