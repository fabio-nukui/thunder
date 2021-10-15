from decimal import Decimal

from chains.terra import UST

MAX_SLIPPAGE = Decimal("0.001")
MIN_PROFIT_UST = UST.to_amount(2)
MIN_START_AMOUNT = UST.to_amount(50)
MIN_UST_RESERVED_AMOUNT = 10
OPTIMIZATION_TOLERANCE = UST.to_amount("0.01")
