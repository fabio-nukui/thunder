import re
from typing import Iterable, Sequence

from chains.cosmos.terra import terraswap
from exceptions import InsufficientLiquidity, NoPairFound


async def pairs_from_factories(
    terraswap_factories: Sequence[terraswap.Factory],
    symbol_0: str = None,
    symbol_1: str = None,
    excluded_symbols: Iterable[str] = None,
    include_repeated: bool = False,
) -> list[terraswap.LiquidityPair]:
    assert symbol_0 is None or "\\" not in symbol_0
    assert symbol_1 is None or "\\" not in symbol_1
    if symbol_0 == symbol_1 and symbol_0 is not None:
        raise NoPairFound(f"Invalid pair [{symbol_0}]-[{symbol_1}]")

    symbol_0 = symbol_0 or r"[\w\-]+"
    symbol_1 = symbol_1 or r"[\w\-]+"
    excluded_symbols = set(excluded_symbols) if excluded_symbols else set()

    pat = re.compile(fr"\[({symbol_0})\]-\[({symbol_1})\]|\[({symbol_1})\]-\[({symbol_0})\]")
    pairs = []
    for factory in terraswap_factories:
        for pair_symbol in factory.pairs_addresses:
            if match := pat.match(pair_symbol):
                if not excluded_symbols & set(match.groups()):
                    try:
                        pair = await factory.get_pair(pair_symbol)
                    except InsufficientLiquidity:
                        continue
                    if include_repeated or pair.tokens[0] != pair.tokens[1]:
                        pairs.append(pair)
    if not pairs:
        raise NoPairFound(f"No pair found for [{symbol_0}]-[{symbol_1}]")
    return pairs
