class BlockchainNewState(Exception):
    pass


class FeeEstimationError(Exception):
    pass


class InsufficientLiquidity(Exception):
    pass


class IsBusy(Exception):
    pass


class MaxSpreadAssertion(Exception):
    pass


class NodeSyncing(Exception):
    def __init__(self, latest_height: int, *args) -> None:
        self.latest_height = latest_height
        super().__init__(f"Latest height={latest_height}", *args)


class NotContract(Exception):
    pass


class OptimizationError(Exception):
    pass


class TokenAmountRoundingError(Exception):
    pass


class TokenNotFound(Exception):
    pass


class TxAlreadyBroadcasted(Exception):
    pass


class UnprofitableArbitrage(Exception):
    pass
