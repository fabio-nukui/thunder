class BlockchainNewState(Exception):
    pass


class InsufficientLiquidity(Exception):
    pass


class IsBusy(Exception):
    pass


class NodeSyncing(Exception):
    def __init__(self, latest_height: int, *args) -> None:
        self.latest_height = latest_height
        super().__init__(f"Latest height={latest_height}", *args)


class NotContract(Exception):
    pass


class TxError(Exception):
    pass


class UnprofitableArbitrage(Exception):
    pass
