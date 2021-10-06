class BlockchainNewState(Exception):
    pass


class InsufficientLiquidity(Exception):
    pass


class IsBusy(Exception):
    pass


class NodeSyncing(Exception):
    def __init__(self, latest_block: int, *args) -> None:
        self.latest_block = latest_block
        super().__init__(latest_block, *args)


class NotContract(Exception):
    pass


class TxError(Exception):
    pass


class UnprofitableArbitrage(Exception):
    pass
