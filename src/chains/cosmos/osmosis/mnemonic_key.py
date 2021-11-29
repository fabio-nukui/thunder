from cosmos_sdk.core import AccAddress, AccPubKey, ValAddress, ValPubKey
from cosmos_sdk.key.key import get_bech
from cosmos_sdk.key.mnemonic import MnemonicKey as TerraMnemonicKey

OSMO_COIN_TYPE = 118


class MnemonicKey(TerraMnemonicKey):
    def __init__(
        self,
        mnemonic: str = None,
        account: int = 0,
        index: int = 0,
        coin_type: int = OSMO_COIN_TYPE,
    ):
        super().__init__(mnemonic, account, index, coin_type)

    @property
    def acc_address(self) -> AccAddress:
        if not self.raw_address:
            raise ValueError("could not compute acc_address: missing raw_address")
        return AccAddress(get_bech("osmo", self.raw_address.hex()))

    @property
    def val_address(self) -> ValAddress:
        if not self.raw_address:
            raise ValueError("could not compute val_address: missing raw_address")
        return ValAddress(get_bech("osmovaloper", self.raw_address.hex()))

    @property
    def acc_pubkey(self) -> AccPubKey:
        if not self.raw_pubkey:
            raise ValueError("could not compute acc_pubkey: missing raw_pubkey")
        return AccPubKey(get_bech("osmopub", self.raw_pubkey.hex()))

    @property
    def val_pubkey(self) -> ValPubKey:
        if not self.raw_pubkey:
            raise ValueError("could not compute val_pubkey: missing raw_pubkey")
        return ValPubKey(get_bech("osmovaloperpub", self.raw_pubkey.hex()))
