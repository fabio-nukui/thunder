from __future__ import annotations

import asyncio
import json
from copy import copy
from decimal import Decimal
from typing import TYPE_CHECKING, Sequence, TypeVar

from cosmos_sdk.core import AccAddress, Coins
from cosmos_sdk.core.msg import Msg
from cosmos_sdk.core.wasm import MsgExecuteContract

from utils.cache import CacheGroup, ttl_cache

from . import terraswap
from .denoms import LUNA
from .native_liquidity_pair import BaseTerraLiquidityPair
from .token import TerraCW20Token, TerraNativeToken, TerraToken, TerraTokenAmount

if TYPE_CHECKING:
    from .client import TerraClient

ADDRESSES_FILE = "resources/addresses/cosmos/{chain_id}/prism.json"
_CONFIG_CACHE_TTL = 60
_MAX_ITER_PRISM_SPLIT = 10
_PRISM_SPLIT_RTOL = Decimal("0.0001")

_PrismVaultT = TypeVar("_PrismVaultT", bound="PrismVault")


def _get_addresses(chain_id: str) -> dict[str, AccAddress]:
    return json.load(open(ADDRESSES_FILE.format(chain_id=chain_id)))


class XPrismMinter(BaseTerraLiquidityPair):
    contract_addr: AccAddress
    client: TerraClient
    prism: TerraCW20Token
    xprism: TerraCW20Token
    _exchange_rate: Decimal

    @classmethod
    async def new(cls, client: TerraClient) -> XPrismMinter:
        self = super().__new__(cls)
        self.client = client
        self.contract_addr = _get_addresses(client.chain_id)["prism_gov"]

        config = await self.get_config()
        self.tokens = self.prism, self.xprism = await asyncio.gather(
            TerraCW20Token.from_contract(config["prism_token"], client),
            TerraCW20Token.from_contract(config["xprism_token"], client),
        )

        self._exchange_rate = Decimal(1)
        self.stop_updates = False

        return self

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.contract_addr})"

    async def op_swap(
        self,
        sender: AccAddress,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = True,
        simulate: bool = False,
    ) -> tuple[TerraTokenAmount, Sequence[Msg]]:
        if amount_in.token == self.prism:
            msg = self.get_deposit_msg(sender, amount_in)
        else:
            msg = self.get_withdraw_msg(sender, amount_in)
        amount_out = await self.get_swap_amount_out(amount_in, safety_margin, simulate, msg)
        return amount_out, [msg]

    async def get_exchange_rate(self) -> Decimal:
        if not self.stop_updates:
            state = await self.get_state()
            self._exchange_rate = Decimal(state["exchange_rate"])
        return self._exchange_rate

    @ttl_cache(CacheGroup.TERRA, ttl=_CONFIG_CACHE_TTL)
    async def get_config(self) -> dict:
        return await self.client.contract_query(self.contract_addr, {"config": {}})

    @ttl_cache(CacheGroup.TERRA)
    async def get_state(self) -> dict:
        return await self.client.contract_query(self.contract_addr, {"xprism_state": {}})

    async def get_swap_amount_out(
        self,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = True,
        simulate: bool = False,
        simulate_msg: Msg = None,
    ) -> TerraTokenAmount:
        if simulate:
            raise NotImplementedError
        exchange_rate = await self.get_exchange_rate()
        if amount_in.token == self.prism:
            amount_out = self.xprism.to_amount(amount_in.amount / exchange_rate)
        elif amount_in.token == self.xprism:
            amount_out = self.prism.to_amount(amount_in.amount * exchange_rate)
        else:
            raise TypeError(f"{amount_in.token=} not PRISM nor xPRISM")
        return amount_out.safe_margin(safety_margin)

    def get_deposit_msg(
        self,
        sender: AccAddress,
        amount_deposit: TerraTokenAmount,
    ) -> MsgExecuteContract:
        return MsgExecuteContract(
            sender=sender,
            contract=self.prism.contract_addr,
            execute_msg={
                "send": {
                    "msg": "eyJtaW50X3hwcmlzbSI6e319",  # {"mint_xprism":{}}
                    "amount": str(amount_deposit.int_amount),
                    "contract": self.contract_addr,
                }
            },
        )

    def get_withdraw_msg(
        self,
        sender: AccAddress,
        amount_withdraw: TerraTokenAmount,
    ) -> MsgExecuteContract:
        raise NotImplementedError("xPRISM burn not implemented")

    async def simulate_reserve_change(
        self, amounts: tuple[TerraTokenAmount, TerraTokenAmount]
    ) -> XPrismMinter:
        return self

    async def get_reserve_changes_from_msg(
        self, msg: dict
    ) -> tuple[TerraTokenAmount, TerraTokenAmount]:
        raise NotImplementedError


class PrismVault:
    contract_addr: AccAddress
    client: TerraClient
    base_token: TerraToken
    c_token: TerraCW20Token
    p_token: TerraCW20Token
    y_token: TerraCW20Token

    @classmethod
    async def new(
        cls: type[_PrismVaultT],
        client: TerraClient,
        vault_name: str,
        base_token: TerraToken,
        asset_name: str,
    ) -> _PrismVaultT:
        self = super().__new__(cls)
        self.client = client
        self.contract_addr = _get_addresses(client.chain_id)[vault_name]
        self.base_token = base_token

        config = await self.get_config()
        self.c_token, self.p_token, self.y_token = await asyncio.gather(
            TerraCW20Token.from_contract(config[f"c{asset_name}_contract"], client),
            TerraCW20Token.from_contract(config[f"p{asset_name}_contract"], client),
            TerraCW20Token.from_contract(config[f"y{asset_name}_contract"], client),
        )
        return self

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.contract_addr})"

    def get_bond_msg(
        self,
        sender: AccAddress,
        amount_bond: TerraTokenAmount,
    ) -> MsgExecuteContract:
        assert amount_bond.token == self.base_token

        if isinstance(self.base_token, TerraNativeToken):
            return MsgExecuteContract(
                sender=sender,
                contract=self.contract_addr,
                execute_msg={"bond": {}},
                coins=Coins([amount_bond.to_coin()]),
            )
        raise NotImplementedError

    def get_bond_split_msg(
        self,
        sender: AccAddress,
        amount_bond: TerraTokenAmount,
    ) -> MsgExecuteContract:
        assert amount_bond.token == self.base_token

        if isinstance(self.base_token, TerraNativeToken):
            return MsgExecuteContract(
                sender=sender,
                contract=self.contract_addr,
                execute_msg={"bond_split": {}},
                coins=Coins([amount_bond.to_coin()]),
            )
        raise NotImplementedError

    def get_split_msgs(
        self,
        sender: AccAddress,
        amount_bond: TerraTokenAmount,
    ) -> list[MsgExecuteContract]:
        assert amount_bond.token == self.c_token
        msg_increase_allowance = self.c_token.build_msg_increase_allowance(
            self.contract_addr, sender, amount_bond.int_amount
        )
        msg_split = MsgExecuteContract(
            sender=sender,
            contract=self.contract_addr,
            execute_msg={"split": {"amount": str(amount_bond.int_amount)}},
        )
        return [msg_increase_allowance, msg_split]

    def get_merge_msgs(
        self,
        sender: AccAddress,
        amount_merge: int,
    ) -> list[MsgExecuteContract]:
        msg_increase_allowance_pluna = self.p_token.build_msg_increase_allowance(
            self.contract_addr, sender, amount_merge
        )
        msg_increase_allowance_yluna = self.y_token.build_msg_increase_allowance(
            self.contract_addr, sender, amount_merge
        )
        msg_merge = MsgExecuteContract(
            sender=sender,
            contract=self.contract_addr,
            execute_msg={"merge": {"amount": str(amount_merge)}},
        )
        return [msg_increase_allowance_pluna, msg_increase_allowance_yluna, msg_merge]

    def get_unbond_msg(
        self,
        sender: AccAddress,
        amount_unbound: TerraTokenAmount,
    ) -> MsgExecuteContract:
        raise NotImplementedError

    @ttl_cache(CacheGroup.TERRA, ttl=_CONFIG_CACHE_TTL)
    async def get_config(self) -> dict:
        return await self.client.contract_query(self.contract_addr, {"config": {}})

    @ttl_cache(CacheGroup.TERRA)
    async def get_state(self) -> dict:
        return await self.client.contract_query(self.contract_addr, {"state": {}})


class LunaVault(PrismVault):
    @classmethod
    async def new(
        cls,
        client: TerraClient,
        vault_name: str = "luna_vault",
        base_token: TerraToken = LUNA,
        asset_name: str = "luna",
    ) -> LunaVault:
        return await super().new(client, vault_name, base_token, asset_name)


class PrismLunaBonder(BaseTerraLiquidityPair):
    vault: LunaVault

    @classmethod
    async def new(cls, client: TerraClient) -> PrismLunaBonder:
        self = super().__new__(cls)

        self.client = client
        self.vault = await LunaVault.new(client)
        self.tokens = self.vault.base_token, self.vault.c_token
        self.stop_updates = False

        return self

    async def op_swap(
        self,
        sender: AccAddress,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = True,
        simulate: bool = False,
    ) -> tuple[TerraTokenAmount, Sequence[Msg]]:
        amount_out = await self.get_swap_amount_out(amount_in, safety_margin, simulate)
        if amount_in.token == self.vault.base_token:
            msg = self.vault.get_bond_msg(sender, amount_in)
        else:
            msg = self.vault.get_unbond_msg(sender, amount_in)
        return amount_out, [msg]

    async def get_swap_amount_out(
        self,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = False,
        simulate: bool = False,
        simulate_msg: Msg = None,
    ) -> TerraTokenAmount:
        if amount_in.token == self.vault.base_token:
            return self.vault.c_token.to_amount(amount_in.amount)
        if amount_in.token == self.vault.c_token:
            return self.vault.base_token.to_amount(amount_in.amount)
        valid_tokens = self.vault.c_token, self.vault.base_token
        raise TypeError(f"{amount_in.token=} not in {valid_tokens}")

    async def simulate_reserve_change(
        self,
        amounts: tuple[TerraTokenAmount, TerraTokenAmount],
    ) -> PrismLunaBonder:
        return self

    async def get_reserve_changes_from_msg(
        self, msg: dict
    ) -> tuple[TerraTokenAmount, TerraTokenAmount]:
        raise NotImplementedError


class PrismCLunaPair(BaseTerraLiquidityPair):
    vault: LunaVault
    prism: TerraCW20Token
    pair_pluna: terraswap.LiquidityPair
    pair_yluna: terraswap.LiquidityPair

    @classmethod
    async def new(
        cls,
        client: TerraClient,
        prism: TerraCW20Token,
        pair_pluna: terraswap.LiquidityPair,
        pair_yluna: terraswap.LiquidityPair,
    ) -> PrismCLunaPair:
        self = super().__new__(cls)

        self.client = client
        self.prism = prism
        self.pair_pluna = pair_pluna
        self.pair_yluna = pair_yluna

        self.vault = await LunaVault.new(client)
        self.tokens = self.prism, self.vault.c_token
        self.stop_updates = False

        return self

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.vault.contract_addr})"

    async def op_swap(
        self,
        sender: AccAddress,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = True,
        simulate: bool = False,
    ) -> tuple[TerraTokenAmount, Sequence[Msg]]:
        if amount_in.token == self.vault.c_token:
            msgs_split = self.vault.get_split_msgs(sender, amount_in)
            prism_amount_p, msgs_swap_pluna = await self.pair_pluna.op_swap(
                sender,
                self.vault.p_token.to_amount(amount_in.amount),
                safety_margin,
                simulate,
            )
            prism_amount_y, msgs_swap_yluna = await self.pair_yluna.op_swap(
                sender,
                self.vault.y_token.to_amount(amount_in.amount),
                safety_margin,
                simulate,
            )
            msgs = [*msgs_split, *msgs_swap_pluna, *msgs_swap_yluna]
            return prism_amount_p + prism_amount_y, msgs
        elif amount_in.token == self.prism:
            prism_to_pluna, prism_to_yluna = await self._get_prism_split(amount_in)
            pluna_amount, msgs_swap_pluna = await self.pair_pluna.op_swap(
                sender, prism_to_pluna, safety_margin, simulate
            )
            yluna_amount, msgs_swap_yluna = await self.pair_yluna.op_swap(
                sender, prism_to_yluna, safety_margin, simulate
            )
            amount_merge = min(pluna_amount.int_amount, yluna_amount.int_amount)
            msgs_merge = self.vault.get_merge_msgs(sender, amount_merge)
            msgs = [*msgs_swap_pluna, *msgs_swap_yluna, *msgs_merge]
            return self.vault.c_token.to_amount(int_amount=amount_merge), msgs
        else:
            raise TypeError(f"{amount_in.token=} not PRISM nor cLUNA")

    async def _get_prism_split(
        self,
        prism_amount_in: TerraTokenAmount,
        prism_to_pluna: TerraTokenAmount = None,
        _n_iter: int = 0,
    ) -> tuple[TerraTokenAmount, TerraTokenAmount]:
        prism_to_pluna = prism_amount_in / 2 if prism_to_pluna is None else prism_to_pluna
        prism_to_yluna = prism_amount_in - prism_to_pluna
        pluna_amount, yluna_amount = await asyncio.gather(
            self.pair_pluna.get_swap_amount_out(prism_to_pluna),
            self.pair_yluna.get_swap_amount_out(prism_to_yluna),
        )
        pluna_price = pluna_amount.amount / prism_to_pluna.amount
        yluna_price = yluna_amount.amount / prism_to_yluna.amount
        prism_to_pluna = prism_amount_in * yluna_price / (pluna_price + yluna_price)
        prism_to_yluna = prism_amount_in * pluna_price / (pluna_price + yluna_price)

        error = abs(1 - pluna_amount.amount / yluna_amount.amount)
        if error < _PRISM_SPLIT_RTOL or _n_iter >= _MAX_ITER_PRISM_SPLIT:
            return prism_to_pluna, prism_to_yluna
        return await self._get_prism_split(prism_amount_in, prism_to_pluna, _n_iter + 1)

    async def get_swap_amount_out(
        self,
        amount_in: TerraTokenAmount,
        safety_margin: bool | int = False,
        simulate: bool = False,
        simulate_msg: Msg = None,
    ) -> TerraTokenAmount:
        if amount_in.token == self.vault.c_token:
            prism_amount_p = await self.pair_pluna.get_swap_amount_out(
                self.vault.p_token.to_amount(amount_in.amount),
                safety_margin,
                simulate,
            )
            prism_amount_y = await self.pair_yluna.get_swap_amount_out(
                self.vault.y_token.to_amount(amount_in.amount),
                safety_margin,
                simulate,
            )
            return prism_amount_p + prism_amount_y

        elif amount_in.token == self.prism:
            prism_to_pluna, prism_to_yluna = await self._get_prism_split(amount_in)
            pluna_amount = await self.pair_pluna.get_swap_amount_out(
                prism_to_pluna, safety_margin, simulate
            )
            yluna_amount = await self.pair_yluna.get_swap_amount_out(
                prism_to_yluna, safety_margin, simulate
            )
            amount_merge = min(pluna_amount.int_amount, yluna_amount.int_amount)
            return self.vault.c_token.to_amount(int_amount=amount_merge)
        else:
            raise TypeError(f"{amount_in.token=} not PRISM nor cLUNA")

    async def simulate_reserve_change(
        self: PrismCLunaPair,
        amounts: tuple[TerraTokenAmount, TerraTokenAmount],
    ) -> PrismCLunaPair:
        simulation = copy(self)
        simulation.stop_updates = True

        if amounts[0].token < amounts[1].token:
            sorted_tokens = amounts[0].token, amounts[1].token
        else:
            sorted_tokens = amounts[1].token, amounts[0].token

        if sorted_tokens == self.pair_pluna.sorted_tokens:
            simulation.pair_pluna = await self.pair_pluna.simulate_reserve_change(amounts)
        elif sorted_tokens == self.pair_yluna.sorted_tokens:
            simulation.pair_yluna = await self.pair_yluna.simulate_reserve_change(amounts)
        else:
            raise TypeError(f"{amounts=} must contain pLUNA-PRISM or yLUNA-PRISM")
        return simulation

    async def get_reserve_changes_from_msg(
        self, msg: dict
    ) -> tuple[TerraTokenAmount, TerraTokenAmount]:
        try:
            return await self.pair_pluna.get_reserve_changes_from_msg(msg)
        except Exception:
            return await self.pair_yluna.get_reserve_changes_from_msg(msg)
