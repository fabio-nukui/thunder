from __future__ import annotations

from terra_sdk.util.base import create_demux, create_demux_proto
from terra_sdk.util.parse_msg import msgs

from .osmosis.data.gamm import MsgSwapExactAmountIn

osmosis_msgs = [MsgSwapExactAmountIn]

parse_msg = create_demux(msgs + osmosis_msgs)
parse_proto = create_demux_proto(msgs + osmosis_msgs)
