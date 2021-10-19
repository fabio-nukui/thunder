import os
from decimal import Decimal

# Arbitrage params
STRATEGY = os.getenv("STRATEGY", "no_strategy")

# Logs
LOG_AWS = os.getenv("LOG_AWS", "").lower() == "true"

# Secrets
SECRET_HD_WALLET = os.getenv("SECRET_HD_WALLET", "")
SECRET_BINANCE_KEY = os.getenv("SECRET_BINANCE_KEY", "")

# Cache
DEFAULT_CACHE_TTL = float(os.getenv("DEFAULT_CACHE_TTL", "5.0"))

# Blockchain
RAISE_ON_SYNCING = os.getenv("RAISE_ON_SYNCING", "").lower() == "true"

# Terra
TERRA_CHAIN_ID = os.getenv("TERRA_CHAIN_ID", "columbus-5")
TERRA_LCD_URI = os.getenv("TERRA_LCD_URI", "https://lcd.terra.dev")
TERRA_FCD_URI = os.getenv("TERRA_FCD_URI", "https://fcd.terra.dev")
TERRA_RPC_HTTP_URI = os.getenv("TERRA_RPC_HTTP_URI", "")
TERRA_RPC_WEBSOCKET_URI = os.getenv("TERRA_RPC_WEBSOCKET_URI", "")
TERRA_BROADCAST_LCD_URIS = os.getenv("TERRA_BROADCAST_LCD_URIS", "https://lcd.terra.dev").split(",")
TERRA_CACHE_TTL = float(os.getenv("TERRA_CACHE_TTL", "5.0"))
TERRA_POLL_INTERVAL = float(os.getenv("TERRA_POLL_INTERVAL", "0.001"))
TERRA_GAS_ADJUSTMENT = Decimal(os.getenv("TERRA_GAS_ADJUSTMENT", "1.15"))
TERRA_GAS_MULTIPLIER = Decimal(os.getenv("TERRA_GAS_MULTIPLIER", "1.06001"))

# Ethereum
ETHEREUM_CHAIN_ID = int(os.getenv("ETHEREUM_CHAIN_ID", "1"))
ETHEREUM_RPC_URI = os.getenv("ETHEREUM_RPC_URI", "http://localhost:8545")
ETHEREUM_CACHE_TTL = float(os.getenv("ETHEREUM_CACHE_TTL", "3.5"))
ETHEREUM_POLL_INTERVAL = float(os.getenv("ETHEREUM_POLL_INTERVAL", "0.1"))
ETHEREUM_WEB3_MIDDEWARES = os.getenv("ETHEREUM_WEB3_MIDDEWARES", "").split(",")
ETHEREUM_GAS_MULTIPLIER = float(os.getenv("ETHEREUM_GAS_MULTIPLIER", "1.10"))
ETHEREUM_BASE_FEE_MULTIPLIER = float(os.getenv("ETHEREUM_BASE_FEE_MULTIPLIER", "2.1"))

# Binance Smart Chain
BSC_CHAIN_ID = int(os.getenv("BSC_CHAIN_ID", "56"))
BSC_RPC_URI = os.getenv("BSC_RPC_URI", "http://localhost:8547")
BSC_CACHE_TTL = float(os.getenv("BSC_CACHE_TTL", "2.9"))
BSC_POLL_INTERVAL = float(os.getenv("BSC_POLL_INTERVAL", "0.01"))
BSC_WEB3_MIDDEWARES = os.getenv("BSC_WEB3_MIDDEWARES", "geth_poa_middleware").split(",")
BSC_GAS_MULTIPLIER = float(os.getenv("BSC_GAS_MULTIPLIER", "1.0000000012"))

# Debug / optimization
CACHE_STATS = os.getenv("CACHE_STATS", "").lower() == "true"
CACHE_LOG_LEVEL = os.getenv("CACHE_LOG_LEVEL", "INFO")
