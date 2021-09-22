import os

# Logs
LOG_AWS = os.getenv('LOG_AWS') == 'True'

# Secrets
SECRET_NAME_MNEMONIC = os.environ['SECRET_NAME_MNEMONIC']
SECRET_NAME_BINANCE_KEY = os.environ['SECRET_NAME_BINANCE_KEY']

# Cache
DEFAULT_CACHE_TTL = float(os.getenv('DEFAULT_CACHE_TTL', 5.0))

# Terra
TERRA_CHAIN_ID = os.getenv('TERRA_CHAIN_ID', 'columbus-4')
TERRA_LCD_URI = os.getenv('TERRA_LCD_URI', 'https://lcd.terra.dev')
TERRA_FCD_URI = os.getenv('TERRA_FCD_URI', 'https://fcd.terra.dev')
TERRA_CACHE_TTL = float(os.getenv('TERRA_CACHE_TTL', 5.0))
TERRA_POLL_INTERVAL = float(os.getenv('TERRA_POLL_INTERVAL', 0.001))
TERRA_GAS_ADJUSTMENT = float(os.getenv('TERRA_GAS_ADJUSTMENT', '1.45'))

# Ethereum
ETHEREUM_CHAIN_ID = int(os.getenv('ETHEREUM_CHAIN_ID', '1'))
ETHEREUM_RPC_URI = os.getenv('ETHEREUM_RPC_URI', 'http://localhost:8545')
ETHEREUM_CACHE_TTL = float(os.getenv('ETHEREUM_CACHE_TTL', 5.0))
ETHEREUM_POLL_INTERVAL = float(os.getenv('ETHEREUM_POLL_INTERVAL', 0.1))
ETHEREUM_WEB3_MIDDEWARES = os.getenv('ETHEREUM_WEB3_MIDDEWARES', '').split(',')

# Binance Smart Chain
BSC_CHAIN_ID = int(os.getenv('BSC_CHAIN_ID', '56'))
BSC_RPC_URI = os.getenv('BSC_RPC_URI', 'http://localhost:8547')
BSC_CACHE_TTL = float(os.getenv('BSC_CACHE_TTL', 3.0))
BSC_POLL_INTERVAL = float(os.getenv('BSC_POLL_INTERVAL', 0.1))
BSC_WEB3_MIDDEWARES = os.getenv('BSC_WEB3_MIDDEWARES', 'geth_poa_middleware').split(',')

# Arbitrage params
STRATEGY = os.getenv('STRATEGY', 'no_strategy')

# Debug / optimization
CACHE_STATS = os.getenv('CACHE_STATS') == 'True'
CACHE_LOG_LEVEL = os.getenv('CACHE_LOG_LEVEL', 'INFO')
