import os

# Logs
LOG_AWS = os.getenv('LOG_AWS') == 'True'

# Terra blockchain configs
CHAIN_ID = os.getenv('FCD_URI', 'columbus-4')
LCD_URI = os.getenv('LCD_URI', 'https://lcd.terra.dev')
FCD_URI = os.getenv('FCD_URI', 'https://fcd.terra.dev')
TERRA_GAS_ADJUSTMENT = float(os.getenv('GAS_ADJUSTMENT', '1.4'))

# Secrets
MNEMONIC_SECRET_NAME = os.environ['MNEMONIC_SECRET_NAME']
BINANCE_KEY_SECRET_NAME = os.environ['BINANCE_KEY_SECRET_NAME']

# Connection params
CACHE_TTL = float(os.environ['CACHE_TTL'])
POLL_INTERVAL = float(os.environ['POLL_INTERVAL'])

# Arbitrage params
STRATEGY = os.getenv('STRATEGY', 'no_strategy')

# Debug / optimization
CACHE_STATS = os.getenv('CACHE_STATS') == 'True'
CACHE_LOG_LEVEL = os.getenv('CACHE_LOG_LEVEL', 'INFO')
