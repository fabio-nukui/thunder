import auth_secrets
from cexes.binance import BinanceClient
from chains.evm import BSCClient, EthereumClient
from chains.terra import TerraClient


def main():
    hd_wallet_secret = auth_secrets.hd_wallet()
    terra = TerraClient(hd_wallet_secret)
    bsc = BSCClient(hd_wallet_secret)
    ethereum = EthereumClient(hd_wallet_secret)

    binance_secret = auth_secrets.binance_api()
    binance = BinanceClient(binance_secret['api_key'], binance_secret['api_secret'])
    print(terra, bsc, ethereum, binance)


if __name__ == '__main__':
    main()
