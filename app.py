import auth_secrets
from cexes.binance import BinanceClient
from chains.evm import BSCClient, EthereumClient
from chains.terra import TerraClient


def main():
    hd_wallet_secret = auth_secrets.hd_wallet()
    terra_client = TerraClient(hd_wallet_secret)
    bsc_client = BSCClient(hd_wallet_secret)
    ethereum_client = EthereumClient(hd_wallet_secret)

    binance_secret = auth_secrets.binance_api()
    binance_client = BinanceClient(binance_secret['api_key'], binance_secret['api_secret'])
    print(terra_client, bsc_client, ethereum_client, binance_client)


if __name__ == '__main__':
    main()
