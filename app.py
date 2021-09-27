from cexes.binance import BinanceClient
from chains.evm import BSCClient, EthereumClient
from chains.terra import TerraClient


def main():
    terra_client = TerraClient()
    bsc_client = BSCClient()
    ethereum_client = EthereumClient()

    binance_client = BinanceClient()
    print(terra_client, bsc_client, ethereum_client, binance_client)


if __name__ == '__main__':
    main()
