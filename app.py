from chains.evm import BSCClient, EthereumClient
from chains.terra import TerraClient


def main():
    terra = TerraClient()
    bsc = BSCClient()
    ethereum = EthereumClient()
    print(terra, bsc, ethereum)


if __name__ == '__main__':
    main()
