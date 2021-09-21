from chains.terra import TerraClient
from chains.evm import BSCClient, EthereumClient


def main():
    terra = TerraClient()
    bsc = BSCClient()
    ethereum = EthereumClient()
    print(terra, bsc, ethereum)


if __name__ == '__main__':
    main()
