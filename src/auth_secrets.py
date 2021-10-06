import configs
import utils


def hd_wallet(secret_name: str = configs.SECRET_HD_WALLET) -> dict:
    secret = utils.aws.get_secret(secret_name)
    assert "mnemonic" in secret
    secret.setdefault("account", 0)

    return secret


def binance_api(secret_name: str = configs.SECRET_BINANCE_KEY) -> dict:
    secret = utils.aws.get_secret(secret_name)
    assert "api_key" in secret
    assert "api_secret" in secret

    return secret
