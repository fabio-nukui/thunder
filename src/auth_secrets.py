import configs
import utils


def hd_wallet(secret_name: str = configs.SECRET_NAME_MNEMONIC) -> dict:
    secret = utils.aws.get_secret(secret_name, decode_json=True)
    secret['account'] = int(secret.get('account', 0))

    return secret


def binance_api(secret_name: str = configs.SECRET_NAME_BINANCE_KEY) -> dict:
    return utils.aws.get_secret(secret_name, decode_json=True)
