import base64
import json
from typing import Union

import boto3

sm = boto3.client('secretsmanager')


def get_secret(name: str, decode_json: bool = True) -> Union[dict, str]:
    response = sm.get_secret_value(SecretId=name)
    if 'SecretString' in response:
        secret = response['SecretString']
    else:
        secret = base64.b64decode(response['SecretBinary'])
    if decode_json:
        return json.loads(secret)
    return secret
