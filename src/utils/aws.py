import base64
import json

import boto3

sm = boto3.client("secretsmanager")


def get_secret(name: str) -> dict:
    """Return value from AWS secretsmanager. Value must be a JSON-encoded object"""
    response = sm.get_secret_value(SecretId=name)
    if "SecretString" in response:
        secret = response["SecretString"]
    else:
        secret = base64.b64decode(response["SecretBinary"])
    return json.loads(secret)
