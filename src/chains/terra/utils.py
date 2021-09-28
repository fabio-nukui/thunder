import base64
import json


def encode_msg(msg: dict) -> str:
    return base64.b64encode(json.dumps(msg).encode('utf-8')).decode('ascii')
