import json

from flask import Flask

app = Flask(__name__)


@app.route("/healthcheck")
def healthcheck():
    return json.dumps({"value": "OK"})


if __name__ == "__main__":
    app.run(debug=False)
