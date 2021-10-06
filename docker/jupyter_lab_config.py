# flake8: noqa
SSL_FILES_DIR = "/home/thunder/work/docker"
c.ExtensionApp.open_browser = False
c.ServerApp.allow_origin = "*"
c.ServerApp.allow_password_change = False
c.ServerApp.certfile = f"{SSL_FILES_DIR}/jupyter-server.pem"
c.ServerApp.ip = "*"
c.ServerApp.keyfile = f"{SSL_FILES_DIR}/jupyter-server.key"
c.ServerApp.password = (
    "argon2:$argon2id$v=19$m=10240,t=10,p=8$1GnzWJ6NTZPmQXkDt3rO2A$JaSJFBZh+NXWbIDp3ApD+g"
)
