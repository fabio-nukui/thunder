#! /usr/bin/env python

import configparser
from copy import copy
from pathlib import Path

LOG_PREFIX_FILE = Path("~/INSTANCE_NAME").expanduser()


def get_region(filepath: Path = Path("~/.aws/config")) -> str:
    config = configparser.ConfigParser()
    config.read(filepath.expanduser())

    if "thunder" in config:
        return config["thunder"]["region"]
    else:
        return config["default"]["region"]


def get_log_prefix() -> str:
    if not LOG_PREFIX_FILE.exists():
        return ""
    return open(LOG_PREFIX_FILE).read().strip()


def main():
    region = get_region()
    log_prefix = get_log_prefix()
    print("Fixing env files with local configs")
    for file in Path("env").iterdir():
        if not file.name.startswith(".env"):
            continue
        lines_orig = open(file).readlines()
        lines = copy(lines_orig)
        for n, line in enumerate(lines):
            if "AWS_DEFAULT_REGION" in line:
                if line != (fix := f"AWS_DEFAULT_REGION={region}\n"):
                    print(f"Fixing region on {file.name}")
                    lines[n] = fix
            if "LOG_AWS_PREFIX" in line and log_prefix:
                if line != (fix := f"LOG_AWS_PREFIX={log_prefix}\n"):
                    print(f"Fixing log prefix on {file.name}")
                    lines[n] = fix
        if lines != lines_orig:
            open(file, "w").write("".join(lines))


if __name__ == "__main__":
    main()
