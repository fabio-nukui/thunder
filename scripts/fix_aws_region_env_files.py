import configparser
from pathlib import Path


def get_region(filepath: Path = Path("~/.aws/config")) -> str:
    config = configparser.ConfigParser()
    config.read(filepath.expanduser())

    if "thunder" in config:
        return config["thunder"]["region"]
    else:
        return config["default"]["region"]


def main():
    region = get_region()
    print(f"Replacing config files using {region=}")
    for file in Path("env").iterdir():
        if not file.name.startswith(".env"):
            continue
        lines = open(file).readlines()
        for n, line in enumerate(lines):
            if "AWS_DEFAULT_REGION" in line:
                print(f"Fixing {file.name}")
                lines[n] = f"AWS_DEFAULT_REGION={region}\n"
        open(file, "w").write("".join(lines))


if __name__ == "__main__":
    main()
