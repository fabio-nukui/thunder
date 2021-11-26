import importlib
import os
import sys
from glob import glob

import pytest

sys.path.append(os.getcwd())

import_files = [
    *(path[4:] for path in glob("src/**/*.py", recursive=True)),
    *glob("app/*.py"),
]


@pytest.mark.parametrize("file", import_files)
def test_imports(file: str):
    module_name = file.replace("/", ".")[:-3]
    importlib.import_module(module_name)
