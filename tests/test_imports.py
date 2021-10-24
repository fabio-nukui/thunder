import importlib
from glob import glob

import pytest


@pytest.mark.parametrize("file", (path[4:] for path in glob("src/**/*.py", recursive=True)))
def test_imports(file: str):
    module_name = file.replace("/", ".")[:-3]
    importlib.import_module(module_name)
