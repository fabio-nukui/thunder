import importlib
from glob import glob

import pytest

files = [path[4:] for path in glob('src/**/*.py', recursive=True)]


@pytest.mark.parametrize("file", files)
def test_imports(file):
    module_name = file.replace('/', '.')[:-3]
    importlib.import_module(module_name)
