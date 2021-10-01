"""Debugging tools"""
import code
import sys
import traceback
from contextlib import contextmanager


def _get_last_frame(tb):
    return _get_last_frame(tb.tb_next) if tb.tb_next else tb


def _interact_last_frame():
    traceback.print_exc()
    *_, tb = sys.exc_info()
    frame = _get_last_frame(tb).tb_frame  # type: ignore
    code.interact(local={**frame.f_globals, **frame.f_locals})


@contextmanager
def bp():
    """Breakpoints code execution on exceptions.
    Usage:
    >>> with bp():
    >>>    ...
    """
    try:
        yield
    except Exception:
        _interact_last_frame()
