"""Debugging tools"""
import code
import sys
import traceback
from contextlib import contextmanager
from types import TracebackType


def _tb_stack_list(tb: TracebackType) -> list[TracebackType]:
    frames = [tb]
    while tb.tb_next:
        tb = tb.tb_next
        frames.append(tb)
    return frames


def _interact_frame(n: int):
    traceback.print_exc()
    *_, tb = sys.exc_info()
    assert tb is not None, 'No traceback found'
    tb_list = _tb_stack_list(tb)
    frame = tb_list[n].tb_frame
    code.interact(local={**frame.f_globals, **frame.f_locals})


@contextmanager
def bp(n: int = -1):
    """Breakpoints code execution on exceptions.
    Args:
        n (int, optional): Frame to interact with, defaults to last frame (-1)

    Usage:
    >>> with bp():
    >>>    ...
    """
    try:
        yield
    except Exception:
        _interact_frame(n)
