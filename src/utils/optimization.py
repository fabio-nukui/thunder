import logging
from decimal import Decimal
from typing import Callable

log = logging.getLogger(__name__)

DEFAULT_MAX_ITER = 100
BISSECTION_SEARCH_EXPANSION = 2


def bissection_optimizer(
    func: Callable[[Decimal], Decimal],
    x0: Decimal,
    dx: Decimal,
    tol: Decimal = Decimal(10 ** 18),
    max_iter: int = DEFAULT_MAX_ITER,
) -> tuple[Decimal, Decimal]:
    """Optimizes function by searching for point where derivative is zero using bissection search.
    Works only if derivative(x0) > 0. Guaranteed to converge for convex functions

    Args:
        func (Callable): Function to be maximized
        x0 (Decimal): Left-most boundary to beggin search
        dx (Decimal): Interval to calculate derivatives
        tol (int): Absolute tolerance between iterations to stop optimization
        max_iter (Decimal): Maximum number of iterations

    Returns:
        tuple[Decimal, Decimal]: Result in x and func(x)
    """
    def derivative(x: Decimal) -> Decimal:
        return (func(x + dx) - func(x - dx)) / (2 * dx)
    x_left = x0
    x_right = x0 * BISSECTION_SEARCH_EXPANSION

    y_left = derivative(x0)
    assert y_left >= 0, "bissection_optimizer only work for f'(x0) >= 0"

    x = bissection_search(derivative, x_left, x_right, tol, max_iter, y_left=y_left)
    return x, func(x)


def bissection_search(
    func: Callable[[Decimal], Decimal],
    x_left: Decimal,
    x_right: Decimal,
    tol: Decimal,
    max_iter: int,
    i: int = 0,
    y_left: Decimal = None,
    y_right: Decimal = None,
) -> Decimal:
    i += 1
    if x_right - x_left < tol or i >= max_iter:
        return (x_left + x_right) / 2
    y_left = func(x_left) if y_left is None else y_left
    y_right = func(x_right) if y_right is None else y_right
    if y_right > 0:
        x_right *= BISSECTION_SEARCH_EXPANSION
        return bissection_search(func, x_left, x_right, tol, max_iter, i, y_left)
    x_mid = (x_left + x_right) // 2
    y_mid = func(x_mid)
    if y_mid > 0:
        x_left = x_mid
        y_left = y_mid
    else:
        x_right = x_mid
        y_right = y_mid
    return bissection_search(func, x_left, x_right, tol, max_iter, i, y_left, y_right)
