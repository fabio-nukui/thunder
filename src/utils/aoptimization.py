import logging
from decimal import Decimal
from typing import Awaitable, Callable

from exceptions import OptimizationError

log = logging.getLogger(__name__)

DEFAULT_MAX_ITER = 100
BISSECTION_SEARCH_EXPANSION = 2
_MIN_LEFT_BOUND_FACTOR = 10


async def optimize(
    func: Callable[[Decimal], Awaitable[Decimal]],
    x0: Decimal,
    dx: Decimal,
    tol: Decimal = Decimal(0.01),
    max_iter: int = DEFAULT_MAX_ITER,
    use_fallback: bool = True,
) -> tuple[Decimal, Decimal]:
    """Maximizes function using Newton's method and finite differences

    Args:
        x0 (Decimal): Initial guess
        dx (Decimal): Interval to calculate derivatives
        tol (Decimal): Absolute tolerance between iterations to stop optimization

    Returns:
        tuple[Decimal, Decimal]: Result in x and func(x)
    """
    try:
        return await optimize_newton(func, x0, dx, tol, max_iter)
    except Exception:
        if not use_fallback:
            raise
        log.debug("Error on newton optimization", exc_info=True)
        return await optimize_bissection(func, x0, dx, tol, max_iter)


async def optimize_newton(
    func: Callable[[Decimal], Awaitable[Decimal]],
    x0: Decimal,
    dx: Decimal,
    tol: Decimal = Decimal(0.01),
    max_iter: int = DEFAULT_MAX_ITER,
    positive_only: bool = True,
) -> tuple[Decimal, Decimal]:
    """Optimizes function using Newton's method and finite differences
    Guaranteed to converge for convex functions

    Args:
        func (Callable): Function to be maximized
        x0 (Decimal): Initial guess
        dx (Decimal): Interval to calculate derivatives
        tol (Decimal): Absolute tolerance between iterations to stop optimization
        max_iter (int): Maximum number of iterations

    Returns:
        tuple[Decimal, Decimal]: Result in x and func(x)
    """
    x_i = x_i_next = x0
    for i in range(max_iter):
        f_x_i = await func(x_i)
        f_x_ip = await func(x_i + dx)
        f_x_im = await func(x_i - dx)
        first_derivative = (f_x_ip - f_x_im) / (2 * dx)
        second_derivative = (f_x_ip - 2 * f_x_i + f_x_im) / (dx ** 2)
        x_i_next = x_i - first_derivative / second_derivative
        if x_i_next < 0 and x_i < 0 and positive_only:
            raise OptimizationError(f"Negative result when {positive_only=}")
        if abs(x_i_next - x_i) < tol:
            break
        x_i = x_i_next
    return x_i_next, await func(x_i_next)


async def optimize_bissection(
    func: Callable[[Decimal], Awaitable[Decimal]],
    x0: Decimal,
    dx: Decimal,
    tol: Decimal = Decimal(0.01),
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

    async def derivative(x: Decimal) -> Decimal:
        return (await func(x + dx) - await func(x - dx)) / (2 * dx)

    x_left = x0
    x_right = x0 * BISSECTION_SEARCH_EXPANSION

    y_left = await derivative(x_left)

    if y_left < 0:
        x_left /= _MIN_LEFT_BOUND_FACTOR
        y_left = await derivative(x_left)
        if y_left < 0:
            raise OptimizationError("bissection_optimizer only work for f'(x0) >= 0")

    x = await bissection_search(derivative, x_left, x_right, tol, max_iter, y_left=y_left)
    return x, await func(x)


async def bissection_search(
    func: Callable[[Decimal], Awaitable[Decimal]],
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
    y_left = await func(x_left) if y_left is None else y_left
    y_right = await func(x_right) if y_right is None else y_right
    if y_right > 0:
        x_right *= BISSECTION_SEARCH_EXPANSION
        return await bissection_search(func, x_left, x_right, tol, max_iter, i, y_left)
    x_mid = (x_left + x_right) / 2
    y_mid = await func(x_mid)
    if y_mid > 0:
        x_left = x_mid
        y_left = y_mid
    else:
        x_right = x_mid
        y_right = y_mid
    return await bissection_search(func, x_left, x_right, tol, max_iter, i, y_left, y_right)
