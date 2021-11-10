"""
Adapted from https://github.com/hephex/asyncache/blob/master/asyncache/__init__.py
"""
import asyncio
import functools
import inspect

from cachetools import keys


class _FetchEvent(asyncio.Event):
    pass


def cached(cache, key=keys.hashkey, lock=None):
    """
    Decorator to wrap a function or a coroutine with a memoizing callable
    that saves results in a cache.

    When ``lock`` is provided for a standard function, it's expected to
    implement ``__enter__`` and ``__exit__`` that will be used to lock
    the cache when gets updated. If it wraps a coroutine, ``lock``
    must implement ``__aenter__`` and ``__aexit__``.
    """

    def decorator(func):
        if inspect.iscoroutinefunction(func):
            if lock is None:

                async def async_wrapper_no_lock(*args, **kwargs):
                    k = key(*args, **kwargs)
                    try:
                        val = cache[k]
                        if type(val) is _FetchEvent:
                            await val.wait()
                            return cache[k]
                        else:
                            return val
                    except KeyError:
                        pass  # key not found

                    event = _FetchEvent()
                    cache[k] = event
                    try:
                        val = await func(*args, **kwargs)
                    except Exception:
                        del cache[k]
                        event.set()
                        raise
                    try:
                        cache[k] = val
                    except ValueError:  # val too large
                        del cache[k]
                    event.set()
                    return val

                return functools.update_wrapper(async_wrapper_no_lock, func)

            async def async_wrapper_lock(*args, **kwargs):
                k = key(*args, **kwargs)
                try:
                    val = cache[k]
                    if type(val) is _FetchEvent:
                        await val.wait()
                        async with lock:
                            return cache[k]
                    else:
                        return val
                except KeyError:
                    pass  # key not found
                event = _FetchEvent()
                async with lock:
                    cache[k] = event
                try:
                    val = await func(*args, **kwargs)
                except Exception:
                    async with lock:
                        del cache[k]
                    event.set()
                    raise
                async with lock:
                    try:
                        cache[k] = val
                    except ValueError:  # val too large
                        del cache[k]
                event.set()
                return val

            return functools.update_wrapper(async_wrapper_lock, func)

        else:
            if lock is None:

                def sync_wrapper_no_lock(*args, **kwargs):
                    k = key(*args, **kwargs)
                    try:
                        return cache[k]
                    except KeyError:
                        pass  # key not found

                    val = func(*args, **kwargs)
                    try:
                        cache[k] = val
                    except ValueError:
                        pass  # val too large
                    return val

                return functools.update_wrapper(sync_wrapper_no_lock, func)

            def sync_wrapper_lock(*args, **kwargs):
                k = key(*args, **kwargs)
                try:
                    with lock:
                        return cache[k]
                except KeyError:
                    pass  # key not found

                val = func(*args, **kwargs)
                try:
                    with lock:
                        cache[k] = val
                except ValueError:
                    pass  # val too large
                return val

            return functools.update_wrapper(sync_wrapper_lock, func)

    return decorator
