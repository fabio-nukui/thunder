import json

from cachetools.keys import _HashedTuple  # type: ignore


class _HashedTupleJSON(_HashedTuple):
    """Use JSON serialization as fallback for non-hashable arguments in caches"""

    __hashvalue = None

    def __hash__(self, hash=tuple.__hash__):
        hashvalue = self.__hashvalue
        if hashvalue is None:
            try:
                hashvalue = hash(self)
            except TypeError:
                args = []
                for arg in self:
                    try:
                        hash((arg,))
                    except TypeError:
                        args.append(json.dumps(arg))
                    else:
                        args.append(arg)
                hashvalue = hash(_HashedTupleJSON(args))
            self.__hashvalue = hashvalue
        return hashvalue


_kwmark = (_HashedTupleJSON,)  # Marker for calls with keyword arguments


def json_hashkey(*args, **kwargs):
    """Return a cache key for the specified hashable arguments."""
    if kwargs:
        return _HashedTupleJSON(args + sum(sorted(kwargs.items()), _kwmark))
    else:
        return _HashedTupleJSON(args)
