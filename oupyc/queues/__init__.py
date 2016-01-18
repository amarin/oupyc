# -*- coding: utf-8 -*-
import logging
from threading import RLock, Condition
from oupyc.internals.variable import NamedObject

_l = logging.getLogger(__name__)
_l.setLevel(logging.INFO)
debug, info, warning, error, critical = _l.debug, _l.info, _l.warning, _l.error,  _l.critical


class QueueItemNotFoundException(Exception):
    pass


class SimpleQueue(NamedObject):
    kwargs = []

    @classmethod
    def make_kwargs(cls, *args):
        return dict(zip(cls.kwargs, args))

    @classmethod
    def instance(cls, *args):
        kwargs = cls.make_kwargs(*args)
        return cls(**kwargs)

    def __init__(self, **kwargs):
        super(SimpleQueue, self).__init__(**kwargs)
        self._queue = []
        self._mutex = RLock()

    def put(self, val):
        with self._mutex:
            self._queue.append(val)
            self.on_change()

    def get(self):
        with self._mutex:
            item = self._queue.pop(0)
            self.on_change()
            return item

    def len(self):
        return len(self._queue)

    def __len__(self):
        return len(self._queue)

    length = property(len, None, None, "Queue length")

    def pop_filtered(self, filter_func):
        with self._mutex:
            filtered = filter(filter_func, self._queue)
            if filtered:
                self._queue = [x for x in self._queue if x not in filtered]
                self.on_change()
            return filtered

    def remove(self, item):
        with self._mutex:
            item = self.pop_filtered(lambda x: x==item)
            if not item:
                raise QueueItemNotFoundException("Item %s not found in queue" % item)

    def on_change(self):
        pass

    def __repr__(self):
        return "%s[%s]" % (self.__class__.__name__, self.name)

class FixedSizeQueue(SimpleQueue):
    """ Simple Thread-safe queue with on_change method to add any workarounds """
    kwargs = ["size"]

    def __init__(self, **kwargs):
        super(FixedSizeQueue, self).__init__(**kwargs)
        self._size = kwargs.get('size', None)
        assert isinstance(self._size, int), "Queue must be limited by 'size'(int) kwarg, got %s<%s>" % (
            self._size, type(self._size)
        )

        self._empty = Condition(self._mutex)
        self._full = Condition(self._mutex)

    def put(self, val):
        with self._full:
            while len(self._queue) >= self._size:
                self._full.wait()
            super(FixedSizeQueue, self).put(val)
            self._empty.notify()

    def put_wait(self, call):
        with self._full:
            while len(self._queue) >= self._size:
                self._full.wait()
            self._queue.append(call())
            self._empty.notify()

    def get(self):
        with self._empty:
            while len(self._queue) == 0:
                self._empty.wait()
            ret = super(FixedSizeQueue, self).get()
            self._full.notify()
            return ret



class NamedAndTypedQueue(FixedSizeQueue, NamedObject):
    """ Queue with name and restricted item type """

    def __init__(self, *args, **kwargs):
        debug("kwargs: %s" % kwargs)
        super(NamedAndTypedQueue, self).__init__(**kwargs)
        self.__allowed_type = kwargs.get("allow", None)
        assert isinstance(self.__allowed_type, type), "Queue expects 'allow' kwarg to be allowed instance class"

    def put(self, _v):
        assert isinstance(_v, self.__allowed_type), "Allowed only %s, got %s" % (self.__allowed_type.__name__, type(_v))
        super(NamedAndTypedQueue, self).put(_v)

