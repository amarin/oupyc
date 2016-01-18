# -*- coding: utf-8 -*-
import threading

__author__ = 'AMarin'


class ThreadSafeSingletonMixin(object):
    """ Threadsafe singleton mixin """
    _singleton_lock = threading.Lock()
    __singleton_instance = None

    @classmethod
    def instance(cls):
        if not cls.__singleton_instance:
            with cls._singleton_lock:
                if not cls.__singleton_instance:
                    cls.__singleton_instance = cls()
        return cls.__singleton_instance