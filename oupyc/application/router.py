# -*- coding: utf-8 -*-
import logging

from oupyc.inthreads.singleton import ThreadSafeSingletonMixin
from oupyc.inthreads.statistics import StatisticsEnabledQueuesProcessorThread
from oupyc.utils import underscore_to_camelcase

_l = logging.getLogger(__name__)
_l.setLevel(logging.WARNING)
debug, info, warning, error, critical, log = _l.debug, _l.info, _l.warning, _l.error,  _l.critical, _l.log


class RouterThread(StatisticsEnabledQueuesProcessorThread):

    def run(self):
        while not self._exit_event.isSet():
            debug("Waiting for next item")
            item = self.get_queue('incoming').get()
            debug("Got item, processing")
            self.get_queue(self.route_item(item)).put(item)

    def route_item(self, item):
        raise NotImplementedError("%s to define its own process_item" % self.__class__.__name__)

    @classmethod
    def make(cls, func):
        if not hasattr(func, "description"):
            raise NotImplementedError("%s to have description attribute" % func.__name__)
        return type("Router%s" % underscore_to_camelcase(func.__name__), (cls,), dict(
            description = func.description,
            # generate item from callable
            route_item=lambda self, item: func(item),
        ))()



