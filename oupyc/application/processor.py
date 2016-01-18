# -*- coding: utf-8 -*-
import logging

from oupyc.inthreads.statistics import StatisticsEnabledQueuesProcessorThread
from oupyc.utils import underscore_to_camelcase

__author__ = 'AMarin'

_l = logging.getLogger(__name__)
_l.setLevel(logging.DEBUG)
debug, info, warning, error, critical = _l.debug, _l.info, _l.warning, _l.error,  _l.critical


class ProcessorThread(StatisticsEnabledQueuesProcessorThread):

    def add_queue(self, name, queue):
        assert name == 'incoming', "%s allows only incoming thread" % self.__class__.__name__
        super(ProcessorThread, self).add_queue(name, queue)

    def run(self):
        while not self._exit_event.isSet():
            debug("Waiting for next item")
            item = self.get_queue('incoming').get()
            debug("Got item, processing")
            self.process_item(item)

    def process_item(self, item):
        raise NotImplementedError("%s to define its own process_item" % self.__class__.__name__)

    @classmethod
    def make(cls, func):
        if not hasattr(func, "description"):
            raise NotImplementedError("%s to have description attribute" % func.__name__)
        return type("Processor%s" % underscore_to_camelcase(func.__name__), (cls,), dict(
            description = func.description,
            # generate item from callable
            process_item=lambda self, item: func(item),
        ))()