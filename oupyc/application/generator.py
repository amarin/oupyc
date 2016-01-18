# -*- coding: utf-8 -*-
import logging

from oupyc.inthreads.statistics import StatisticsEnabledQueuesProcessorThread
from oupyc.utils import underscore_to_camelcase

_l = logging.getLogger(__name__)
_l.setLevel(logging.DEBUG)
debug, info, warning, error, critical = _l.debug, _l.info, _l.warning, _l.error,  _l.critical


class GeneratorThread(StatisticsEnabledQueuesProcessorThread):
    """ Takes items from incoming queue, process with transform_item and pass to result queue """

    def add_queue(self, name, queue):
        assert name in ['result'], "%s allows only result thread" % self.__class__.__name__
        super(GeneratorThread, self).add_queue(name, queue)

    def run(self):
        while not self._exit_event.isSet():
            self.get_queue('result').put_wait(lambda: self.generate_item())

    def generate_item(self):
        raise NotImplementedError("%s to define its own generate_item" % self.__class__.__name__)

    @classmethod
    def make(cls, func):
        if not hasattr(func, "description"):
            raise NotImplementedError("%s to have description attribute" % func.__name__)
        return type("Generator%s" % underscore_to_camelcase(func.__name__), (cls,), dict(
            description= func.description,
            # generate item from callable
            generate_item=lambda self: func(),
        ))()

