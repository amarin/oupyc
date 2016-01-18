# -*- coding: utf-8 -*-
import logging

import time

from abc import abstractmethod, ABCMeta

from oupyc.inthreads.statistics import StatisticsEnabledQueuesProcessorThread
from oupyc.utils import underscore_to_camelcase

_l = logging.getLogger(__name__)
_l.setLevel(logging.WARNING)
debug, info, warning, error, critical, log = _l.debug, _l.info, _l.warning, _l.error,  _l.critical, _l.log



class ItemRouter(StatisticsEnabledQueuesProcessorThread):
    """ Simple queue processor. """
    __metaclass__ = ABCMeta
    description = "task router thread"

    def __init__(self, *args, **kwargs):
        super(ItemRouter, self).__init__(*args, **kwargs)
        self.setName("ROUTER")
        self.__destinations = kwargs.get('destinations_threads', dict())
        self.__incoming_key = kwargs.get('key_function', None)

    def add_item_route(self, task_name, queue_name):
        with self._mutex:
            self.__destinations[task_name] = queue_name

    def add_chain(self, task_name, processor_thread, result_queue=None):
        debug("Adding chain to %s.incoming", processor_thread.getName())
        target_queue = processor_thread.get_queue('incoming')
        if result_queue:
            processor_thread.add_queue('result', result_queue)
        debug('Target queue %s', target_queue.getName())
        self.add_queue(task_name, target_queue)
        debug('Adding internal route')
        self.add_item_route(task_name, task_name)
        debug('Chain created')

    def get_item_key(self, item):
        assert callable(self.__incoming_key), "Either set key_function or redefine get_item_key()"
        return self.__incoming_key(item)

    def process_next_item(self):
        # wait for task, put to running registry and start task thread
        with self._mutex:
            info("Waiting for received task")
            item = self.get_queue('incoming').get()
            key = self.get_item_key(item)
            target_queue_name = self.__destinations.get(key, None)
            if target_queue_name:
                info("Incoming [%s] route to destination %s", key, target_queue_name)
                self.get_queue(target_queue_name).put(item)
            else:
                error("No destination for item [%s], return it to queue", key)
                self.get_queue('incoming').put(item)
                time.sleep(1)

    def run(self):
        info("Starting %s", self.__class__.__name__)
        while not self._exit_event.isSet():
            self.process_next_item()
        warning("Got exit signal, finish existed tasks")
        while self.get_queue('incoming').length() > 0:
            self.process_next_item()
        info("Stopping %s", self.__class__.__name__)