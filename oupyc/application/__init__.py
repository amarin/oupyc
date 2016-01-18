# -*- coding: utf-8 -*-
from threading import RLock

from oupyc.application.condition import ConditionGenerator
from oupyc.application.generator import GeneratorThread
from oupyc.application.processor import ProcessorThread
from oupyc.application.router import RouterThread
from oupyc.application.transformer import TransformerThread
from oupyc.queues import FixedSizeQueue

__author__ = 'AMarin'

# -*- coding: utf-8 -*-
import threading
import time
import logging

from abc import ABCMeta, abstractmethod
from oupyc.checks import require_kwarg_type

_l = logging.getLogger(__name__)
_l.setLevel(logging.INFO)
debug, info, warning, error, critical = _l.debug, _l.info, _l.warning, _l.error,  _l.critical

GENERATOR="generator"
ROUTER="router",
TRANSFORMER="transformer"
PROCESSOR="processor"
CONDITION="condition"

KNOWN_THREADS = {
    GENERATOR: GeneratorThread,
    ROUTER: RouterThread,
    TRANSFORMER: TransformerThread,
    PROCESSOR: ProcessorThread,
    CONDITION: ConditionGenerator
}

class ThreadSearchError(Exception):
    pass


'''
        self._prepare_threads()

    def _prepare_threads(self):
        if hasattr(self, "_thread_classes") and len(getattr(self, "_thread_classes")):
            info("Initiating %s threads", len(self._thread_classes))
            for thread_class in self._thread_classes:
                debug("Make %s thread", thread_class.description)
                thread_instance = thread_class(exit_event=self._exit_event)
                self._threads.append(thread_instance)
'''

class ThreadedApplication(object):

    def __init__(self, threads, *args, **kwargs):
        super(ThreadedApplication, self).__init__()
        self._threads = []
        self._mutex = RLock()
        with self._mutex:
            for th in threads:
                self._threads.append(th)
        self._exit_event = threading.Event()

    def add_thread(self, th):
        info("Adding thread %s" % th)
        th._exit_event = self._exit_event
        self._threads.append(th)

    def add_threads(self, *threads):
        map(lambda th: self.add_thread(th), threads)

    def make_thread(self, thread_type, func):
        if thread_type in KNOWN_THREADS:
            thread_class = KNOWN_THREADS[thread_type]
            return thread_class.make(func)
        else:
            raise ThreadSearchError("Unknown thread type %s, choose one of %s" % (thread_type, KNOWN_THREADS))

    def make_gtp_chain(self, *callables):
        current_queue = None
        max_index = len(callables)-1
        with self._mutex:
            for func in callables:
                idx = callables.index(func)
                item = None
                if 0 == idx:
                    # first thread is item generator
                    item = self.make_thread(GENERATOR, callables[idx])
                    item.add_queue("result", FixedSizeQueue(size=1))

                elif idx < max_index:
                    # internal threads
                    item = self.make_thread(TRANSFORMER, callables[idx])
                    item.set_input(self._threads[-1])
                    item.add_queue("result", FixedSizeQueue(size=1))

                elif idx == max_index:
                    # last thread is item processor
                    item = self.make_thread(PROCESSOR, callables[idx])
                    item.set_input(self._threads[-1])

                self.add_thread(item)

    def main(self):
        """ Main execution process """
        info("Starting %s threads", len(self._threads))

        for th in self._threads:
            if hasattr(th, "get_all_queues"):
                for queue in th.get_all_queues():
                    print("%s %s" % (th.getName(), queue))

        for th in self._threads:
            debug("%s starting" % th.description)
            th.start()
            info("%s started", th.description)
        info("All internal threads started")
        while not self._exit_event.isSet():
            pass

    def exit_gracefully(self):
        """ Gracefull stop """
        info("Stopping all threads")
        for th in self._threads[::-1]:
            info("Waiting %s", th.description)
            debug("thread %s[%s]", type(th), th)
            th.join(10)

    def run(self):
        info("Run %s", self.__class__.__name__)
        try:
            self.main()
        except KeyboardInterrupt:
            warning("Processing CTRL-C")
            self._exit_event.set()
        except Exception as exc:
            critical(exc)
        finally:
            info("Exit gracefully")
            self.exit_gracefully()
        for th in threading._enumerate():
            error("Thread %s still active", th)
            if not 'MainThread'==th.getName():
                th.join(1)
        info("Done")

    def get_threads_by_class(self, thread_cls):
        return filter(lambda x: isinstance(x, thread_cls), self._threads)

    def get_threads_by_name(self, thread_name):
        return filter(lambda x: thread_name==x.getName(), self._threads)

    def get_threads(self, cls_or_name):
        """ Get threads filtered by name or class """
        return isinstance(cls_or_name, type) \
                   and self.get_threads_by_class(cls_or_name) \
                   or self.get_threads_by_name(cls_or_name)

    def get_thread(self, cls_or_name):
        """ Get single thread with requested name or class. Raises ThreadSearchError if found multiple or nothing"""
        _filtered = self.get_threads(cls_or_name)
        if 1>len(_filtered):
            raise ThreadSearchError("No threads found with class or name %s" % (cls_or_name))
        elif 1<len(_filtered):
            raise ThreadSearchError("Multiple threads found with class or name %s." % (cls_or_name))
        else:
            return _filtered[0]




class ApplicationWithStatistics(ThreadedApplication):
    __metaclass__ = ABCMeta

    def __init__(self, threads, *args, **kwargs):
        super(ApplicationWithStatistics, self).__init__(threads)

        # init statistics subsystem
        stat_queue_size = require_kwarg_type('statistics_queue_size', int, kwargs)
        import oupyc.inthreads.statistics

        stat_thread, stat_aggregate_thread = oupyc.inthreads.statistics.start(
            self._exit_event,
            stat_queue_size,
            stat_queue_size,
            lambda x: self.process_stat_record(x)
        )
        self._threads.insert(0, stat_thread)
        self._threads.insert(0, stat_aggregate_thread)


    @abstractmethod
    def process_stat_record(self, record):
        pass

