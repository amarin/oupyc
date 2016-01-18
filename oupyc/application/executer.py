# -*- coding: utf-8 -*-
import importlib
import logging

from oupyc.checks import require_kwarg_type
from oupyc.application.transformer import TransformerThread
from oupyc.inthreads.statistics import StatisticsEnabledQueuesProcessorThread, NamedQueueWithStatistics
from oupyc.remote.task import TaskPrototype, ResultImplementationError

__author__ = 'AMarin'

_l = logging.getLogger(__name__)
_l.setLevel(logging.DEBUG)
debug, info, warning, error, critical = _l.debug, _l.info, _l.warning, _l.error,  _l.critical


class TaskExecutorThread(TransformerThread):
    """ Takes items from incoming queue, process with transform_item and pass to result queue """

    def __init__(self, *args, **kwargs):
        super(TaskExecutorThread, self).__init__(*args, **kwargs)
        _task_class = kwargs.get("task_class", TaskPrototype)
        assert issubclass(self.task_class, TaskPrototype), '%s requires task_class to be %s subtype, got %s' % (
            self.__class__.__name__,
            TaskPrototype.__name__,
            type(_task_class)
        )
        self.__task_class = _task_class

        self.__max_threads = require_kwarg_type("max_threads", int, **kwargs)
        assert issubclass(self.task_class, TaskPrototype), '%s requires task_class to be %s instance, got %s' % (
            self.task_class,
            TaskPrototype,
            type(self.task_class)
        )
        self.add_queue('incoming', NamedQueueWithStatistics(
            allow=dict,
            size=self.__max_threads,
            name=self.task_class.__name__
        ))

    task_class = property(lambda self: self.__task_class, None, None, "Task class to be processed")

    def transform_item(self, item):
        debug("Process item %s", item)
        task_instance_object = self.task_class.deserialize(item)
        debug("Task instance object created, call process_request")
        result = task_instance_object.process_request()
        debug('request processed, return result')
        return result