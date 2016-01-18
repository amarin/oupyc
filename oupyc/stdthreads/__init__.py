# -*- coding: utf-8 -*-
from threading import Thread, RLock, Event

__author__ = 'AMarin'


class QueueOverwritingException(Exception):
    pass


class QueueNotFoundException(Exception):
    pass


class ExitEventAwareThread(Thread):
    """ Simple thread having event to exit """

    def __init__(self, *args, **kwargs):
        super(ExitEventAwareThread, self).__init__()

        # define internal properties
        self._mutex = RLock()
        self._exit_event = None

        # do additional initialization
        self.exit_event = kwargs.get('exit_event')
        self.setName("%s[%s]" % (self.__class__.__name__, id(self)))

    def set_exit_event(self, event):
        """ Define exit event """
        print("Exit event %s" % event)
        #assert isinstance(event, Event), "exit_event to be %s instance, got %s" % (Event(), type(event))
        with self._mutex:
            self._exit_event=event

    def get_exit_event(self):
        """ Return defined exit event """
        return self._exit_event

    exit_event = property(get_exit_event, set_exit_event, None, "Exit event to check")


class QueueProcessorThread(ExitEventAwareThread):
    """ Simple thread to process queue(s) """

    def __init__(self, *args, **kwargs):
        super(QueueProcessorThread, self).__init__(*args, **kwargs)
        self.__queues = dict()

    def add_queue(self, name, queue):
        """ Register named queue """
        with self._mutex:
            if not name in self.__queues.keys():
                self.__queues[name] = queue
            else:
                raise QueueOverwritingException("Thread %s already has queue with name %s" % (self.getName(), name))

    def get_queue(self, name):
        """ Return named queue if registered """
        if name not in self.__queues.keys():
            raise QueueNotFoundException("No such queue %s in %s" % (name, self.getName()))
        return self.__queues[name]

    def get_all_queues(self):
        return self.__queues

    def set_input(self, thread, queue_from='result', queue_to='incoming'):
        self.add_queue(queue_to, thread.get_queue(queue_from))

    def set_output(self, thread, queue_to="incoming", queue_from="result"):
        self.add_queue(queue_from, thread.get_queue(queue_to))