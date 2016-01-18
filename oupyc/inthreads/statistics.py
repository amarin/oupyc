# -*- coding: utf-8 -*-
import logging
from datetime import datetime, timedelta
from time import sleep
from oupyc.queues import NamedAndTypedQueue
from oupyc.stdthreads import ExitEventAwareThread, QueueProcessorThread

__author__ = 'AMarin'

_l = logging.getLogger(__name__)
_l.setLevel(logging.INFO)
debug, info, warning, error, critical = _l.debug, _l.info, _l.warning, _l.error,  _l.critical

# Internal queues for stat records
_IN_QUEUE = None
_OUT_QUEUE_MINUTES = None
def _assert_is_started():
    global _IN_QUEUE, _OUT_QUEUE_MINUTES
    assert _IN_QUEUE is not None, "You must call %s.start() first" % __name__
    assert _OUT_QUEUE_MINUTES is not None, "You must call %s.start() first" % __name__


class StatRecord(object):

    def __init__(self, counter, value, dt=None):
        self.counter = counter
        self.value = value
        self.time = dt is not None and dt or datetime.now()

    def __repr__(self):
        return '%s %s %s' % (self.time, self.counter, self.value)


class _StatRecordQueue(NamedAndTypedQueue):

    def __init__(self, *args, **kwargs):
        debug("Init %s: kwargs %s" % (self.__class__.__name__, kwargs))
        kwargs['allow'] = StatRecord
        super(_StatRecordQueue, self).__init__(*args, **kwargs)

    def put_record(self, name, val, dt=None):
        self.put(StatRecord(name, val, dt))

    def put_event(self, name):
        self.put(StatRecord(name, 0))


class StatRecordQueue(_StatRecordQueue):

    def __init__(self, *args, **kwargs):
        kwargs['name'] = 'internals.stat.in.queue'
        super(StatRecordQueue, self).__init__(*args, **kwargs)


class StatAgregatorQueue(_StatRecordQueue):

    def __init__(self, *args, **kwargs):
        kwargs['name'] = 'internals.stat.out.queue'
        super(StatAgregatorQueue, self).__init__(*args, **kwargs)


class NamedQueue(NamedAndTypedQueue):
    """ NamedQueue with accounting on demand """

    def __init__(self, *args, **kwargs):
        assert 'name' in kwargs, "%s required name kwarg"
        self.__name = kwargs.pop('name', '%s[%s]' % (self.__class__.__name__, id(self)))
        super(NamedQueue, self).__init__(*args, **kwargs)

    def _account(self):
        _assert_is_started()
        global _IN_QUEUE
        _IN_QUEUE.put_record("%s.length" % self.__name, self.length())

    def getName(self):
        return self.__name

    def setName(self, name):
        with self._mutex:
            self.__name=name


class NamedQueueWithStatistics(NamedQueue):
    """ Queue counting self length on every change """

    def __init__(self, *args, **kwargs):
        super(NamedQueueWithStatistics, self).__init__(*args, **kwargs)

    def on_change(self):
        global _IN_QUEUE
        _assert_is_started()
        self._account()


class StatisticsProcessorThread(ExitEventAwareThread):
    description = 'statistics processor'

    def __init__(self, *args, **kwargs):
        global _IN_QUEUE, _OUT_QUEUE_MINUTES
        _assert_is_started()
        super(StatisticsProcessorThread, self).__init__(*args, **kwargs)
        self.__previous_time = self.minute_start()

    def _aggregate_stat(self):
        _assert_is_started()
        global _IN_QUEUE, _OUT_QUEUE_MINUTES
        current_time = datetime.now()
        prev_start = self.minute_start() - timedelta(minutes=1)
        prev_end = prev_start + timedelta(seconds=59, microseconds=999999)
        debug("%s aggregate slice %s-%s", current_time, prev_start, prev_end)
        records = _IN_QUEUE.pop_filtered(lambda x: prev_start <= x.time <= prev_end)
        debug("Got %s records to aggregate, %s rest in queue", len(records), len(_IN_QUEUE))

        # process unique metrica names
        names = list(set(map(lambda x: x.counter, records)))
        for name in names:
            # debug("Process %s", name)
            values = map(lambda x: x.value, filter(lambda x: x.counter == name, records))
            if '.event' == name[-6:]:
                # just counting events
                _OUT_QUEUE_MINUTES.put_record('%s.count' % name, len(values), prev_start)
            else:
                _OUT_QUEUE_MINUTES.put_record('%s.max' % name, max(values), prev_start)
                _OUT_QUEUE_MINUTES.put_record('%s.min' % name, min(values), prev_start)
                # sum with 0.0 to make float result
                _OUT_QUEUE_MINUTES.put_record('%s.avg' % name, sum(values, 0.0) / len(values), prev_start)
                _OUT_QUEUE_MINUTES.put_record('%s.last' % name, values[-1], prev_start)

        # push some handy stat values
        _OUT_QUEUE_MINUTES.put_record(
            'stat.aggregate.duration.ms',
            (datetime.now()-current_time).microseconds/1000.0,
            prev_start
        )

    def minute_start(self):
        current_time = datetime.now()
        return current_time - timedelta(seconds=current_time.second, microseconds=current_time.microsecond)

    def seconds_start(self):
        current_time = datetime.now()
        return current_time - timedelta(microseconds=current_time.microsecond)

    def run(self):
        _assert_is_started()
        while not self._exit_event.isSet():
            self.put_event('stat.threads.switch.event')
            debug("Processing stat queue")
            debug(self.seconds_start())
            if self.__previous_time < self.minute_start():
                self._aggregate_stat()
                self.__previous_time = self.minute_start()
            sleep(1)

    def put_record(self, name, value):
        _assert_is_started()
        global _IN_QUEUE
        debug("+STAT %s %s", name, value)
        _IN_QUEUE.put_record(name, value)

    def put_event(self, name):
        _assert_is_started()
        global _IN_QUEUE
        debug("+EVENT %s", name)
        _IN_QUEUE.put_record('%s.event' % name, 0)


class StatisticsSaverThread(ExitEventAwareThread):
    description = 'statistics saver'

    def __init__(self, process_record_function, *args, **kwargs):
        super(StatisticsSaverThread, self).__init__(*args, **kwargs)
        assert callable(process_record_function), "argument to be callable"
        self.__process_record = process_record_function

    def run(self):
        global _OUT_QUEUE_MINUTES
        _assert_is_started()
        while not self._exit_event.isSet():
            self.__process_record(_OUT_QUEUE_MINUTES.get())


class StatisticsEnabledThread(ExitEventAwareThread):
    """ Thread having internal methods to put stat metrics values """

    def __init__(self, *args, **kwargs):
        super(StatisticsEnabledThread, self).__init__(*args, **kwargs)
        assert hasattr(self.__class__, 'description'), "%s must have description attribute" % self.__class__.__name__

    def put_record(self, name, value):
        """ Put metrica specifyed by name and value """
        global _IN_QUEUE
        _assert_is_started()
        _IN_QUEUE.put_record(name, value)

    def put_event(self, name):
        """ Put event specifyed by name """
        global _IN_QUEUE
        _assert_is_started()
        _IN_QUEUE.put_event(name)


class StatisticsEnabledQueuesProcessorThread(QueueProcessorThread, StatisticsEnabledThread):

    def __init__(self, *args, **kwargs):
        super(StatisticsEnabledQueuesProcessorThread, self).__init__(*args, **kwargs)


def start(exit_event, in_queue_length, out_queue_length, save_func):
    debug("Start statistics events with queues %s/%s" % (in_queue_length, out_queue_length))
    global _IN_QUEUE, _OUT_QUEUE_MINUTES
    if not _IN_QUEUE:
        _IN_QUEUE = StatRecordQueue(size=in_queue_length)
    if not _OUT_QUEUE_MINUTES:
        _OUT_QUEUE_MINUTES = StatAgregatorQueue(size=out_queue_length)
    th_stat = StatisticsProcessorThread(exit_event=exit_event)
    th_processor_class = StatisticsSaverThread(save_func, exit_event=exit_event)
    return th_processor_class, th_stat