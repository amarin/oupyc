# -*- coding: utf-8 -*-
from threading import Thread, RLock, Condition, Event
import logging

_l = logging.getLogger(__name__)
_l.setLevel(logging.WARNING)
debug, info, warning, error, critical = _l.debug, _l.info, _l.warning, _l.error,  _l.critical

__author__ = 'AMarin'










