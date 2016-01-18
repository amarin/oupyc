# -*- coding: utf-8 -*-
import logging
from abc import ABCMeta, abstractmethod
from oupyc.checks import require_kwarg_type

__author__ = 'AMarin'

TASK_REGISTRY = dict()
RESULT_SUCCESS=0
RESULT_ERROR=1
RESULT_ERROR_REPEAT=2
RESULT_ERROR_NO_MESSAGE="No messsage set"
RESULT_ERROR_NOT_IMPLEMENTED="Not implemented"
RESULT_ERROR_INCORRECT_IMPLEMENTATION="Incorrect implementation"


# Logging
_l = logging.getLogger(__name__)
_l.setLevel(logging.INFO)
debug, info, warning, error, critical = _l.debug, _l.info, _l.warning, _l.error,  _l.critical


class SerializeWithVersionCheck(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def _serialize(self):
        raise NotImplementedError("%s to have its own serialize() instance method" % self.__class__.__name__)

    @classmethod
    def _deserialize(cls, json_data):
        raise NotImplementedError("%s to have its own _deserialize() class method" % cls.__name__)

    @classmethod
    def _get_version(cls):
        raise NotImplementedError("%s to have its own _get_version() class method" % cls.__name__)

    def serialize(self):
        return dict(
            class_name=self.__class__.__name__,
            class_module=self.__class__.__module__,
            class_version=self.__class__._get_version(),
            data=self._serialize()
        )

    @classmethod
    def deserialize(cls, data):
        # check keys in kwarg
        check_kwargs = ['class_name', 'class_module', 'class_version']
        # translate possibly wrong data into dict
        json_data = dict(**data)
        debug("Deserializing data: %s", json_data)
        # check class_name
        _name = require_kwarg_type('class_name', basestring, json_data)
        assert cls.__name__ == _name, 'Expected %s items class, got %s' % (cls.__name__, _name)
        # check class module
        _module = require_kwarg_type('class_module', basestring, json_data)
        assert cls.__module__ == _module, 'Expected %s.%s item, got %s' % (cls.__module__, cls.__name__, _module)
        # check class version
        _version = require_kwarg_type('class_version', basestring, json_data)
        assert cls._get_version() == _version, 'Expected %s.%s version %s, got %s' % (
            cls.__module__, cls.__name__, cls._get_version(), _version
        )
        # that keys are not needed anymore
        for kwarg in check_kwargs:
            del json_data[kwarg]
        # return self with data populated
        return cls._deserialize(json_data['data'])

    def as_dict(self):
        return self.serialize()


class TaskRequestPrototype(SerializeWithVersionCheck):
    __metaclass__ = ABCMeta

    def __init__(self, **kwargs):
        self.data = kwargs

    def _serialize(self):
        return self.data

    @classmethod
    def _deserialize(cls, json_data):
        return cls(**json_data)

    def get(self, name):
        return self.data.get(name)

    def as_dict(self):
        return self.serialize()


class TaskResultPrototype(SerializeWithVersionCheck):

    def __init__(self, code=RESULT_ERROR, message=RESULT_ERROR_NO_MESSAGE, data=None, request_id=None):
        self.code = code
        self.message = message,
        self.data = data
        self.request_id=request_id

    def _serialize(self):
        return dict(
            request_id=self.request_id,
            code=self.code,
            message=self.message,
            data=self.data,
        )

    @classmethod
    def _deserialize(cls, json_data):
        return cls(**json_data)

    def set_request_id(self, request_id):
        self.request_id = request_id


class ResultError(TaskResultPrototype):
    def __init__(self, message=RESULT_ERROR_NO_MESSAGE, data=None, request_id=None):
        super(ResultError, self).__init__(code=RESULT_ERROR, message=message, data=data, request_id=request_id)


class ResultTaskNotImplementedError(ResultError):

    def __init__(self, message=RESULT_ERROR_NOT_IMPLEMENTED, data=None, request_id=None):
        super(ResultTaskNotImplementedError, self).__init__(message=message, data=None, request_id=request_id)


class ResultImplementationError(ResultError):
    def __init__(self, message=RESULT_ERROR_INCORRECT_IMPLEMENTATION, data=None, request_id=None):
        super(ResultError, self).__init__(code=RESULT_ERROR, message=message, data=data, request_id=request_id)


class TaskNotImplementedError(ResultError):
    pass


class DeserializationException(Exception):
    pass


class TaskPrototype(SerializeWithVersionCheck):
    __metaclass__ = ABCMeta
    response_class = None
    request_class = None
    task_name = None

    def __init__(self, **kwargs):
        assert self.task_name is not None, "%s to have name attribute" % self.__class__.__name__
        assert self.request_class is not None, "%s to have task_class attribute" % self.__class__.__name__
        assert self.response_class is not None, "%s to have result_class attribute" % self.__class__.__name__
        self.request_data = kwargs
        self.request = None

    def make_request(self):
        self.request = self.request_class(**self.request_data)
        return self.serialize()

    def _serialize(self):
        if self.request_data:
            self.request = self.request_class(**self.request_data)
        return dict(
            request=self.request and self.request.serialize() or None,
        )

    @classmethod
    def _deserialize(cls, json_data):
        debug("Deserializing %s with data: %s", cls.__name__, json_data)
        request_serialized = json_data.get('request', None)
        if not request_serialized:
            raise DeserializationException("Cant deserialize %s cause to no request found", cls.__name__)
        # proceed with request
        obj = cls()
        obj.request = cls.request_class.deserialize(request_serialized)
        obj.request_data = obj.request.data
        return obj

    @abstractmethod
    def _process_request(self, **kwargs):
        raise TaskNotImplementedError("%s to define its own process_request")

    def process_request(self, **kwargs):
        return self._process_request(**kwargs)

    def return_success(self, **kwargs):
        kwargs.update(dict(code=RESULT_SUCCESS))
        return self.response_class(**kwargs)

    def return_error(self, message, **kwargs):
        kwargs.update(dict(code=RESULT_ERROR, message=message))
        return self.response_class(**kwargs)

    def get_result(self, json_data):
        return self.response_class(json_data)

def register_task_class(cls):
    TASK_REGISTRY[cls.task_name]=cls