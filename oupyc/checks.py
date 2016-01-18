# -*- coding: utf-8 -*-
__author__ = 'AMarin'


def require_kwarg(name, kwargs):
    assert name in kwargs, "Argument [%s] required in kwarg" % name


def require_kwarg_type(name, required_type, kwargs):
    require_kwarg(name, kwargs)
    item = kwargs.get(name)
    assert isinstance(item, required_type), "Keyword argument [%s] must be %s, got %s" % (
        name, required_type.__name__, type(item)
    )
    return item
