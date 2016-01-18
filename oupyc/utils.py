# -*- coding: utf-8 -*-
__author__ = 'AMarin'


def underscore_to_camelcase(string_value):
    return ''.join(map(lambda x: "%s%s" % (x[0].upper(), x[1:].lower()), string_value.split("_")))

