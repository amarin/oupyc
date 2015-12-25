# -*- coding: utf-8 -*-
from oupyc.internals import NamedObject


class Variable(NamedObject):
    """ Variable is a NamedObject having value """

    def __init__(self, name=None, value=None):
        super(Variable, self).__init__()

        # init internal properties
        self.__value = None

        # initialize properties
        self.value = value

    # value property wrappers
    def get_value(self):
        """ get variable value """
        return self.__value

    def set_value(self, value):
        """ set variable value """
        self.__value = value

    def delete_value(self):
        """ unset variable value """
        self.__value = None

    value = property(get_value, set_value, delete_value, "Variable value")