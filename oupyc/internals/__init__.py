# -*- coding: utf-8 -*-


class NamedObject(object):
    """ Simple object having name """

    def __init__(self, **kwargs):
        super(NamedObject, self).__init__()

        # define internal properties
        self.__name = None

        # init internal properties
        self.name = kwargs.get("name", "%s[%s]" % (self.__class__.__name__, id(self)))

    # name property wrappers
    def get_name(self):
        """ get variable name """
        return self.__name

    def set_name(self, name):
        """ set variable name """
        assert isinstance(name, basestring), "expected string name, got %s" % type(name)
        self.__name = name

    def delete_name(self):
        """ delete variable name. Set default name inplace of existing """
        self.__name = "%s[%s]" % (self.__class__.__name__, id(self))

    name = property(get_name, set_name, None, "Object name")