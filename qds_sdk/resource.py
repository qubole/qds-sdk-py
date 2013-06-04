import util
from qubole import Qubole

class ResourceMeta(type):
    """
    A metaclass for Resource objects.
    Defines the path for the entity if one is not defined
    """

    def __new__(mcs, name, bases, new_attrs):
        """Create a new class.

        Args:
            mcs: The metaclass.
            name: The name of the class.
            bases: List of base classes from which mcs inherits.
            new_attrs: The class attribute dictionary.
        """
        if 'rest_entity_path' not in new_attrs:
            new_attrs['rest_entity_path'] = util.pluralize(util.underscore(name))
        return type.__new__(mcs, name, bases, new_attrs)


class Resource(object):

    """ subclasses should uncomment this if it helps"""
    # __metaclass__ = ResourceMeta

    def __init__(self, attributes=None):
        if attributes is None:
            attributes = {}
        self.attributes=attributes

    @classmethod
    def element_path(cls, id):
        return "%s/%s" % (cls.rest_entity_path, str(id))

    @classmethod
    def find(cls, id=None, **kwargs):
        conn=Qubole.agent()
        if id is not None:
            return cls(conn.get(cls.element_path(id)))


    @classmethod
    def create(cls, **kwargs):
        conn=Qubole.agent()
        return cls(conn.post(cls.rest_entity_path, data=kwargs))


    @property
    def my_element_path(self):
        return self.__class__.element_path(self.id)


    def __getattr__(self, name):
        """Retrieve the requested attribute if it exists.

        Args:
            name: The attribute name.
        Returns:
            The attribute's value.
        Raises:
            AttributeError: if no such attribute exists.
        """
        value=self.attributes.get(name)
        if value is not None:
            return value
        raise AttributeError(name)
