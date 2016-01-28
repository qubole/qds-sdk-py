"""
qds_sdk.Resource represents a REST based resource with standard methods like
create/find etc.
"""
import inflection
import json
from six import add_metaclass
from qds_sdk import util
from qds_sdk.qubole import Qubole


class ResourceMeta(type):
    """
    A metaclass for Resource objects.
    Defines the path for the entity if one is not defined
    """

    def __new__(mcs, name, bases, new_attrs):
        """Create a new class.

        Args:
            `mcs`: The metaclass.

            `name`: The name of the class.

            `bases`: List of base classes from which mcs inherits.

            `new_attrs`: The class attribute dictionary.
        """
        if 'rest_entity_path' not in new_attrs:
            new_attrs['rest_entity_path'] = util.pluralize(util.underscore(name))
        return type.__new__(mcs, name, bases, new_attrs)


class ResourceMetaSingleton(type):
    """
    A metaclass for Singleton Resource objects.
    Defines the path for the entity if one is not defined
    """

    def __new__(mcs, name, bases, new_attrs):
        """Create a new class.

        Args:
            `mcs`: The metaclass.

            `name`: The name of the class.

            `bases`: List of base classes from which mcs inherits.

            `new_attrs`: The class attribute dictionary.
        """
        if 'rest_entity_path' not in new_attrs:
            new_attrs['rest_entity_path'] = util.underscore(name)
        return type.__new__(mcs, name, bases, new_attrs)


class BaseResource(object):

    def __init__(self, attributes=None):
        if attributes is None:
            attributes = {}
        self.attributes = attributes

    def __getattr__(self, name):
        """Retrieve the requested attribute if it exists.

        Args:
            `name`: The attribute name.

        Returns:
            The attribute's value.

        Raises:
            AttributeError: if no such attribute exists.
        """
        try:
            return self.attributes[name]
        except KeyError:
            raise AttributeError(name)

    def __str__(self):
        return json.dumps(self.attributes)


class Resource(BaseResource):

    # subclasses should uncomment this if it helps
    # __metaclass__ = ResourceMeta

    @classmethod
    def element_path(cls, id):
        return "%s/%s" % (cls.rest_entity_path, str(id))

    @classmethod
    def find(cls, id, **kwargs):
        conn = Qubole.agent()
        if id is not None:
            return cls(conn.get(cls.element_path(id)))

    @classmethod
    def create(cls, **kwargs):
        conn = Qubole.agent()
        return cls(conn.post(cls.rest_entity_path, data=kwargs))

    @property
    def my_element_path(self):
        return self.__class__.element_path(self.id)

    @classmethod
    def list(cls, page = None, per_page = None):
        conn = Qubole.agent()
        url_path = cls.rest_entity_path
        page_attr = []
        if page is not None:
            page_attr.append("page=%s" % page)
        if per_page is not None:
            page_attr.append("per_page=%s" % per_page)
        if page_attr:
            url_path = "%s?%s" % (cls.rest_entity_path, "&".join(page_attr))

        # To get the required object-list from paginated result,
        # (e.g. {nezha_data_sources:[], page:1, prev_page:nil, next_page:2}),
        # Convert Object classname to plural and add '_' to fetch object-list
        # from result. (NezhaDataSource -> nezha_data_sources)
        resource_json = conn.get(url_path)
        resource_list = []
        for s in resource_json[inflection.pluralize(inflection.underscore(cls.__name__))]:
            resource_list.append(cls(s))
        return resource_list

    @classmethod
    def update(cls, id, **kwargs):
        conn = Qubole.agent()
        return conn.put(cls.element_path(id), data=kwargs)

    @classmethod
    def delete(cls, id):
        conn = Qubole.agent()
        return conn.delete(cls.element_path(id))


@add_metaclass(ResourceMetaSingleton)
class SingletonResource(BaseResource):

    cached_resource = None

    @classmethod
    def find(cls, **kwargs):
        if cls.cached_resource is None:
            conn = Qubole.agent()
            cls.cached_resource = cls(conn.get(cls.rest_entity_path))

        return cls.cached_resource

    @classmethod
    def clear_cache(cls):
        cls.cached_resource = None
