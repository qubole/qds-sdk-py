import json
import sys

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from qds_sdk.cmd_line import CmdLine
from argparse import ArgumentParser

class NezhaCmdLine(CmdLine):
    """
    qds_sdk.NezhaCmdLine is the interface used by qds.py.
    """

    arg_to_classname = {'cubes' : 'NezhaCube',
                        'measures' : 'NezhaMeasure',
                        'partitions' : 'NezhaPartition',
                        'dimensions' : 'NezhaDimension',
                        'data_sources' : 'NezhaDataSource',
                        'default_data_source' : 'NezhaDefaultDS'}
    classname = ""

    @staticmethod
    def parsers(nezha_entity):
        argparser = ArgumentParser(prog="qds.py nezha %s"%nezha_entity,
                                        description="Nezha client for Qubole Data Service.")
        subparsers = argparser.add_subparsers()

        #Create
        create = subparsers.add_parser("create",
                                       help="Create a new %s"%nezha_entity)
        create.add_argument("--data", dest="data", required=True,
                            help="Path to a file that contains scheduler attributes as a json object")
        create.set_defaults(func=NezhaCmdLine.create)

        #List
        list = subparsers.add_parser("list",
                                     help="List all %s"%nezha_entity)
        list.add_argument("--fields", nargs="*", dest="fields",
                          help="List of fields to show")
        list.add_argument("--per-page", dest="per_page",
                          help="Number of items per page")
        list.add_argument("--page", dest="page",
                          help="Page Number")
        list.set_defaults(func=NezhaCmdLine.list)

        #View
        view = subparsers.add_parser("view",
                                     help="View a specific %s"%nezha_entity)
        view.add_argument("id", help="Numeric id for entity")
        view.add_argument("--fields", nargs="*", dest="fields",
                          help="List of fields to show")
        view.set_defaults(func=NezhaCmdLine.view)

        #update
        update = subparsers.add_parser("update", help="Update a specific Role's name or policy")
        NezhaCmdLine._update_parsers(update, nezha_entity)
        

        #Delete
        delete = subparsers.add_parser("delete", help="Delete a %s"%nezha_entity)
        delete.add_argument("id", help="Numeric id of the nezha_entity to be deleted")
        delete.set_defaults(func=NezhaCmdLine.delete)

        return argparser

    @staticmethod
    def _update_parsers(update, nezha_entity):
        if nezha_entity == "data_sources":
            update.add_argument("id", help="Numeric id of the DataSource")
            update.add_argument("--url", dest="url", help="URL of the data source")
            update.add_argument("--type", dest="type", help="Type of data source")
            update.add_argument("--name", dest="name", help="Name of the data source")

            update.add_argument("--user", dest="user", help="User name of the data source")
            update.add_argument("--password", dest="password", help="Password of the data source")

            update.add_argument("--auth_token", dest="auth_token", help="User name of the data source")
            update.add_argument("--dbtap_id", dest="dbtap_id", help="Password of the data source")

        elif nezha_entity == "cubes":
            update.add_argument("id", help="Numeric id of the Cube")
            update.add_argument("--cost", dest="cost", help="Cost for cubes")
            update.add_argument("--description", dest="description", help="Description")
            update.add_argument("--name", dest="name", help="Name of the Cube")
            update.add_argument("--schema_name", dest="schema_name", help="schema_name")
            update.add_argument("--table_name", dest="table_name", help="Table name")
            update.add_argument("--grouping_column", dest="grouping_column", help="Grouping Column")
            update.add_argument("--destination_id", dest="destination_id", help="Data Source id acting as destiantion")
            update.add_argument("--query", dest="query", help="Query")

        elif nezha_entity == "partitions":
            update.add_argument("id", help="Numeric id of the Partition")
            update.add_argument("--cost", dest="cost", help="Cost for partitions")
            update.add_argument("--description", dest="description", help="Description")
            update.add_argument("--name", dest="name", help="Name of the Partition")
            update.add_argument("--schema_name", dest="schema_name", help="schema_name")
            update.add_argument("--table_name", dest="table_name", help="Table name")
            update.add_argument("--destination_id", dest="destination_id", help="Data Source id acting as destiantion")
            update.add_argument("--query", dest="query", help="Query")

        elif nezha_entity == "default_datasource":
            update.add_argument("--id", help="Numeric id of the Default DataSource")

        update.set_defaults(func=NezhaCmdLine.update, default=None)


    @staticmethod
    def run(args):
        a1 = args.pop(0)
        NezhaCmdLine.classname = NezhaCmdLine.arg_to_classname[a1]
        parser = NezhaCmdLine.parsers(a1)
        parsed = parser.parse_args(args)
        return parsed.func(globals()[NezhaCmdLine.classname], parsed)

    @staticmethod
    def update(cls, args):
        args_dict = {}
        for key, value in args.__dict__.iteritems():
            if key != "func" and key != "id" and value is not None:
                args_dict[key] = value
        return json.dumps(globals()[NezhaCmdLine.classname].update(args.id, **args_dict), sort_keys=True, indent=4)

class NezhaDataSource(Resource):
    """
    qds_sdk.NezhaDataSource setting up Qubole Nezha class.
    """

    """ endpoint for data source is  /nezha_data_sources endpoint"""

    rest_entity_path = "nezha_data_sources"

class NezhaPartition(Resource):
    """
    qds_sdk.NezhaDataSource setting up Qubole Nezha class.
    """

    """ endpoint for data source is  /nezha_data_sources endpoint"""

    rest_entity_path = "nezha_partitions"

class NezhaCube(Resource):
    """
    qds_sdk.NezhaDataSource setting up Qubole Nezha class.
    """

    """ endpoint for data source is  /nezha_data_sources endpoint"""

    rest_entity_path = "nezha_cubes"

class NezhaDimension(Resource):
    """
    qds_sdk.NezhaDataSource setting up Qubole Nezha class.
    """

    """ endpoint for data source is  /nezha_data_sources endpoint"""

    rest_entity_path = ""

class NezhaMeasure(Resource):
    """
    qds_sdk.NezhaDataSource setting up Qubole Nezha class.
    """

    """ endpoint for data source is  /nezha_data_sources endpoint"""

    rest_entity_path = ""


class NezhaDefaultDS(Resource):
    """ 
    Get/Set default-datasource for DSSet
    """

    rest_entity_path = "accounts/default_datasource"

    @classmethod
    def update(cls, **kwargs):
        conn = Qubole.agent()
        return conn.put(cls.rest_entity_path, data=kwargs)

    @classmethod
    def find(cls, **kwargs):
        conn = Qubole.agent()
        return cls(conn.get(cls.rest_entity_path))