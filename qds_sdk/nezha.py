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

    arg_to_classname = {'cubes': 'NezhaCube',
                        'measures': 'NezhaMeasure',
                        'partitions': 'NezhaPartition',
                        'dimensions': 'NezhaDimension',
                        'data_sources': 'NezhaDataSource',
                        'default_datasource': 'NezhaDefaultDS'}
    classname = ""

    @staticmethod
    def parsers():
        argparser = ArgumentParser(
            prog="qds.py nezha",
            description="Nezha client for Qubole Data Service.")
        subparsers = argparser.add_subparsers()

        # Create Nezha-Entities
        create = subparsers.add_parser(
            "create", help="Create a new cube/partition/data_source")
        create.add_argument(
            "nezha_entity", choices=['cubes', 'partitions', 'data_sources'],
            help="Name of nezha-entity")
        create.add_argument(
            "--data", dest="data", required=True,
            help="Attributes of nezha-entity as a json object")
        create.set_defaults(func=NezhaCmdLine.create)

        # List Nezha-Entities
        list = subparsers.add_parser(
            "list",
            help="List cubes/partitions/data_sources/default-datasource")
        list.add_argument(
            "nezha_entity",
            choices=[
                'cubes', 'partitions', 'data_sources', 'default_datasource'],
            help="Name of nezha-entity")
        list.add_argument(
            "--fields", nargs="*", dest="fields",
            help="List of fields to show for nezha-entity")
        list.add_argument(
            "--per-page", dest="per_page",
            help="Number of items per page")
        list.add_argument(
            "--page", dest="page",
            help="Page Number")
        list.set_defaults(func=NezhaCmdLine.list)

        # View Nezha-Entites
        view = subparsers.add_parser(
            "view", help="View a specific cube/partition/data_source")
        view.add_argument(
            "nezha_entity", choices=['cubes', 'partitions', 'data_sources'],
            help="Name of nezha-entity")
        view.add_argument(
            "id", help="Numeric id for entity")
        view.add_argument(
            "--fields", nargs="*", dest="fields",
            help="List of fields to show for nezha-entity")
        view.set_defaults(func=NezhaCmdLine.view)

        # Update Nezha-Entites
        update = subparsers.add_parser(
            "update",
            help="Update cube/partition/data_source/default_datasource")
        NezhaCmdLine._update_parsers(update)

        # Delete Nezha-Entites
        delete = subparsers.add_parser(
            "delete", help="Delete a cube/partition/data_source")
        delete.add_argument(
            "nezha_entity", choices=['cubes', 'partitions', 'data_sources'],
            help="Name of nezha-entity")
        delete.add_argument(
            "id", help="Numeric id of the nezha-entity to be deleted")
        delete.set_defaults(func=NezhaCmdLine.delete)

        return argparser

    @staticmethod
    def _update_parsers(update):
        update_subparsers = update.add_subparsers(
            help="update cubes/partitions/data_sources/default_datasource",
            dest="nezha_entity")

        parser_datasource = update_subparsers.add_parser(
            "data_sources", help="Update data-sources")
        parser_datasource.add_argument(
            "id", help="Numeric id of the DataSource")
        parser_datasource.add_argument(
            "--url", dest="url", help="URL of the data source")
        parser_datasource.add_argument(
            "--type", dest="type", help="Type of data source")
        parser_datasource.add_argument(
            "--name", dest="name", help="Name of the data source")
        parser_datasource.add_argument(
            "--user", dest="user", help="Username for data source")
        parser_datasource.add_argument(
            "--password", dest="password", help="Password for data source")
        parser_datasource.add_argument(
            "--auth_token", dest="auth_token",
            help="Authentication token for qds")
        parser_datasource.add_argument(
            "--dbtap_id", dest="dbtap_id", help="Id of the data-source in qds")

        parser_cube = update_subparsers.add_parser(
            "cubes", help="update cubes")
        parser_cube.add_argument(
            "id", help="Numeric id of the Cube")
        parser_cube.add_argument(
            "--cost", dest="cost", help="Cost for cube")
        parser_cube.add_argument(
            "--description", dest="description",
            help="Description of the cube")
        parser_cube.add_argument(
            "--name", dest="name", help="Name of the Cube")
        parser_cube.add_argument(
            "--schema_name", dest="schema_name", help="Schema name")
        parser_cube.add_argument(
            "--table_name", dest="table_name", help="Table name")
        parser_cube.add_argument(
            "--grouping_column", dest="grouping_column",
            help="Grouping Column")
        parser_cube.add_argument(
            "--destination_id", dest="destination_id",
            help="Data Source id acting as destiantion")
        parser_cube.add_argument("--query", dest="query", help="Query")

        parser_partition = update_subparsers.add_parser(
            "partitions", help="update partitions")
        parser_partition.add_argument(
            "id", help="Numeric id of the Partition")
        parser_partition.add_argument(
            "--cost", dest="cost", help="Cost for partition")
        parser_partition.add_argument(
            "--description", dest="description",
            help="Description of the partition")
        parser_partition.add_argument(
            "--name", dest="name", help="Name of the Partition")
        parser_partition.add_argument(
            "--schema_name", dest="schema_name", help="Schema name")
        parser_partition.add_argument(
            "--table_name", dest="table_name", help="Table name")
        parser_partition.add_argument(
            "--destination_id", dest="destination_id", help="Data Source id")
        parser_partition.add_argument("--query", dest="query", help="Query")

        parser_ds = update_subparsers.add_parser(
            "default_datasource", help="update default-datasource")
        parser_ds.add_argument(
            "default_datasource_id", help="DataSource id which is to be made default-datasource")

        update.set_defaults(func=NezhaCmdLine.update, default=None)

    @staticmethod
    def run(args):
        parser = NezhaCmdLine.parsers()
        parsed = parser.parse_args(args)
        NezhaCmdLine.classname = NezhaCmdLine.arg_to_classname[
            parsed.nezha_entity]
        return parsed.func(globals()[NezhaCmdLine.classname], parsed)

    @staticmethod
    def update(cls, args):
        args_dict = {}
        for key, value in args.__dict__.items():
            params = (key != "func" and key != "id" and key != "nezha_entity")
            if params and value is not None:
                args_dict[key] = value

        if args.nezha_entity == "default_datasource":
            return json.dumps(
                globals()[NezhaCmdLine.classname].update(**args_dict),
                sort_keys=True, indent=4)

        return json.dumps(
            globals()[NezhaCmdLine.classname].update(args.id, **args_dict),
            sort_keys=True, indent=4)


class NezhaDataSource(Resource):
    """
    qds_sdk.NezhaDataSource: is the class for retrieving and
    manipulating Qubole's NezhaDataSource.

    Endpoint for data source is: /nezha_data_sources.
    """

    rest_entity_path = "nezha_data_sources"


class NezhaPartition(Resource):
    """
    qds_sdk.NezhaPartition: is the class for retrieving and
    manipulating Qubole's NezhaPartition.

    Endpoint for partitions is: /nezha_partitions.
    """

    rest_entity_path = "nezha_partitions"


class NezhaCube(Resource):
    """
    qds_sdk.NezhaCube: is the class for retrieving and
    manipulating Qubole's NezhaCube.

    Endpoint for cubes is: /nezha_cubes.
    """

    rest_entity_path = "nezha_cubes"


class NezhaDimension(Resource):
    """
    qds_sdk.NezhaDimension: is the class for retrieving and
    manipulating Qubole's NezhaDimension.

    Endpoint for dimensions is: /nezha_cubes/{nezha_cube_id}/nezha_dimensions.
    User needs to set it before using.
    """

    rest_entity_path = ""


class NezhaMeasure(Resource):
    """
    qds_sdk.NezhaMeasure: is the class for retrieving and
    manipulating Qubole's NezhaMeasure.

    Endpoint for dimensions is: /nezha_cubes/{nezha_cube_id}/nezha_dimensions.
    User needs to set it before using.
    """

    rest_entity_path = ""


class NezhaDefaultDS(Resource):
    """
    qds_sdk.NezhaDefaultDS: is the class to update/get
    default datasource for NezhaDSSet class.
    """

    rest_entity_path = "accounts/default_datasource"

    @classmethod
    def update(cls, **kwargs):
        conn = Qubole.agent()
        return conn.put(cls.rest_entity_path, data=kwargs)
