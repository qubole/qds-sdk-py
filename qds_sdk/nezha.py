import json

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from qds_sdk.cmd_line import CmdLine
from argparse import ArgumentParser

class NezhaCmdLine(CmdLine):
    """
    qds_sdk.ScheduleCmdLine is the interface used by qds.py.
    """

    @staticmethod
    def parsers():
        argparser = ArgumentParser(prog="qds.py nezha_data_sources",
                                        description="Scheduler client for Qubole Data Service.")
        subparsers = argparser.add_subparsers()

        #Create
        create = subparsers.add_parser("create",
                                       help="Create a new schedule")
        create.add_argument("--data", dest="data", required=True,
                            help="Path to a file that contains scheduler attributes as a json object")
        create.set_defaults(func=NezhaCmdLine.create)

        #List
        list = subparsers.add_parser("list",
                                     help="List all schedulers")
        list.add_argument("--fields", nargs="*", dest="fields",
                          help="List of fields to show")
        list.add_argument("--per-page", dest="per_page",
                          help="Number of items per page")
        list.add_argument("--page", dest="page",
                          help="Page Number")
        list.set_defaults(func=NezhaCmdLine.list)

        #View
        view = subparsers.add_parser("view",
                                     help="View a specific schedule")
        view.add_argument("id", help="Numeric id of the schedule")
        view.add_argument("--fields", nargs="*", dest="fields",
                          help="List of fields to show")
        view.set_defaults(func=NezhaCmdLine.view)

        #update
        update = subparsers.add_parser("update", help="Update a specific Role's name or policy")
        update.add_argument("id", help="Numeric id of the Role")
        update.add_argument("--url", dest="url", help="URL of the data source")
        update.add_argument("--type", dest="type", help="Type of data source")
        update.add_argument("--user", dest="user", help="User name of the data source")
        update.add_argument("--password", dest="password", help="Password of the data source")
        update.add_argument("--name", dest="name", help="Name of the data source")
        update.add_argument("--default", dest="default", action="store_true", help="Is data source default ?")
        update.add_argument("--not-default", dest="default", action="store_false", help="Is data source default ?")
        update.set_defaults(func=NezhaCmdLine.update, default=None)

        #Delete
        delete = subparsers.add_parser("delete", help="Delete a Role")
        delete.add_argument("id", help="Numeric id of the Role to be deleted")
        delete.set_defaults(func=NezhaCmdLine.delete)

        return argparser

    @staticmethod
    def run(args):
        parser = NezhaCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(NezhaDataSource, parsed)

    @staticmethod
    def update(cls, args):
        args_dict = {}
        for key, value in args.__dict__.iteritems():
            if key != "func" and key != "id" and value is not None:
                args_dict[key] = value
        return json.dumps(NezhaDataSource.update(args.id, **args_dict), sort_keys=True, indent=4)

class NezhaDataSource(Resource):
    """
    qds_sdk.NezhaDataSource setting up Qubole Nezha class.
    """

    """ all commands use the /nezha_data_sources endpoint"""

    rest_entity_path = "nezha_data_sources"
