__author__ = 'avinashj'

"""
The published hivetables module contains the definitions for basic CRUD operations on PublishedHivetable
"""

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser

import logging
import json

log = logging.getLogger("qds_published_hivetable")


class PublishedHivetableCmdLine:
    """
    qds_sdk.PublishedHivetableCmdLine is the interface used a qds.py
    """

    @staticmethod
    def parsers():
        """
        Parse command line arguments to construct a dictionary of hivetables
        parameters that can be used to publish a hivetable in a qbucket.

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used to create a qbucket
        """
        argparser = ArgumentParser(prog="qds.py published_hivetables",
                                        description="Published hivetables client for Qubole Data Service.")
        subparsers = argparser.add_subparsers()

        # Create
        create = subparsers.add_parser("publish",
                                       help="Publish a new hivetable")
        create.add_argument("--qbucket_id", dest="qbucket_id",
                            help="publish the hivetable in the given qbucket id")
        create.add_argument("--table_name", dest="table_name",
                            help="Name of the hivetable to be published")
        create.add_argument("--schema_name", dest="schema_name", default="default",
                            help="Name of the schema")
        create.set_defaults(func=PublishedHivetableCmdLine.create)

        # List
        list = subparsers.add_parser("list",
                                     help="List all published hivetables")
        list.set_defaults(func=PublishedHivetableCmdLine.list)

        # View
        view = subparsers.add_parser("view",
                                     help="View a specific published hivetable")
        view.add_argument("id",
                          help="Numeric id of the Published hivetable")
        view.set_defaults(func=PublishedHivetableCmdLine.view)

        # Edit
        edit = subparsers.add_parser("edit",
                                     help="Edit a specific Published hivetable")
        edit.add_argument("id",
                          help="Numeric id of the Published hivetable")
        edit.add_argument("--table_name", dest="table_name",
                          help="Name of the hivetable to be published")
        edit.add_argument("--schema_name", dest="schema_name", default="default",
                          help="Name of the schema")
        edit.set_defaults(func=PublishedHivetableCmdLine.edit)

        # Delete
        delete = subparsers.add_parser("unpublish",
                                       help="Unpublish a specific Published Hivetable")
        delete.add_argument("id",
                            help="Numeric id of the Published Hivetable")
        delete.set_defaults(func=PublishedHivetableCmdLine.delete)

        return argparser

    @staticmethod
    def run(args):
        parser = PublishedHivetableCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)

    @staticmethod
    def create(args):
        published_hivetable = PublishedHivetable.create(qbucket_id=args.qbucket_id,
                                                        table_name=args.table_name,
                                                        schema_name=args.schema_name)
        return json.dumps(published_hivetable.attributes, sort_keys=True, indent=4)

    @staticmethod
    def list(args):
        published_hivetable_list = PublishedHivetable.list()
        return json.dumps(published_hivetable_list, sort_keys=True, indent=4)

    @staticmethod
    def view(args):
        tap = PublishedHivetable.find(args.id)
        return json.dumps(tap.attributes, sort_keys=True, indent=4)

    @staticmethod
    def edit(args):
        published_hivetable = PublishedHivetable.find(args.id)
        options = {}
        if args.table_name is not None:
            options["table_name"] = args.table_name
        if args.schema_name is not None:
            options["schema_name"]=args.schema_name
        published_hivetable = published_hivetable.edit(**options)
        return json.dumps(published_hivetable.attributes, sort_keys=True, indent=4)

    @staticmethod
    def delete(args):
        published_hivetable = PublishedHivetable.find(args.id)
        return json.dumps(published_hivetable.delete(), sort_keys=True, indent=4)


class PublishedHivetable(Resource):
    """
    qds_sdk.PublishedHivetable is the base Qubole PublishedHivetable class.
    """

    """ all commands use the /qbucket endpoint"""
    rest_entity_path = "published_hivetables"

    @staticmethod
    def list():
        conn = Qubole.agent()
        url_path = PublishedHivetable.rest_entity_path
        return conn.get(url_path)

    def edit(self, **kwargs):
        conn = Qubole.agent()
        return PublishedHivetable(conn.put(self.element_path(self.id), data=kwargs))

    def delete(self):
        conn = Qubole.agent()
        return conn.delete(self.element_path(self.id))