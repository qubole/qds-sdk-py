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
        parameters that can be used to publish a hivetable in a space.

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used to create a space
        """
        argparser = ArgumentParser(prog="qds.py published_hivetables",
                                   description="Published hivetables client for Qubole Data Service.")
        subparsers = argparser.add_subparsers()

        # Publish
        publish = subparsers.add_parser("publish",
                                        help="Publish a new hivetable")
        publish.add_argument("--space_id", dest="space_id",
                             help="publish the hivetable in the given space id")
        publish.add_argument("--table_name", dest="table_name",
                             help="Name of the hivetable to be published")
        publish.add_argument("--schema_name", dest="schema_name", default="default",
                             help="Name of the schema")
        publish.set_defaults(func=PublishedHivetableCmdLine.publish)

        # List
        list = subparsers.add_parser("list",
                                     help="List all published hivetables")
        list.set_defaults(func=PublishedHivetableCmdLine.list)

        # View
        view = subparsers.add_parser("view",
                                     help="View a specific published hivetable")
        view.add_argument("id",
                          help="Numeric id of the Published hivetable")
        view.add_argument("--meta_data", dest="meta_data", default=False,
                          help="Meta data of the published hivetable")
        view.set_defaults(func=PublishedHivetableCmdLine.view)

        # Update
        update = subparsers.add_parser("update",
                                       help="Update a specific Published hivetable")
        update.add_argument("id",
                            help="Numeric id of the Published hivetable")
        update.set_defaults(func=PublishedHivetableCmdLine.update)

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
    def publish(args):
        published_hivetable = PublishedHivetable.create(space_id=args.space_id,
                                                        table_name=args.table_name,
                                                        schema_name=args.schema_name)
        return json.dumps(published_hivetable.attributes, sort_keys=True, indent=4)

    @staticmethod
    def list(args):
        published_hivetable_list = PublishedHivetable.list()
        return json.dumps(published_hivetable_list, sort_keys=True, indent=4)

    @staticmethod
    def view(args):
        published_hivetable = PublishedHivetable.find(args)
        return json.dumps(published_hivetable, sort_keys=True, indent=4)

    @staticmethod
    def update(args):
        options = {}
        published_hivetable = PublishedHivetable.update(args.id, **options)
        return json.dumps(published_hivetable.attributes, sort_keys=True, indent=4)

    @staticmethod
    def delete(args):
        return json.dumps(PublishedHivetable.delete(args.id), sort_keys=True, indent=4)


class PublishedHivetable(Resource):
    """
    qds_sdk.PublishedHivetable is the base Qubole PublishedHivetable class.
    """

    """ all commands use the /space endpoint"""
    rest_entity_path = "published_hivetables"

    @staticmethod
    def find(args):
        conn = Qubole.agent()
        url_path = PublishedHivetable.rest_entity_path + "/" + str(args.id)
        if args.meta_data == 'true' or args.meta_data == 'True':
            url_path += '?meta_data=true'
        return conn.get(url_path)

    @staticmethod
    def list():
        conn = Qubole.agent()
        url_path = PublishedHivetable.rest_entity_path
        return conn.get(url_path)

    @staticmethod
    def update(id, **kwargs):
        conn = Qubole.agent()
        url_path = PublishedHivetable.rest_entity_path + "/" + str(id)
        return PublishedHivetable(conn.put(url_path, data=kwargs))

    @staticmethod
    def delete(id):
        conn = Qubole.agent()
        url_path = PublishedHivetable.rest_entity_path + "/" + str(id)
        return conn.delete(url_path)