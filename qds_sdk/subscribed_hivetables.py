__author__ = 'avinashj'


"""
The subscribed hivetables module contains the definitions for basic CRUD operations on SubscribedHivetable
"""

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser

import logging
import json

log = logging.getLogger("qds_subscribed_hivetable")


class SubscribedHivetableCmdLine:
    """
    qds_sdk.SubscribedHivetableCmdLine is the interface used a qds.py
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
        argparser = ArgumentParser(prog="qds.py subscribed_hivetables",
                                        description="Subscribed hivetables client for Qubole Data Service.")
        subparsers = argparser.add_subparsers()

        # Create
        create = subparsers.add_parser("subscribe",
                                       help="Subscribe a new hivetable")
        create.add_argument("--published_hivetable_id", dest="published_hivetable_id",
                            help="Numeric id of the hivetable to subscribe")
        create.add_argument("--schema_name", dest="schema_name", default="default",
                            help="Name of the schema")
        create.set_defaults(func=SubscribedHivetableCmdLine.create)

        # List
        list = subparsers.add_parser("list",
                                     help="List all subscribed hivetables")
        list.set_defaults(func=SubscribedHivetableCmdLine.list)

        # View
        view = subparsers.add_parser("view",
                                     help="View a specific subscribed hivetable")
        view.add_argument("id",
                          help="Numeric id of the Subscribed hivetable")
        view.set_defaults(func=SubscribedHivetableCmdLine.view)

        # Available for subscription
        available = subparsers.add_parser("available_subscription",
                                          help="Get all available hivetables for subscribe")
        available.set_defaults(func=SubscribedHivetableCmdLine.available)

        # Edit
        edit = subparsers.add_parser("edit",
                                     help="Edit a specific Subscribed hivetable")
        edit.add_argument("id",
                          help="Numeric id of the Subscribed hivetable")
        edit.set_defaults(func=SubscribedHivetableCmdLine.edit)

        # Delete
        delete = subparsers.add_parser("unsubscribe",
                                       help="Delete a specific Qbucket Subscriber")
        delete.add_argument("id",
                            help="Numeric id of the Qbucket Subscriber")
        delete.set_defaults(func=SubscribedHivetableCmdLine.delete)

        return argparser

    @staticmethod
    def run(args):
        parser = SubscribedHivetableCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)

    @staticmethod
    def create(args):
        subscribed_hivetable = SubscribedHivetable.create(published_hivetable_id=args.published_hivetable_id,
                                                          schema_name=args.schema_name)
        return json.dumps(subscribed_hivetable.attributes, sort_keys=True, indent=4)

    @staticmethod
    def list(args):
        subscribed_hivetable_list = SubscribedHivetable.list()
        return json.dumps(subscribed_hivetable_list, sort_keys=True, indent=4)

    @staticmethod
    def view(args):
        subscribed_hivetable = SubscribedHivetable.find(args.id)
        return json.dumps(subscribed_hivetable.attributes, sort_keys=True, indent=4)

    @staticmethod
    def available(args):
        subscribed_hivetable = SubscribedHivetable.available()
        return json.dumps(subscribed_hivetable, sort_keys=True, indent=4)

    @staticmethod
    def edit(args):
        subscribed_hivetable = SubscribedHivetable.find(args.id)
        options = {}
        subscribed_hivetable = subscribed_hivetable.edit(**options)
        return json.dumps(subscribed_hivetable.attributes, sort_keys=True, indent=4)

    @staticmethod
    def delete(args):
        subscribed_hivetable = SubscribedHivetable.find(args.id)
        return json.dumps(subscribed_hivetable.delete(), sort_keys=True, indent=4)


class SubscribedHivetable(Resource):
    """
    qds_sdk.SubscribedHivetables is the base Qubole SubscribedHivetables class.
    """

    """ all commands use the /qbucket endpoint"""
    rest_entity_path = "subscribed_hivetables"

    @staticmethod
    def list():
        conn = Qubole.agent()
        url_path = SubscribedHivetable.rest_entity_path
        return conn.get(url_path)

    @staticmethod
    def available():
        conn = Qubole.agent()
        url_path = SubscribedHivetable.rest_entity_path + "/available"
        return conn.get(url_path)

    def edit(self, **kwargs):
        conn = Qubole.agent()
        return SubscribedHivetable(conn.put(self.element_path(self.id), data=kwargs))

    def delete(self):
        conn = Qubole.agent()
        return conn.delete(self.element_path(self.id))