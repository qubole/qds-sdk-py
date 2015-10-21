__author__ = 'avinashj'

"""
The space subscribers module contains the definitions for basic CRUD operations on SpaceSubscriber
"""

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser

import logging
import json

log = logging.getLogger("qds_space_subscriber")


class SpaceSubscriberCmdLine:
    """
    qds_sdk.SpaceSubscriberCmdLine is the interface used a qds.py
    """

    @staticmethod
    def parsers():
        """
        Parse command line arguments to construct a dictionary of space
        parameters that can be used to create a space.

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used to create a space
        """
        argparser = ArgumentParser(prog="qds.py space_subscribers",
                                   description="SpaceSubscriber client for Qubole Data Service.")
        subparsers = argparser.add_subparsers()

        # Create
        create = subparsers.add_parser("create",
                                       help="Create a new space subscriber")
        create.add_argument("--space_id", dest="space_id",
                            help="id of the space for subscription")
        create.add_argument("--role_arn", dest="role_arn",
                            help="role_arn to access the space")
        create.add_argument("--external_id", dest="external_id",
                            help="external_id to access the space")
        create.set_defaults(func=SpaceSubscriberCmdLine.create)

        # List
        list = subparsers.add_parser("list",
                                     help="List all space subscribers")
        list.set_defaults(func=SpaceSubscriberCmdLine.list)

        # View
        view = subparsers.add_parser("view",
                                     help="View a specific Space Subscriber")
        view.add_argument("id",
                          help="Numeric id of the Space Subscriber")
        view.set_defaults(func=SpaceSubscriberCmdLine.view)

        # Edit
        update = subparsers.add_parser("update",
                                       help="Edit a specific Space Subscriber")
        update.add_argument("id",
                            help="Numeric id of the Space Subscriber")
        update.add_argument("--role_arn", dest="role_arn",
                            help="role_arn to access the space")
        update.add_argument("--external_id", dest="external_id",
                            help="external_id to access the space")
        update.set_defaults(func=SpaceSubscriberCmdLine.update)

        # Delete
        delete = subparsers.add_parser("delete",
                                       help="Delete a specific Space Subscriber")
        delete.add_argument("id",
                            help="Numeric id of the Space Subscriber")
        delete.set_defaults(func=SpaceSubscriberCmdLine.delete)

        # Hivetables
        hivetables = subparsers.add_parser("hivetables",
                                           help="Get all hivetables available/subscribed inside a space")
        hivetables.add_argument("id",
                                help="Numeric id of the Space")
        hivetables.set_defaults(func=SpaceSubscriberCmdLine.hivetables)
        return argparser

    @staticmethod
    def run(args):
        parser = SpaceSubscriberCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)

    @staticmethod
    def create(args):
        space_subscriber = SpaceSubscriber.create(space_id=args.space_id,
                                                  role_arn=args.role_arn, external_id=args.external_id)
        return json.dumps(space_subscriber.attributes, sort_keys=True, indent=4)

    @staticmethod
    def list(args):
        space_list = SpaceSubscriber.list()
        return json.dumps(space_list, sort_keys=True, indent=4)

    @staticmethod
    def view(args):
        tap = SpaceSubscriber.find(args.id)
        return json.dumps(tap.attributes, sort_keys=True, indent=4)

    @staticmethod
    def update(args):
        space_subscriber = SpaceSubscriber.find(args.id)
        options = {'role_arn': args.role_arn, 'external_id': args.external_id}
        space_subscriber = space_subscriber.update(**options)
        return json.dumps(space_subscriber.attributes, sort_keys=True, indent=4)

    @staticmethod
    def delete(args):
        space_subscriber = SpaceSubscriber.find(args.id)
        return json.dumps(space_subscriber.delete(), sort_keys=True, indent=4)

    @staticmethod
    def hivetables(args):
        res = SpaceSubscriber.hivetables(args.id)
        return json.dumps(res, sort_keys=True, indent=4)


class SpaceSubscriber(Resource):
    """
    qds_sdk.SpaceSubscriber is the base Qubole SpaceSubscriber class.
    """

    """ all commands use the /space endpoint"""
    rest_entity_path = "space_subscribers"

    @staticmethod
    def list():
        conn = Qubole.agent()
        url_path = SpaceSubscriber.rest_entity_path
        return conn.get(url_path)

    def update(self, **kwargs):
        conn = Qubole.agent()
        return SpaceSubscriber(conn.put(self.element_path(self.subscription_id), data=kwargs))

    def delete(self):
        conn = Qubole.agent()
        return conn.delete(self.element_path(self.subscription_id))

    @staticmethod
    def hivetables(id):
        conn = Qubole.agent()
        url_path = SpaceSubscriber.rest_entity_path + "/" + str(id) + "/hivetables"
        return conn.get(url_path)