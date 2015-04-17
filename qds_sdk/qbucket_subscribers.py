__author__ = 'avinashj'


"""
The qbucket subscribers module contains the definitions for basic CRUD operations on QbucketSubscriber
"""

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser

import logging
import json

log = logging.getLogger("qds_qbucket_subscriber")


class QbucketSubscriberCmdLine:
    """
    qds_sdk.QbucketSubscriberCmdLine is the interface used a qds.py
    """

    @staticmethod
    def parsers():
        """
        Parse command line arguments to construct a dictionary of qbucket
        parameters that can be used to create a qbucket.

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used to create a qbucket
        """
        argparser = ArgumentParser(prog="qds.py qbucket_subscribers",
                                   description="QbucketSubscriber client for Qubole Data Service.")
        subparsers = argparser.add_subparsers()

        # Create
        create = subparsers.add_parser("create",
                                       help="Create a new qbucket subscriber")
        create.add_argument("--qbucket_id", dest="qbucket_id",
                            help="id of the qbucket for subscription")
        create.add_argument("--storage_access_key", dest="storage_access_key",
                            help="Access key of the qbucket")
        create.add_argument("--storage_secret_key", dest="storage_secret_key",
                            help="Secret key of the qbucket")
        create.set_defaults(func=QbucketSubscriberCmdLine.create)

        # List
        list = subparsers.add_parser("list",
                                     help="List all qbucket subscribers")
        list.set_defaults(func=QbucketSubscriberCmdLine.list)

        # View
        view = subparsers.add_parser("view",
                                     help="View a specific Qbucket Subscriber")
        view.add_argument("id",
                          help="Numeric id of the Qbucket Subscriber")
        view.set_defaults(func=QbucketSubscriberCmdLine.view)

        # Edit
        edit = subparsers.add_parser("edit",
                                     help="Edit a specific Qbucket Subscriber")
        edit.add_argument("id",
                          help="Numeric id of the Qbucket Subscriber")
        edit.add_argument("--storage_access_key", dest="storage_access_key",
                          help="Access key of the qbucket")
        edit.add_argument("--storage_secret_key", dest="storage_secret_key",
                          help="Secret key of the qbucket")
        edit.set_defaults(func=QbucketSubscriberCmdLine.edit)

        # Delete
        delete = subparsers.add_parser("delete",
                                       help="Delete a specific Qbucket Subscriber")
        delete.add_argument("id",
                            help="Numeric id of the Qbucket Subscriber")
        delete.set_defaults(func=QbucketSubscriberCmdLine.delete)

        return argparser

    @staticmethod
    def run(args):
        parser = QbucketSubscriberCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)

    @staticmethod
    def create(args):
        qbucket_subscriber = QbucketSubscriber.create(qbucket_id=args.qbucket_id,
                                                      storage_access_key=args.storage_access_key,
                                                      storage_secret_key=args.storage_secret_key)
        return json.dumps(qbucket_subscriber.attributes, sort_keys=True, indent=4)

    @staticmethod
    def list(args):
        qbucket_list = QbucketSubscriber.list()
        return json.dumps(qbucket_list, sort_keys=True, indent=4)

    @staticmethod
    def view(args):
        tap = QbucketSubscriber.find(args.id)
        return json.dumps(tap.attributes, sort_keys=True, indent=4)

    @staticmethod
    def edit(args):
        qbucket_subscriber = QbucketSubscriber.find(args.id)
        options = {}
        if args.storage_access_key is not None:
            options["storage_access_key"] = args.storage_access_key
        if args.storage_secret_key is not None:
            options["storage_secret_key"]=args.storage_secret_key
        qbucket_subscriber = qbucket_subscriber.edit(**options)
        return json.dumps(qbucket_subscriber.attributes, sort_keys=True, indent=4)

    @staticmethod
    def delete(args):
        qbucket_subscriber = QbucketSubscriber.find(args.id)
        return json.dumps(qbucket_subscriber.delete(), sort_keys=True, indent=4)


class QbucketSubscriber(Resource):
    """
    qds_sdk.QbucketSubscriber is the base Qubole QbucketSubscriber class.
    """

    """ all commands use the /qbucket endpoint"""
    rest_entity_path = "qbucket_subscribers"

    @staticmethod
    def list():
        conn = Qubole.agent()
        url_path = QbucketSubscriber.rest_entity_path
        return conn.get(url_path)

    def edit(self, **kwargs):
        conn = Qubole.agent()
        return QbucketSubscriber(conn.put(self.element_path(self.id), data=kwargs))

    def delete(self):
        conn = Qubole.agent()
        return conn.delete(self.element_path(self.id))