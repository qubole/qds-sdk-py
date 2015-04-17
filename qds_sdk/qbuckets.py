"""
The qbucket module contains the definitions for basic CRUD operations on Qbucket
"""

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser

import logging
import json

log = logging.getLogger("qds_qbucket")


class QbucketCmdLine:
    """
    qds_sdk.QbucketCmdLine is the interface used a qds.py
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
        argparser = ArgumentParser(prog="qds.py qbuckets",
                                        description="Qbuckets client for Qubole Data Service.")
        subparsers = argparser.add_subparsers()

        # Create
        create = subparsers.add_parser("create",
                                       help="Create a new qbucket")
        create.add_argument("--name", dest="name",
                            help="create qbucket with the this name")
        create.add_argument("--path", dest="path",
                            help="create qbucket on this path")
        create.add_argument("--object_store_type", dest="object_store_type", default="s3",
                            help="create qbucket of this store type")
        create.add_argument("--acl", dest="acl", default="private",
                            help="create qbucket with this acl")
        create.set_defaults(func=QbucketCmdLine.create)

        # List
        list = subparsers.add_parser("list",
                                     help="List all qbuckets of the given account")
        list.set_defaults(func=QbucketCmdLine.list)

        # View
        view = subparsers.add_parser("view",
                                     help="View a specific Qbucket")
        view.add_argument("id",
                          help="Numeric id of the Qbucket")
        view.set_defaults(func=QbucketCmdLine.view)

        return argparser

    @staticmethod
    def run(args):
        parser = QbucketCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)

    @staticmethod
    def create(args):
        qbucket = Qbucket.create(name=args.name,
                                 path=args.path,
                                 object_store_type=args.object_store_type,
                                 acl=args.acl)

        return json.dumps(qbucket.attributes, sort_keys=True, indent=4)

    @staticmethod
    def list(args):
        qbucket_list = Qbucket.list()
        return json.dumps(qbucket_list, sort_keys=True, indent=4)

    @staticmethod
    def view(args):
        tap = Qbucket.find(args.id)
        return json.dumps(tap.attributes, sort_keys=True, indent=4)


class Qbucket(Resource):
    """
    qds_sdk.Qbucket is the base Qubole Qbucket class.
    """

    """ all commands use the /qbucket endpoint"""
    rest_entity_path = "qbuckets"

    @staticmethod
    def list():
        conn = Qubole.agent()
        url_path = Qbucket.rest_entity_path
        return conn.get(url_path)
