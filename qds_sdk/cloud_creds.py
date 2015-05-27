__author__ = 'avinashj'

"""
The cloud creds module contains the definitions for basic CRUD operations on CloudCred
"""

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser

import logging
import json

log = logging.getLogger("qds_cloud_cred")


class CloudCredCmdLine:
    """
    qds_sdk.CloudCredCmdLine is the interface used a qds.py
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
        argparser = ArgumentParser(prog="qds.py cloud_creds",
                                   description="CloudCred client for Qubole Data Service.")
        subparsers = argparser.add_subparsers()

        # Create
        create = subparsers.add_parser("create",
                                       help="Create a new cloud cred")
        create.add_argument("--name", dest="name",
                            help="name of cloud creds")
        create.add_argument("--role_arn", dest="role_arn",
                            help="role arn of cloud creds")
        create.add_argument("--external_id", dest="external_id",
                            help="external id of cloud creds")
        create.set_defaults(func=CloudCredCmdLine.create)

        return argparser


    @staticmethod
    def run(args):
        parser = CloudCredCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)

    @staticmethod
    def create(args):
        aws_creds = CloudCred.create(name=args.name,
                                     role_arn=args.role_arn,
                                     external_id=args.external_id)
        return json.dumps(aws_creds.attributes, sort_keys=True, indent=4)


class CloudCred(Resource):
    """
    qds_sdk.QbucketSubscriber is the base Qubole QbucketSubscriber class.
    """

    """ all commands use the /qbucket endpoint"""
    rest_entity_path = "cloud_creds"