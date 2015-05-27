__author__ = 'avinashj'

"""
The cross account config module contains the definitions for basic CRUD operations on CrossAccountConfig
"""

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser

import logging
import json

log = logging.getLogger("qds_qbucket_subscriber")


class CrossAccountConfigCmdLine:
    """
    qds_sdk.CrossAccountConfigCmdLine is the interface used a qds.py
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
        argparser = ArgumentParser(prog="qds.py cross_acc_config",
                                   description="CrossAccountConfig client for Qubole Data Service.")
        subparsers = argparser.add_subparsers()

        # Create
        create = subparsers.add_parser("create",
                                       help="Create a new cross account config")
        create.add_argument("--uris", dest="uris",
                            help="path in s3")
        create.add_argument("--cloud_cred_id", dest="cloud_cred_id",
                            help="cloud cred id")
        create.set_defaults(func=CrossAccountConfigCmdLine.create)

        return argparser


    @staticmethod
    def run(args):
        parser = CrossAccountConfigCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)

    @staticmethod
    def create(args):
        cross_account_config = CrossAccountConfig.create(uris=args.uris,
                                                         cloud_cred_id=args.cloud_cred_id)
        return json.dumps(cross_account_config.attributes, sort_keys=True, indent=4)


class CrossAccountConfig(Resource):
    """
    qds_sdk.CrossAccountConfig is the base Qubole CrossAccountConfig class.
    """

    """ all commands use the /qbucket endpoint"""
    rest_entity_path = "cross_account_configs"