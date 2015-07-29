__author__ = 'avinashj'

"""
The accounts module contains the definitions for basic CRUD operations on Accounts
"""

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser

import logging
import json

log = logging.getLogger("qds_account")


class AccountCmdLine:
    """
    qds_sdk.AccountCmdLine is the interface used a qds.py
    """

    @staticmethod
    def parsers():
        """
        Parse command line arguments to construct a dictionary of accounts
        parameters that can be used to create an account.

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used to create a space
        """
        argparser = ArgumentParser(prog="qds.py accounts",
                                   description="Accounts client for Qubole Data Service.")
        subparsers = argparser.add_subparsers()

        # create
        create = subparsers.add_parser("create",
                                       help="Create a new account")
        create.add_argument("--account", dest="account",
                            help="create an account with the given account parameters")
        create.set_defaults(func=AccountCmdLine.create)

        return argparser

    @staticmethod
    def run(args):
        parser = AccountCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)

    @staticmethod
    def create(args):
        account = Account.create(args.account)
        return json.dumps(account, sort_keys=True, indent=4)


class Account(Resource):
    """
    qds_sdk.Account is the base Qubole Account class.
    """

    """ all commands use the /account endpoint"""
    rest_entity_path = "account"

    @staticmethod
    def create(args):
        conn = Qubole.agent()
        url_path = Account.rest_entity_path
        return conn.post(url_path, {"account": json.loads(args)})
