"""
The Accounts module contains the base definition for a Qubole account object
"""
import json
from qds_sdk.resource import SingletonResource
from qds_sdk.qubole import Qubole
from qds_sdk.exception import ParseError
from qds_sdk.util import GentleOptionParser
from qds_sdk.util import OptionParsingError
from qds_sdk.util import OptionParsingExit
import argparse

class AccountCmdLine:


    @staticmethod
    def parsers():
        argparser = argparse.ArgumentParser(prog="qds.py account",
                description="Account Creation for Qubole Data Service.")
        subparsers = argparser.add_subparsers(title = "account operation")

        #Create
        create = subparsers.add_parser("create",
                                       help="Create a new account")
        create.add_argument("--name", dest="name", help="account name")
        create.add_argument("--location", dest="defloc", help="Default location of S3")
        create.add_argument("--access", dest = "acc_key", help="access key")
        create.add_argument("--secret", dest= "secret", help="secret key")
        create.add_argument("--compute_access", dest="compute_access_key", help="compute access key")
        create.add_argument("--compute_secret", dest="compute_secret_key", help="compute secret key")
        create.add_argument('--aws', dest="aws_region", help="aws region")
        create.add_argument("--previous_account_plan", dest="use_previous_account_plan",choices=["true", "false"], help="either true or false")
        create.set_defaults(func=AccountCmdLine.create)
        return argparser

    @staticmethod
    def run(args):
        parser = AccountCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)

    @staticmethod
    def create(args):
        args.level = "free"
        args.compute_type = "CUSTOMER_MANAGED"
        args.storage_type = "CUSTOMER_MANAGED"
        args.CacheQuotaSizeInGB = "25"
        v = {}
        args = vars(args)
        args.pop("func")
        v['account'] = args
        result = Account.create(**v)
        return result


class Account(SingletonResource):

    credentials_rest_entity_path = "accounts/get_creds"
    rest_entity_path = "account"

    @classmethod
    def create(cls, **kwargs):
        conn = Qubole.agent()
        return cls(conn.post(cls.rest_entity_path, data=kwargs))