"""
The Accounts module contains the base definition for a Qubole account object
"""
from qds_sdk.resource import SingletonResource
from qds_sdk.qubole import Qubole
import argparse


class AccountCmdLine:
    @staticmethod
    def parsers():
        argparser = argparse.ArgumentParser(
            prog="qds.py account",
            description="Account Creation for Qubole Data Service.")
        subparsers = argparser.add_subparsers(title="account operations")

        # Create
        create = subparsers.add_parser(
            "create", help="Create a new account")
        create.add_argument(
            "--name", dest="name", required=True, help="Account name")
        create.add_argument(
            "--location", dest="defloc", required=True,
            help="Default S3 location for storing logs, result and cached data")
        create.add_argument(
            "--storage-access-key", dest="acc_key", required=True,
            help="AWS Access Key ID for storage")
        create.add_argument(
            "--storage-secret-key", dest="secret", required=True,
            help="AWS Secret Access Key for storage")
        create.add_argument(
            "--compute-access-key", dest="compute_access_key", required=True,
            help="AWS Access Key ID for compute")
        create.add_argument(
            "--compute-secret-key", dest="compute_secret_key", required=True,
            help="AWS Secret Access Key for compute")
        create.add_argument(
            "--aws-region", dest="aws_region", required=True, help="AWS Region",
            choices=["us-east-1", "us-west-2", "ap-northeast-1", "sa-east-1",
                     "eu-west-1", "ap-southeast-1", "us-west-1"])
        create.add_argument(
            "--previous-account-plan", dest="use_previous_account_plan",
            choices=["true", "false"], default="false",
            help="Use previous account plan, default: false")
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
