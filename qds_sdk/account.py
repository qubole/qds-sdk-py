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

        #branding
        branding = subparsers.add_parser(
            "branding", help="Branding logo and link")
        branding.add_argument("--account-id", dest = "account_id", required=True, help = "Account ID of the Qubole account for which branding has to be done")
        branding.add_argument("--logo-uri", dest = "logo_uri", help = "Publicly accessible logo URI image in jpg/gif/svg/jpeg format.Image size must be less than 100 KB.")
        branding.add_argument("--link-url", dest = "link_url", help = "Specify the documentation URL.")
        branding.add_argument("--link-label", dest = "link_label", help = "Add a label to describe the documentation URL.")
        branding.set_defaults(func=AccountCmdLine.branding)
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

    @staticmethod
    def branding(args):
        v= {}
        print(args)
        v['account_id'] = args.account_id
        if args.logo_uri is not None:
            v['logo'] = {'logo_uri' : args.logo_uri }

        link = {}
        if args.link_url is not None:
            link['link_url'] = args.link_url
        if args.link_label is not None:
            link['link_label'] = args.link_label

        if bool(link):
            v['link'] = link

        result = Account.branding(**v)
        return result

class Account(SingletonResource):
    credentials_rest_entity_path = "accounts/get_creds"
    rest_entity_path = "account"

    @classmethod
    def create(cls, **kwargs):
        conn = Qubole.agent()
        return cls(conn.post(cls.rest_entity_path, data=kwargs))

    @classmethod
    def branding(cls, **kwargs):
        conn = Qubole.agent()
        url_path = "accounts/branding"
        return  cls(conn.put(url_path, data=kwargs))
