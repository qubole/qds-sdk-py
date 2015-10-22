"""
The Accounts module contains the base definition for a Qubole account object
"""

from qds_sdk.resource import SingletonResource
from qds_sdk.qubole import Qubole
from qds_sdk.exception import ParseError
from qds_sdk.util import GentleOptionParser
from qds_sdk.util import OptionParsingError
from qds_sdk.util import OptionParsingExit
import argparse

class Account(SingletonResource):

    credentials_rest_entity_path = "accounts/get_creds"
    rest_entity_path = "account"

    @classmethod
    def _parse_account(cls, args, action):

        argparser = argparse.ArgumentParser(prog="account create",
                description="Account Creation for Qubole Data Service.")

        argparser.add_argument("--name", dest="name", help="account name")
        argparser.add_argument("--location", dest="defloc", help="Default location of S3")
        argparser.add_argument("--access", dest = "acc_key", help="access key")
        argparser.add_argument("--secret", dest= "secret", help="secret key")
        argparser.add_argument("--compute_access", dest="compute_access_key", help="compute access key")
        argparser.add_argument("--compute_secret", dest="compute_secret_key", help="compute secret key")
        argparser.add_argument('--aws', dest="aws_region", help="aws region")
        argparser.add_argument("--account_plan", dest="use_previous_account_plan", help="use previous account plan")
        arguments = argparser.parse_args(args)
        arguments.level = "free"
        arguments.compute_type = "CUSTOMER_MANAGED"
        arguments.storage_type = "CUSTOMER_MANAGED"
        arguments.CacheQuotaSizeInGB = "25"
        v = {}
        v['account'] = vars(arguments)
        return v

    @classmethod
    def create(cls, **kwargs):
        conn = Qubole.agent()
        return cls(conn.post(cls.rest_entity_path, data=kwargs))