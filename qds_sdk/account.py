"""
The Accounts module contains the base definition for a Qubole account object
"""

from qds_sdk.resource import SingletonResource
from qds_sdk.qubole import Qubole
from qds_sdk.exception import ParseError
from qds_sdk.util import GentleOptionParser
from qds_sdk.util import OptionParsingError
from qds_sdk.util import OptionParsingExit

class Account(SingletonResource):
    usage = ("account <create> [options]")
    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("--name", dest="name", help="account name")
    optparser.add_option("--location", dest="defloc", help="Default location of S3")
    optparser.add_option("--access", dest = "acc_key", help="access key")
    optparser.add_option("--secret", dest= "secret", help="secret key")
    optparser.add_option("--compute_access", dest="compute_access_key", help="compute access key")
    optparser.add_option("--compute_secret", dest="compute_secret_key", help="compute secret key")
    optparser.add_option('--aws', dest="aws_region", help="aws region")
    optparser.add_option("--account_plan", dest="use_previous_account_plan", help="use previous account plan")


    credentials_rest_entity_path = "accounts/get_creds"
    rest_entity_path = "account"


    @classmethod
    def create(cls, **kwargs):
        conn = Qubole.agent()
        return cls(conn.post(cls.rest_entity_path, data=kwargs))

    @classmethod
    def _parse_account(cls, args, action):
        if action == "create":
            try:
                (options, args) = cls.optparser.parse_args(args)
            except OptionParsingError as e:
                raise ParseError(e.msg, cls.optparser.format_help())
            except OptionParsingExit as e:
                return None
        v = {}
        options.level = "free"
        options.compute_type = "CUSTOMER_MANAGED"
        options.storage_type = "CUSTOMER_MANAGED"
        options.CacheQuotaSizeInGB = "25"

        v['account'] = vars(options)
        return v







