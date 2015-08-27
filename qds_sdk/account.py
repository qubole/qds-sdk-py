"""
The Accounts module contains the base definition for a Qubole account object
"""

from qds_sdk.resource import SingletonResource


class Account(SingletonResource):
    credentials_rest_entity_path = "accounts/get_creds"
    pass
