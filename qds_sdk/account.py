""" 
The Accounts module contains the base definition for
a generic Qubole account command.

"""

#from qubole import Qubole
#from resource import Resource
from exception import ParseError
from qds_sdk.util import GentleOptionParser
from qds_sdk.util import OptionParsingError
from qds_sdk.util import OptionParsingExit

import time
import logging
import sys
import re
import os

from connection import Connection

log = logging.getLogger("qds_accounts")

class Account:
    """
    qds_sdk.Account is the base Qubole account class. Different types of Qubole
    commands can subclass this.
    """

    """ all commands use the /account endpoint"""
    
    rest_entity_path="account"
    
    def __init__(self, conn_obj):
        
        self.results = conn_obj.get(Account.rest_entity_path)
        
    def get_access_key(self):
        return self.results['acc_key']
        
    def get_secret_key(self):
        return self.results['secret']

