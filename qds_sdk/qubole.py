import os
import requests
import cjson
from connection import Connection
from account import Account

class QuboleAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.api_token = token
    def __call__(self, r):
        r.headers['X-AUTH-TOKEN'] = self.api_token
        return r

class Qubole:
    """
    Singleton for storing authorization credentials and other
    configuration parameters for QDS.
    """

    _auth=None
    api_token=None
    base_url=None
    poll_interval=None
    
    account_cache = None
    
    @classmethod
    def configure(cls, api_token, 
                  api_url="https://api.qubole.com/api/", version="v1.2", 
                  poll_interval=5):
        """
        Set parameters governing interaction with QDS
        Args:
            ``api_token``: authorization token for QDS. required
            ``api_url``: the base URL for QDS API. configurable for testing only
            ``version``: QDS REST api version
            ``poll_interval``: interval in secs when polling QDS for events
        """
        cls._auth=QuboleAuth(api_token)
        cls.api_token=api_token
        cls.base_url=os.path.join(api_url, version)
        cls.poll_interval=poll_interval

    @classmethod
    def agent(cls):
        """
        Returns:
           a connection object to make REST calls to QDS
        """
        return Connection(cls._auth, cls.base_url)
        
    @classmethod
    def get_Account(cls):
        """
        Returns:
           a account object to make REST calls to QDS
        """
        if Qubole.account_cache is None:
            account_cache = Account(Connection(cls._auth, cls.base_url))
        
        return account_cache
