import os
import requests
import cjson
from connection import Connection

class QuboleAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.api_token = token
    def __call__(self, r):
        r.headers['X-AUTH-TOKEN'] = self.api_token
        return r

class Qubole:

    _auth=None
    api_token=None
    base_url=None
    poll_interval=None

    @classmethod
    def configure(cls, api_token, 
                  api_url="https://api.qubole.com/api/", version="v1.2", 
                  poll_interval=5):
        cls._auth=QuboleAuth(api_token)
        cls.api_token=api_token
        cls.base_url=os.path.join(api_url, version)
        cls.poll_interval=poll_interval

    @classmethod
    def agent(cls):
        return Connection(cls._auth, cls.base_url)
