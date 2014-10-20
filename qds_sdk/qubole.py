import requests
from qds_sdk.connection import Connection
from qds_sdk.exception import ConfigError


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

    _auth = None
    api_token = None
    base_url = None
    poll_interval = None
    skip_ssl_cert_check = None

    @classmethod
    def configure(cls, api_token,
                  api_url="https://api.qubole.com/api/", version="v1.2",
                  poll_interval=5, skip_ssl_cert_check=False):
        """
        Set parameters governing interaction with QDS

        Args:
            `api_token`: authorization token for QDS. required

            `api_url`: the base URL for QDS API. configurable for testing only

            `version`: QDS REST api version

            `poll_interval`: interval in secs when polling QDS for events
        """
        cls._auth = QuboleAuth(api_token)
        cls.api_token = api_token
        cls.base_url = api_url.rstrip('/') + '/' + version
        cls.poll_interval = poll_interval
        cls.skip_ssl_cert_check = skip_ssl_cert_check

    cached_agent = None

    @classmethod
    def agent(cls):
        """
        Returns:
           a connection object to make REST calls to QDS
        """
        if cls.api_token is None:
            raise ConfigError("No API Token specified - please supply one via Qubole.configure()")

        if cls.cached_agent is None:
            cls.cached_agent = Connection(cls._auth, cls.base_url, cls.skip_ssl_cert_check)

        return cls.cached_agent
