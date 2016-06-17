import requests
import logging
from qds_sdk.connection import Connection
from qds_sdk.exception import ConfigError


log = logging.getLogger("qds_qubole")

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

    MIN_POLL_INTERVAL = 1

    _auth = None
    api_token = None
    baseurl = None
    version = None
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

            `version`: QDS REST api version. Will be used throughout unless overridden in Qubole.agent(..)

            `poll_interval`: interval in secs when polling QDS for events
        """
        cls._auth = QuboleAuth(api_token)
        cls.api_token = api_token
        cls.version = version
        cls.baseurl = api_url
        if poll_interval < Qubole.MIN_POLL_INTERVAL:
            log.warn("Poll interval cannot be less than %s seconds. Setting it to %s seconds.\n" % (Qubole.MIN_POLL_INTERVAL, Qubole.MIN_POLL_INTERVAL))
            cls.poll_interval = Qubole.MIN_POLL_INTERVAL
        else:
            cls.poll_interval = poll_interval
        cls.skip_ssl_cert_check = skip_ssl_cert_check

    cached_agent = None

    @classmethod
    def agent(cls, version=None):
        """
        Returns:
           a connection object to make REST calls to QDS

           optionally override the `version` of the REST endpoint for advanced
           features available only in the newer version of the API available
           for certain resource end points eg: /v1.3/cluster. When version is
           None we default to v1.2
        """
        reuse_cached_agent = True
        if version:
          log.debug("api version changed to %s" % version)
          cls.rest_url = '/'.join([cls.baseurl.rstrip('/'), version])
          reuse_cached_agent = False
        else:
          cls.rest_url = '/'.join([cls.baseurl.rstrip('/'), cls.version])
        if cls.api_token is None:
            raise ConfigError("No API Token specified - please supply one via Qubole.configure()")

        if not reuse_cached_agent:
          uncached_agent = Connection(cls._auth, cls.rest_url, cls.skip_ssl_cert_check)
          return uncached_agent
        if cls.cached_agent is None:
          cls.cached_agent = Connection(cls._auth, cls.rest_url, cls.skip_ssl_cert_check)

        return cls.cached_agent
