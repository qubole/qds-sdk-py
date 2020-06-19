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
    RETRIES_CAP = 7
    MAX_RETRY_DELAY = 10

    _auth = None
    api_token = None
    baseurl = None
    version = None
    poll_interval = None
    skip_ssl_cert_check = None
    cloud_name = None
    cached_agent = None
    cloud = None
    base_retry_delay = None
    max_retries = None

    @classmethod
    def configure(cls, api_token,
                  api_url="https://api.qubole.com/api/", version="v1.2",
                  poll_interval=5, skip_ssl_cert_check=False, cloud_name="AWS",
                  base_retry_delay=10, max_retries=7):
        """
        Set parameters governing interaction with QDS
        Args:
            `api_token`: authorization token for QDS. required
            `api_url`: the base URL for QDS API. configurable for testing only
            `version`: QDS REST api version. Will be used throughout unless overridden in Qubole.agent(..)
            `poll_interval`: interval in secs when polling QDS for events
            `delay` : interval in secs to sleep in between successive retries
            `retries` : maximum number of time to retry an api call in case
                        of retryable exception.
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
        cls.cloud_name = cloud_name.lower()
        cls.cached_agent = None
        if base_retry_delay > Qubole.MAX_RETRY_DELAY:
            log.warn("Sleep between successive retries cannot be greater than"
                     " %s seconds."
                     " Setting it to"
                     " %s seconds.\n"
                     % (Qubole.MAX_RETRY_DELAY, Qubole.MAX_RETRY_DELAY))
            cls.base_retry_delay = Qubole.MAX_RETRY_DELAY
        else:
            cls.base_retry_delay = base_retry_delay
        if max_retries > Qubole.RETRIES_CAP:
            log.warn("Maximum retries cannot be greater than %s."
                     " Setting it to"
                     " default - %s.\n" % (Qubole.RETRIES_CAP, Qubole.RETRIES_CAP))
            cls.max_retries = Qubole.RETRIES_CAP
        else:
            cls.max_retries = max_retries

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
            uncached_agent = Connection(cls._auth, cls.rest_url,
                                        cls.skip_ssl_cert_check,
                                        True, cls.max_retries, cls.base_retry_delay)
            return uncached_agent
        if cls.cached_agent is None:
            cls.cached_agent = Connection(cls._auth, cls.rest_url,
                                          cls.skip_ssl_cert_check,
                                          True, cls.max_retries, cls.base_retry_delay)

        return cls.cached_agent

    @classmethod
    def get_cloud(cls, cloud_name=None):
        if cloud_name and cloud_name.lower() not in ["aws", "oracle_bmc", "azure", "oracle_opc", "gcp"]:
            raise Exception("cloud should be 'aws', 'oracle_bmc', 'azure', 'oracle_opc' or 'gcp'")

        if cloud_name:
            return Qubole.get_cloud_object(cloud_name)
        else:
            if cls.cloud is None:
                cls.cloud = cls.get_cloud_object(cls.cloud_name)
            return cls.cloud

    @classmethod
    def get_cloud_object(cls, cloud_name):
        if cloud_name.lower() == "aws":
            import qds_sdk.cloud.aws_cloud
            return qds_sdk.cloud.aws_cloud.AwsCloud()
        elif cloud_name.lower()  == "oracle_bmc":
            import qds_sdk.cloud.oracle_bmc_cloud
            return qds_sdk.cloud.oracle_bmc_cloud.OracleBmcCloud()
        elif cloud_name.lower()  == "azure":
            import qds_sdk.cloud.azure_cloud
            return qds_sdk.cloud.azure_cloud.AzureCloud()
        elif cloud_name.lower()  == "oracle_opc":
            import qds_sdk.cloud.oracle_opc_cloud
            return qds_sdk.cloud.oracle_opc_cloud.OracleOpcCloud()
        elif cloud_name.lower() == "gcp":
            import qds_sdk.cloud.gcp_cloud
            return qds_sdk.cloud.gcp_cloud.GcpCloud()

    @classmethod
    def get_cloud_name(cls):
        return Qubole.cloud_name
