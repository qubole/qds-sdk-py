from qds_sdk.connection import Connection
from qds_sdk.qubole import Qubole
from qds_sdk.user import User
#from ../qds_sdk/qubole import Qubole
import oracle_bmc_cloud.OracleBmcCloud
from azure_cloud import AzureCloud
from aws_cloud import AwsCloud

class Cloud:

    def __init__(self, token):
        self.api_token = token


    @staticmethod
    def get_cloud():
        return Connection(Qubole._auth, Qubole.baseurl.rstrip("/").replace(Qubole.baseurl.rstrip("/").split("/")[-1], ""),
                          Qubole.skip_ssl_cert_check).get("about").get("provider")

    def get_cloud_object(self):
        cloud = Qubole.cloud
        if cloud == "azure":
            return AzureCloud()
        elif cloud == "oracle_bmc":
            return oracle_bmc_cloud.OracleBmcCloud()
        else:
            return AwsCloud()







