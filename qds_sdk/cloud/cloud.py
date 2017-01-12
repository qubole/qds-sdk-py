from qds_sdk.qubole import Qubole
from qds_sdk.cloud.aws_cloud import AwsCloud
from qds_sdk.cloud.azure_cloud import AzureCloud
from qds_sdk.cloud.oracle_bmc_cloud import OracleBmcCloud

class Cloud:

    @staticmethod
    def get_cloud():
        conn = Qubole.agent(version="v1.2")
        url_path = "about"
        return conn.get(url_path).get("provider")

    @staticmethod
    def get_cloud_object():
        cloud = Qubole.cloud
        if cloud == "azure":
            return AzureCloud()
        elif cloud == "oracle_bmc":
            return OracleBmcCloud()
        else:
            return AwsCloud()