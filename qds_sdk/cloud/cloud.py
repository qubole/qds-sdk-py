from qds_sdk.qubole import Qubole

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
            import qds_sdk.cloud.azure_cloud
            return qds_sdk.cloud.azure_cloud.AzureCloud()
        elif cloud == "oracle_bmc":
            import qds_sdk.cloud.oracle_bmc_cloud
            return qds_sdk.cloud.oracle_bmc_cloud.OracleBmcCloud()
        else:
            import qds_sdk.cloud.aws_cloud
            return qds_sdk.cloud.aws_cloud.AwsCloud()