from qds_sdk.qubole import Qubole

class Cloud:

    @staticmethod
    def get_cloud():
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

    def create_parser(self, argparser):
        return NotImplemented

    def set_cloud_config_from_arguments(self, arguments):
        return NotImplemented
