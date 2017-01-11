from qds_sdk.connection import Connection
from qds_sdk.qubole import Qubole
from qds_sdk.user import User
#from ../qds_sdk/qubole import Qubole
#from oracle_bmc_cloud import OracleBmcCloud
#from azure_cloud import AzureCloud
from qds_sdk.cloud.aws_cloud import AwsCloud

class Cloud:

    def __init__(self):
        f=1


    @staticmethod
    def get_cloud():
        conn = Qubole.agent(version="v1.2")
        url_path = "about"
        return conn.get(url_path).get("provider")

    @staticmethod
    def get_cloud_object():
        print("get cloud obect =======clud.py")
        cloud = Qubole.cloud
        if cloud == "azure":
            pass
            #return AzureCloud()
        elif cloud == "oracle_bmc":
            pass
            #return OracleBmcCloud()
        else:
            print("cloud class()====")
            return AwsCloud()







