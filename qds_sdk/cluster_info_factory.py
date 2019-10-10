from qds_sdk.qubole import Qubole
from qds_sdk.clusterv2 import ClusterInfoV2
from qds_sdk.cluster_info_v22 import ClusterInfoV22

class ClusterInfoFactory:

  @staticmethod
  def get_cluster_info_cls(api_version=None):
    if api_version is None:
      api_version = ClusterV2.api_version
    if api_version == "v2":
      return ClusterInfoV2
    elif api_version == "v2.2":
      return ClusterInfoV22
    else:
      return ClusterInfoV2



