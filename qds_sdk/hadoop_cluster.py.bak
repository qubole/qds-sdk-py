"""
The hadoop_cluster module contains the definitions for retrieving hadoop cluster
information from Qubole
This module is deprecated and will be removed in a future version.
Please use the cluster module instead.
"""

from qubole import Qubole
from resource import Resource
from exception import ParseError

import logging

log = logging.getLogger("qds_hadoop_cluster")


class HadoopCluster(Resource):
    """
    qds_sdk.HadoopCluster is the class for retrieving hadoop cluster information.
    This class is deprecated and will be removed in a future version.
    Please use qds_sdk.Cluster instead.
    """

    rest_entity_path = "hadoop_cluster"

    @classmethod
    def find(cls, name="default", **kwargs):
        if (name is None) or (name == "default"):
            conn = Qubole.agent()
            return cls(conn.get(cls.rest_entity_path))
        else:
            raise ParseError("Bad name %s" % name,
                             "Hadoop Clusters can only be named 'default' currently")
