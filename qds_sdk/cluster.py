"""
The cluster module contains the definitions for retrieving the cluster
information from Qubole
"""

from qubole import Qubole
from resource import Resource
from exception import ParseError

import logging

log = logging.getLogger("qds_cluster")


class Cluster(Resource):
    """
    qds_sdk.Cluster is the class for retrieving the cluster information.
    """

    rest_entity_path = "clusters"

    @classmethod
    def status(cls, cluster_id):
        conn = Qubole.agent()
        return conn.get(cls.element_path(cluster_id) + "/state")

    @classmethod
    def start(cls, cluster_id):
        conn = Qubole.agent()
        data = {"state": "start"}
        return conn.put(cls.element_path(cluster_id) + "/state", data)

    @classmethod
    def terminate(cls, cluster_id):
        conn = Qubole.agent()
        data = {"state": "terminate"}
        return conn.put(cls.element_path(cluster_id) + "/state", data)

    @classmethod
    def delete(cls, cluster_id):
        conn = Qubole.agent()
        return conn.delete(cls.element_path(cluster_id))
