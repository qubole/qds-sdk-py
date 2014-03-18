"""
The cluster module contains the definitions for retrieving the cluster
information from Qubole
"""

from qubole import Qubole
from resource import Resource
from exception import ParseError
from argparse import ArgumentParser

import logging

log = logging.getLogger("qds_cluster")


class Cluster(Resource):
    """
    qds_sdk.Cluster is the class for retrieving the cluster information.
    """

    rest_entity_path = "clusters"

    @classmethod
    def _parse_list(cls, args):
        argparser = ArgumentParser(prog="cluster list")

        group = argparser.add_mutually_exclusive_group()

        group.add_argument("--id", dest="cluster_id",
                         help="show cluster with this id")

        group.add_argument("--label", dest="label",
                         help="show cluster with this label")

        group.add_argument("--state", dest="state", action="store",
                         choices=['up', 'down', 'pending', 'terminating'],
                         help="list only clusters in the given state")

        arguments = argparser.parse_args(args)
        return vars(arguments)

    @classmethod
    def list(cls, label=None, state=None):
        conn = Qubole.agent()
        if label is None and state is None:
            return conn.get(cls.rest_entity_path)
        elif label is not None and state is None:
            cluster_list = conn.get(cls.rest_entity_path)
            result = []
            for cluster in cluster_list:
                if label in cluster['cluster']['label']:
                    result.append(cluster)
            return result
        elif label is None and state is not None:
            cluster_list = conn.get(cls.rest_entity_path)
            result = []
            for cluster in cluster_list:
                if state.lower() == cluster['cluster']['state'].lower():
                    result.append(cluster)
            return result
        else:
            sys.stderr.write("Can filter either by label or by state but not both")

    @classmethod
    def show(cls, cluster_id):
        conn = Qubole.agent()
        return conn.get(cls.element_path(cluster_id))

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
