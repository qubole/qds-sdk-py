from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from qds_sdk.cloud.cloud import Cloud
import argparse

class ClusterCmdLine:

    @staticmethod
    def parsers():
        cloud = Cloud.get_cloud_object()
        argparser = argparse.ArgumentParser(
            prog="qds.py cluster",
            description="Cluster Operations for Qubole Data Service.")
        subparsers = argparser.add_subparsers(title="cluster operations")


        pass

    @staticmethod
    def run(args):
        parser = ClusterCmdLine.parsers()
        parsed = parser.parse_args(args)
        pass



class ClusterV2(Resource):

    @classmethod
    def _parse_create_update(cls, args, action, api_version):

        pass

    @classmethod
    def create(cls, cluster_info, version=None):
        """
        Create a new cluster using information provided in `cluster_info`.

        """
        conn = Qubole.agent(version=version)
        return conn.post(cls.rest_entity_path, data=cluster_info)

    @classmethod
    def update(cls, cluster_id_label, cluster_info, version=None):
        """
        Update the cluster with id/label `cluster_id_label` using information provided in
        `cluster_info`.

        """
        print "data==="
        print cluster_info
        conn = Qubole.agent(version=version)
        return conn.put(cls.element_path(cluster_id_label), data=cluster_info)

    @classmethod
    def clone(cls, cluster_id_label, cluster_info, version=None):
        """
        Update the cluster with id/label `cluster_id_label` using information provided in
        `cluster_info`.

        """
        conn = Qubole.agent(version=version)
        return conn.post(cls.element_path(cluster_id_label) + '/clone', data=cluster_info)

    # implementation needed
    @classmethod
    def list(self, state=None):
        pass







