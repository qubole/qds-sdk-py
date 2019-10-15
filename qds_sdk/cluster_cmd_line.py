from qds_sdk.cluster_info_factory import ClusterInfoFactory
from qds_sdk.clusterv2 import ClusterV2
from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from qds_sdk.cloud.cloud import Cloud
from qds_sdk.engine import Engine
from qds_sdk import util
import argparse
import json

class ClusterCmdLine:

    @staticmethod
    def parsers(action):
        argparser = argparse.ArgumentParser(
            prog="qds.py cluster",
            description="Cluster Operations for Qubole Data Service.")
        subparsers = argparser.add_subparsers(title="Cluster operations")
        if Qubole.version is not None:
            ClusterV2.api_version = Qubole.version
        if action == "create":
            create = subparsers.add_parser("create", help="Create a new cluster")
            ClusterCmdLine.create_update_clone_parser(create, action="create")
            create.set_defaults(func=ClusterV2.create)

        if action == "update":
            update = subparsers.add_parser("update", help="Update the settings of an existing cluster")
            ClusterCmdLine.create_update_clone_parser(update, action="update")
            update.set_defaults(func=ClusterV2.update)

        if action == "clone":
            clone = subparsers.add_parser("clone", help="Clone a cluster from an existing one")
            ClusterCmdLine.create_update_clone_parser(clone, action="clone")
            clone.set_defaults(func=ClusterV2.clone)

        if action == "list":
            li = subparsers.add_parser("list", help="list clusters from existing clusters depending upon state")
            ClusterCmdLine.list_parser(li, action="list")
            li.set_defaults(func=ClusterV2.list)
        return argparser

    @staticmethod
    def list_parser(subparser, action=None, ):

        # cluster info parser
        cluster_info_cls = ClusterInfoFactory.get_cluster_info_cls()
        cluster_info_cls.list_info_parser(subparser, action)

    @staticmethod
    def create_update_clone_parser(subparser, action=None):
        # cloud config parser
        cloud = Qubole.get_cloud()
        cloud.create_parser(subparser)

        # cluster info parser
        cluster_info_cls = ClusterInfoFactory.get_cluster_info_cls()
        cluster_info_cls.cluster_info_parser(subparser, action)

        # engine config parser
        Engine.engine_parser(subparser)

    @staticmethod
    def run(args):
        parser = ClusterCmdLine.parsers(args[0])
        arguments = parser.parse_args(args)
        if args[0] in ["create", "clone", "update"]:
            ClusterCmdLine.get_cluster_create_clone_update(arguments, args[0])
        else:
            return arguments.func(arguments.label, arguments.cluster_id, arguments.state,
                                  arguments.page, arguments.per_page)

    @staticmethod
    def get_cluster_create_clone_update(arguments, action):

        # This will set cluster info and monitoring settings
        cluster_info_cls = ClusterInfoFactory.get_cluster_info_cls()
        cluster_info = cluster_info_cls(arguments.label)
        cluster_info.set_cluster_info_from_arguments(arguments)

        #  This will set cloud config settings
        cloud_config = Qubole.get_cloud()
        cloud_config.set_cloud_config_from_arguments(arguments)

        # This will set engine settings
        engine_config = Engine(flavour=arguments.flavour)
        engine_config.set_engine_config_settings(arguments)
        cluster_request = ClusterCmdLine.get_cluster_request_parameters(cluster_info, cloud_config, engine_config)

        action = action
        if action == "create":
            return arguments.func(cluster_request)
        else:
            return arguments.func(arguments.cluster_id_label, cluster_request)

    @staticmethod
    def get_cluster_request_parameters(cluster_info, cloud_config, engine_config):
        '''
        Use this to return final minimal request from cluster_info, cloud_config or engine_config objects
        Alternatively call util._make_minimal if only one object needs to be implemented
        '''

        cluster_request = {}
        cloud_config = util._make_minimal(cloud_config.__dict__)
        if bool(cloud_config): cluster_request['cloud_config'] = cloud_config

        engine_config = util._make_minimal(engine_config.__dict__)
        if bool(engine_config): cluster_request['engine_config'] = engine_config

        cluster_request.update(util._make_minimal(cluster_info.__dict__))
        return cluster_request