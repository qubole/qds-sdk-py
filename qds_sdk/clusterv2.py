from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from qds_sdk.cloud.cloud import Cloud
from qds_sdk.engine import Engine
from qds_sdk import util
import argparse
import json

def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")

class ClusterCmdLine:

    @staticmethod
    def parsers(action):
        argparser = argparse.ArgumentParser(
            prog="qds.py cluster",
            description="Cluster Operations for Qubole Data Service.")
        subparsers = argparser.add_subparsers(title="Cluster operations")

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
    def list_parser(subparser, action=None):

        # cluster info parser
        ClusterInfoV2.list_info_parser(subparser, action)

    @staticmethod
    def create_update_clone_parser(subparser, action=None):
        # cloud config parser
        cloud = Qubole.get_cloud()
        cloud.create_parser(subparser)

        # cluster info parser
        ClusterInfoV2.cluster_info_parser(subparser, action)

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
        customer_ssh_key = util._read_file(arguments.customer_ssh_key_file)
        # This will set cluster info and monitoring settings
        cluster_info = ClusterInfoV2(arguments.label)
        cluster_info.set_cluster_info(disallow_cluster_termination=arguments.disallow_cluster_termination,
                                      enable_ganglia_monitoring=arguments.enable_ganglia_monitoring,
                                      datadog_api_token=arguments.datadog_api_token,
                                      datadog_app_token=arguments.datadog_app_token,
                                      node_bootstrap=arguments.node_bootstrap_file,
                                      master_instance_type=arguments.master_instance_type,
                                      slave_instance_type=arguments.slave_instance_type,
                                      min_nodes=arguments.initial_nodes,
                                      max_nodes=arguments.max_nodes,
                                      slave_request_type=arguments.slave_request_type,
                                      fallback_to_ondemand=arguments.fallback_to_ondemand,
                                      node_base_cooldown_period=arguments.node_base_cooldown_period,
                                      node_spot_cooldown_period=arguments.node_spot_cooldown_period,
                                      custom_tags=arguments.custom_tags,
                                      heterogeneous_config=arguments.heterogeneous_config,
                                      maximum_bid_price_percentage=arguments.maximum_bid_price_percentage,
                                      timeout_for_request=arguments.timeout_for_request,
                                      maximum_spot_instance_percentage=arguments.maximum_spot_instance_percentage,
                                      stable_maximum_bid_price_percentage=arguments.stable_maximum_bid_price_percentage,
                                      stable_timeout_for_request=arguments.stable_timeout_for_request,
                                      stable_spot_fallback=arguments.stable_spot_fallback,
                                      spot_block_duration=arguments.spot_block_duration,
                                      idle_cluster_timeout=arguments.idle_cluster_timeout,
                                      disk_count=arguments.count,
                                      disk_type=arguments.disk_type,
                                      disk_size=arguments.size,
                                      root_disk_size=arguments.root_disk_size,
                                      upscaling_config=arguments.upscaling_config,
                                      enable_encryption=arguments.encrypted_ephemerals,
                                      customer_ssh_key=customer_ssh_key,
                                      image_uri_overrides=arguments.image_uri_overrides,
                                      env_name=arguments.env_name,
                                      python_version=arguments.python_version,
                                      r_version=arguments.r_version,
                                      disable_cluster_pause=arguments.disable_cluster_pause,
                                      paused_cluster_timeout_mins=arguments.paused_cluster_timeout_mins,
                                      disable_autoscale_node_pause=arguments.disable_autoscale_node_pause,
                                      paused_autoscale_node_timeout_mins=arguments.paused_autoscale_node_timeout_mins)

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

class ClusterInfoV2(object):
    """
    qds_sdk.ClusterInfoV2 is the class which stores information about a cluster_info.
    You can use objects of this class to create/update/clone a cluster.
    """

    def __init__(self, label):
        """
        Args:
        `label`: A list of labels that identify the cluster. At least one label
            must be provided when creating a cluster.
        """
        self.cluster_info = {}
        self.cluster_info['label'] = label
        self.monitoring = {}
        self.internal = {} # right now not supported

    def set_cluster_info(self,
                         disallow_cluster_termination=None,
                         enable_ganglia_monitoring=None,
                         datadog_api_token=None,
                         datadog_app_token=None,
                         node_bootstrap=None,
                         master_instance_type=None,
                         slave_instance_type=None,
                         min_nodes=None,
                         max_nodes=None,
                         slave_request_type=None,
                         fallback_to_ondemand=None,
                         node_base_cooldown_period=None,
                         node_spot_cooldown_period=None,
                         custom_tags=None,
                         heterogeneous_config=None,
                         maximum_bid_price_percentage=None,
                         timeout_for_request=None,
                         maximum_spot_instance_percentage=None,
                         stable_maximum_bid_price_percentage=None,
                         stable_timeout_for_request=None,
                         stable_spot_fallback=None,
                         spot_block_duration=None,
                         idle_cluster_timeout=None,
                         disk_count=None,
                         disk_type=None,
                         disk_size=None,
                         root_disk_size=None,
                         upscaling_config=None,
                         enable_encryption=None,
                         customer_ssh_key=None,
                         cluster_name=None,
                         force_tunnel=None,
                         image_uri_overrides=None,
                         env_name=None,
                         python_version=None,
                         r_version=None,
                         disable_cluster_pause=None,
                         paused_cluster_timeout_mins=None,
                         disable_autoscale_node_pause=None,
                         paused_autoscale_node_timeout_mins=None):
        """
        Args:

                `disallow_cluster_termination`: Set this to True if you don't want
                    qubole to auto-terminate idle clusters. Use this option with
                    extreme caution.

                `enable_ganglia_monitoring`: Set this to True if you want to enable
                    ganglia monitoring for the cluster.

                `node_bootstrap`: name of the node bootstrap file for this
                    cluster. It should be in stored in S3 at
                    <your-default-location>/scripts/hadoop/

                `master_instance_type`: The instance type to use for the Hadoop master
                    node.

                `slave_instance_type`: The instance type to use for the Hadoop slave
                    nodes.

                `min_nodes`: Number of nodes to start the cluster with.

                `max_nodes`: Maximum number of nodes the cluster may be auto-scaled up
                    to.

                `slave_request_type`: Purchasing option for slave instances.
                    Valid values: "ondemand", "hybrid", "spot".

                `fallback_to_ondemand`: Fallback to on-demand nodes if spot nodes could not be
                    obtained. Valid only if slave_request_type is 'spot'.

                `node_base_cooldown_period`: Time for which an on-demand node waits before termination (Unit: minutes)

                `node_spot_cooldown_period`: Time for which a spot node waits before termination (Unit: minutes)

                `maximum_bid_price_percentage`: ( Valid only when `slave_request_type`
                    is hybrid or spot.) Maximum value to bid for spot
                    instances, expressed as a percentage of the base price
                    for the slave node instance type.

                `timeout_for_request`: Timeout for a spot instance request (Unit:
                    minutes)

                `maximum_spot_instance_percentage`: Maximum percentage of instances
                    that may be purchased from the AWS Spot market. Valid only when
                    slave_request_type is "hybrid".

                `stable_maximum_bid_price_percentage`: Maximum value to bid for stable node spot
                    instances, expressed as a percentage of the base price
                    (applies to both master and slave nodes).

                `stable_timeout_for_request`: Timeout for a stable node spot instance request (Unit:
                    minutes)

                `stable_spot_fallback`: Whether to fallback to on-demand instances for
                    stable nodes if spot instances are not available

                `spot_block_duration`: Time for which the spot block instance is provisioned (Unit:
                    minutes)

                `disk_count`: Number of EBS volumes to attach
                    to each instance of the cluster.

                `disk_type`: Type of the EBS volume. Valid
                    values are 'standard' (magnetic) and 'ssd'.

                `disk_size`: Size of each EBS volume, in GB.

                `root_disk_size`: Size of root volume, in GB.

                `enable_encryption`: Encrypt the ephemeral drives on the instance.

                `customer_ssh_key`: SSH key to use to login to the instances.

                `idle_cluster_timeout`: The buffer time (range in 0-6 hrs) after a cluster goes idle
                    and gets terminated,  given cluster auto termination is on and no cluster specific
                    timeout has been set (default is 2 hrs)

                `heterogeneous_config` : Configuring heterogeneous nodes in Hadoop 2 and Spark clusters.
                    It implies that slave nodes can be of different instance types

                `custom_tags` : Custom tags to be set on all instances
                    of the cluster. Specified as JSON object (key-value pairs)

                `datadog_api_token` : Specify the Datadog API token to use the Datadog monitoring service

                `datadog_app_token` : Specify the Datadog APP token to use the Datadog monitoring service

                `image_uri_overrides` : Override the image name provided

                `env_name`: Name of python and R environment. (For Spark clusters)

                `python_version`: Version of Python for environment. (For Spark clusters)

                `r_version`: Version of R for environment. (For Spark clusters)

                `disable_cluster_pause`: Disable cluster pause

                `paused_cluster_timeout_mins`: Paused cluster timeout in mins

                `disable_autoscale_node_pause`: Disable autoscale node pause

                `paused_autoscale_node_timeout_mins`: Paused autoscale node timeout in mins

        Doc: For getting details about arguments
        http://docs.qubole.com/en/latest/rest-api/cluster_api/create-new-cluster.html#parameters

        """
        self.cluster_info['master_instance_type'] = master_instance_type
        self.cluster_info['slave_instance_type'] = slave_instance_type
        self.cluster_info['min_nodes'] = min_nodes
        self.cluster_info['max_nodes'] = max_nodes
        self.cluster_info['cluster_name'] = cluster_name
        self.cluster_info['node_bootstrap'] = node_bootstrap
        self.cluster_info['disallow_cluster_termination'] = disallow_cluster_termination
        self.cluster_info['force_tunnel'] = force_tunnel
        self.cluster_info['fallback_to_ondemand'] = fallback_to_ondemand
        self.cluster_info['node_base_cooldown_period'] = node_base_cooldown_period
        self.cluster_info['node_spot_cooldown_period'] = node_spot_cooldown_period
        self.cluster_info['customer_ssh_key'] = customer_ssh_key
        if custom_tags and custom_tags.strip():
            try:
                self.cluster_info['custom_tags'] = json.loads(custom_tags.strip())
            except Exception as e:
                raise Exception("Invalid JSON string for custom ec2 tags: %s" % e.message)

        self.cluster_info['heterogeneous_config'] = heterogeneous_config
        self.cluster_info['slave_request_type'] = slave_request_type
        self.cluster_info['idle_cluster_timeout'] = idle_cluster_timeout
        self.cluster_info['spot_settings'] = {}

        self.cluster_info['rootdisk'] = {}
        self.cluster_info['rootdisk']['size'] = root_disk_size

        self.set_spot_instance_settings(maximum_bid_price_percentage, timeout_for_request, maximum_spot_instance_percentage)
        self.set_stable_spot_bid_settings(stable_maximum_bid_price_percentage, stable_timeout_for_request, stable_spot_fallback)
        self.set_spot_block_settings(spot_block_duration)
        self.set_data_disk(disk_size, disk_count, disk_type, upscaling_config, enable_encryption)
        self.set_monitoring(enable_ganglia_monitoring, datadog_api_token, datadog_app_token)
        self.set_internal(image_uri_overrides)
        self.set_env_settings(env_name, python_version, r_version)
        self.set_start_stop_settings(disable_cluster_pause, paused_cluster_timeout_mins,
                                     disable_autoscale_node_pause, paused_autoscale_node_timeout_mins)

    def set_datadog_setting(self,
                            datadog_api_token=None,
                            datadog_app_token=None):
        self.monitoring['datadog'] = {}
        self.monitoring['datadog']['datadog_api_token'] = datadog_api_token
        self.monitoring['datadog']['datadog_app_token'] = datadog_app_token

    def set_monitoring(self,
                       enable_ganglia_monitoring=None,
                       datadog_api_token=None,
                       datadog_app_token=None):
        self.monitoring['ganglia'] = enable_ganglia_monitoring
        self.set_datadog_setting(datadog_api_token, datadog_app_token)

    def set_spot_instance_settings(self,
                                   maximum_bid_price_percentage=None,
                                   timeout_for_request=None,
                                   maximum_spot_instance_percentage=None):
        self.cluster_info['spot_settings']['spot_instance_settings'] = {}
        self.cluster_info['spot_settings']['spot_instance_settings']['maximum_bid_price_percentage'] = \
            maximum_bid_price_percentage
        self.cluster_info['spot_settings']['spot_instance_settings']['timeout_for_request'] = timeout_for_request
        self.cluster_info['spot_settings']['spot_instance_settings']['maximum_spot_instance_percentage'] = \
            maximum_spot_instance_percentage

    def set_stable_spot_bid_settings(self,
                                     stable_maximum_bid_price_percentage=None,
                                     stable_timeout_for_request=None,
                                     stable_spot_fallback=None):
        self.cluster_info['spot_settings']['stable_spot_bid_settings'] = {}
        self.cluster_info['spot_settings']['stable_spot_bid_settings']['maximum_bid_price_percentage'] = \
            stable_maximum_bid_price_percentage
        self.cluster_info['spot_settings']['stable_spot_bid_settings']['timeout_for_request'] = \
            stable_timeout_for_request
        self.cluster_info['spot_settings']['stable_spot_bid_settings']['stable_spot_fallback'] = \
            stable_spot_fallback

    def set_spot_block_settings(self,
                                spot_block_duration=None):
        self.cluster_info['spot_settings']['spot_block_settings'] = {}
        self.cluster_info['spot_settings']['spot_block_settings']['duration'] = spot_block_duration

    def set_data_disk(self,
                      disk_size=None,
                      disk_count=None,
                      disk_type=None,
                      upscaling_config=None,
                      enable_encryption=None):
        self.cluster_info['datadisk'] = {}
        self.cluster_info['datadisk']['size'] = disk_size
        self.cluster_info['datadisk']['count'] = disk_count
        self.cluster_info['datadisk']['type'] = disk_type
        self.cluster_info['datadisk']['upscaling_config'] = upscaling_config
        self.cluster_info['datadisk']['encryption'] = enable_encryption

    def set_internal(self, image_uri_overrides=None):
        self.internal['image_uri_overrides'] = image_uri_overrides

    def set_env_settings(self, env_name=None, python_version=None, r_version=None):
        self.cluster_info['env_settings'] = {}
        self.cluster_info['env_settings']['name'] = env_name
        self.cluster_info['env_settings']['python_version'] = python_version
        self.cluster_info['env_settings']['r_version'] = r_version

    def set_start_stop_settings(self,
                                disable_cluster_pause=None,
                                paused_cluster_timeout_mins=None,
                                disable_autoscale_node_pause=None,
                                paused_autoscale_node_timeout_mins=None):
        if disable_cluster_pause is not None:
            disable_cluster_pause = int(disable_cluster_pause)
        self.cluster_info['disable_cluster_pause'] = disable_cluster_pause
        self.cluster_info['paused_cluster_timeout_mins'] = paused_cluster_timeout_mins
        if disable_autoscale_node_pause is not None:
            disable_autoscale_node_pause = int(disable_autoscale_node_pause)
        self.cluster_info['disable_autoscale_node_pause'] = disable_autoscale_node_pause
        self.cluster_info['paused_autoscale_node_timeout_mins'] = paused_autoscale_node_timeout_mins

    @staticmethod
    def list_info_parser(argparser, action):
        argparser.add_argument("--id", dest="cluster_id",
                               help="show cluster with this id")

        argparser.add_argument("--label", dest="label",
                               help="show cluster with this label")
        argparser.add_argument("--state", dest="state",
                               choices=['invalid', 'up', 'down', 'pending', 'terminating'],
                               help="State of the cluster")
        argparser.add_argument("--page", dest="page",
                               type=int,
                               help="Page number")
        argparser.add_argument("--per-page", dest="per_page",
                               type=int,
                               help="Number of clusters to be retrieved per page")

    @staticmethod
    def cluster_info_parser(argparser, action):
        create_required = False
        label_required = False
        if action == "create":
            create_required = True
        elif action == "update":
            argparser.add_argument("cluster_id_label",
                                   help="id/label of the cluster to update")
        elif action == "clone":
            argparser.add_argument("cluster_id_label",
                                   help="id/label of the cluster to update")
            label_required = True

        argparser.add_argument("--label", dest="label",
                               nargs="+", required=(create_required or label_required),
                               help="list of labels for the cluster" +
                                    " (atleast one label is required)")
        cluster_info = argparser.add_argument_group("cluster_info")
        cluster_info.add_argument("--master-instance-type",
                                  dest="master_instance_type",
                                  help="instance type to use for the hadoop" +
                                       " master node")
        cluster_info.add_argument("--slave-instance-type",
                                  dest="slave_instance_type",
                                  help="instance type to use for the hadoop" +
                                       " slave nodes")
        cluster_info.add_argument("--min-nodes",
                                  dest="initial_nodes",
                                  type=int,
                                  help="number of nodes to start the" +
                                       " cluster with", )
        cluster_info.add_argument("--max-nodes",
                                  dest="max_nodes",
                                  type=int,
                                  help="maximum number of nodes the cluster" +
                                       " may be auto-scaled up to")
        cluster_info.add_argument("--idle-cluster-timeout",
                                  dest="idle_cluster_timeout",
                                  help="cluster termination timeout for idle cluster")
        cluster_info.add_argument("--node-bootstrap-file",
                                  dest="node_bootstrap_file",
                                  help="""name of the node bootstrap file for this cluster. It
                                   should be in stored in S3 at
                                   <account-default-location>/scripts/hadoop/NODE_BOOTSTRAP_FILE
                                   """, )
        cluster_info.add_argument("--root-disk-size",
                                  dest="root_disk_size",
                                  type=int,
                                  help="size of the root volume in GB")
        termination = cluster_info.add_mutually_exclusive_group()
        termination.add_argument("--disallow-cluster-termination",
                                 dest="disallow_cluster_termination",
                                 action="store_true",
                                 default=None,
                                 help="don't auto-terminate idle clusters," +
                                      " use this with extreme caution", )
        termination.add_argument("--allow-cluster-termination",
                                 dest="disallow_cluster_termination",
                                 action="store_false",
                                 default=None,
                                 help="auto-terminate idle clusters,")
        fallback_to_ondemand_group = cluster_info.add_mutually_exclusive_group()
        fallback_to_ondemand_group.add_argument("--fallback-to-ondemand",
                                                dest="fallback_to_ondemand",
                                                action="store_true",
                                                default=None,
                                                help="Fallback to on-demand nodes if spot nodes" +
                                                     " could not be obtained. Valid only if slave_request_type is spot", )
        fallback_to_ondemand_group.add_argument("--no-fallback-to-ondemand",
                                                dest="fallback_to_ondemand",
                                                action="store_false",
                                                default=None,
                                                help="Dont Fallback to on-demand nodes if spot nodes" +
                                                     " could not be obtained. Valid only if slave_request_type is spot", )
        node_cooldown_period_group = argparser.add_argument_group("node cooldown period settings")
        node_cooldown_period_group.add_argument("--node-base-cooldown-period",
                                                dest="node_base_cooldown_period",
                                                type=int,
                                                help="Cooldown period for on-demand nodes" +
                                                     " unit: minutes")
        node_cooldown_period_group.add_argument("--node-spot-cooldown-period",
                                                dest="node_spot_cooldown_period",
                                                type=int,
                                                help="Cooldown period for spot nodes" +
                                                     " unit: minutes")
        cluster_info.add_argument("--customer-ssh-key",
                                  dest="customer_ssh_key_file",
                                  help="location for ssh key to use to" +
                                       " login to the instance")
        cluster_info.add_argument("--custom-tags",
                                  dest="custom_tags",
                                  help="""Custom tags to be set on all instances
                                                 of the cluster. Specified as JSON object (key-value pairs)
                                                 e.g. --custom-ec2-tags '{"key1":"value1", "key2":"value2"}'
                                                 """, )

        # datadisk settings
        datadisk_group = argparser.add_argument_group("data disk settings")
        datadisk_group.add_argument("--count",
                                    dest="count",
                                    type=int,
                                    help="Number of EBS volumes to attach to" +
                                         " each instance of the cluster", )
        datadisk_group.add_argument("--disk-type",
                                    dest="disk_type",
                                    choices=["standard", "gp2"],
                                    help="Type of the  volume attached to the instances. Valid values are " +
                                         "'standard' (magnetic) and 'gp2' (ssd).")
        datadisk_group.add_argument("--size",
                                    dest="size",
                                    type=int,
                                    help="Size of each EBS volume, in GB", )
        datadisk_group.add_argument("--upscaling-config",
                                    dest="upscaling_config",
                                    help="Upscaling config to be attached with the instances.", )
        ephemerals = datadisk_group.add_mutually_exclusive_group()
        ephemerals.add_argument("--encrypted-ephemerals",
                                dest="encrypted_ephemerals",
                                action="store_true",
                                default=None,
                                help="encrypt the ephemeral drives on" +
                                     " the instance", )
        ephemerals.add_argument("--no-encrypted-ephemerals",
                                dest="encrypted_ephemerals",
                                action="store_false",
                                default=None,
                                help="don't encrypt the ephemeral drives on" +
                                     " the instance", )

        cluster_info.add_argument("--heterogeneous-config",
                                  dest="heterogeneous_config",
                                  help="heterogeneous config for the cluster")

        cluster_info.add_argument("--slave-request-type",
                                  dest="slave_request_type",
                                  choices=["ondemand", "spot", "hybrid", "spotblock"],
                                  help="purchasing option for slave instaces", )

        # spot settings
        spot_instance_group = argparser.add_argument_group("spot instance settings" +
                                                           " (valid only when slave-request-type is hybrid or spot)")
        spot_instance_group.add_argument("--maximum-bid-price-percentage",
                                         dest="maximum_bid_price_percentage",
                                         type=float,
                                         help="maximum value to bid for spot instances" +
                                              " expressed as a percentage of the base" +
                                              " price for the slave node instance type", )
        spot_instance_group.add_argument("--timeout-for-spot-request",
                                         dest="timeout_for_request",
                                         type=int,
                                         help="timeout for a spot instance request" +
                                              " unit: minutes")
        spot_instance_group.add_argument("--maximum-spot-instance-percentage",
                                         dest="maximum_spot_instance_percentage",
                                         type=int,
                                         help="maximum percentage of instances that may" +
                                              " be purchased from the aws spot market," +
                                              " valid only when slave-request-type" +
                                              " is 'hybrid'", )

        stable_spot_group = argparser.add_argument_group("stable spot instance settings")
        stable_spot_group.add_argument("--stable-maximum-bid-price-percentage",
                                       dest="stable_maximum_bid_price_percentage",
                                       type=float,
                                       help="maximum value to bid for stable node spot instances" +
                                            " expressed as a percentage of the base" +
                                            " price for the master and slave node instance types", )
        stable_spot_group.add_argument("--stable-timeout-for-spot-request",
                                       dest="stable_timeout_for_request",
                                       type=int,
                                       help="timeout for a stable node spot instance request" +
                                            " unit: minutes")
        stable_spot_group.add_argument("--stable-allow-fallback",
                                       dest="stable_spot_fallback", default=None,
                                       type=str2bool,
                                       help="whether to fallback to on-demand instances for stable nodes" +
                                            " if spot instances aren't available")

        spot_block_group = argparser.add_argument_group("spot block settings")
        spot_block_group.add_argument("--spot-block-duration",
                                      dest="spot_block_duration",
                                      type=int,
                                      help="spot block duration" +
                                           " unit: minutes")

        # monitoring settings
        monitoring_group = argparser.add_argument_group("monitoring settings")
        ganglia = monitoring_group.add_mutually_exclusive_group()
        ganglia.add_argument("--enable-ganglia-monitoring",
                             dest="enable_ganglia_monitoring",
                             action="store_true",
                             default=None,
                             help="enable ganglia monitoring for the" +
                                  " cluster", )
        ganglia.add_argument("--disable-ganglia-monitoring",
                             dest="enable_ganglia_monitoring",
                             action="store_false",
                             default=None,
                             help="disable ganglia monitoring for the" +
                                  " cluster", )

        datadog_group = argparser.add_argument_group("datadog settings")
        datadog_group.add_argument("--datadog-api-token",
                                   dest="datadog_api_token",
                                   default=None,
                                   help="fernet key for airflow cluster", )
        datadog_group.add_argument("--datadog-app-token",
                                   dest="datadog_app_token",
                                   default=None,
                                   help="overrides for airflow cluster", )

        internal_group = argparser.add_argument_group("internal settings")
        internal_group.add_argument("--image-overrides",
                                    dest="image_uri_overrides",
                                    default=None,
                                    help="overrides for image", )

        env_group = argparser.add_argument_group("environment settings")
        env_group.add_argument("--env-name",
                               dest="env_name",
                               default=None,
                               help="name of Python and R environment")
        env_group.add_argument("--python-version",
                               dest="python_version",
                               default=None,
                               help="version of Python in environment")
        env_group.add_argument("--r-version",
                               dest="r_version",
                               default=None,
                               help="version of R in environment")

        start_stop_group = argparser.add_argument_group("start stop settings")
        start_stop_group.add_argument("--disable-cluster-pause",
                                      dest="disable_cluster_pause",
                                      action='store_true',
                                      default=None,
                                      help="disable cluster pause")
        start_stop_group.add_argument("--no-disable-cluster-pause",
                                      dest="disable_cluster_pause",
                                      action='store_false',
                                      default=None,
                                      help="disable cluster pause")
        start_stop_group.add_argument("--paused-cluster-timeout",
                                      dest="paused_cluster_timeout_mins",
                                      default=None,
                                      type=int,
                                      help="paused cluster timeout in min")
        start_stop_group.add_argument("--disable-autoscale-node-pause",
                                      dest="disable_autoscale_node_pause",
                                      action='store_true',
                                      default=None,
                                      help="disable autoscale node pause")
        start_stop_group.add_argument("--no-disable-autoscale-node-pause",
                                      dest="disable_autoscale_node_pause",
                                      action='store_false',
                                      default=None,
                                      help="disable autoscale node pause")
        start_stop_group.add_argument("--paused-autoscale-node-timeout",
                                      dest="paused_autoscale_node_timeout_mins",
                                      default=None,
                                      type=int,
                                      help="paused autoscale node timeout in min")

class ClusterV2(Resource):

    rest_entity_path = "clusters"

    @classmethod
    def create(cls, cluster_info):
        """
        Create a new cluster using information provided in `cluster_info`.
        """
        conn = Qubole.agent(version="v2")
        return conn.post(cls.rest_entity_path, data=cluster_info)

    @classmethod
    def update(cls, cluster_id_label, cluster_info):
        """
        Update the cluster with id/label `cluster_id_label` using information provided in
        `cluster_info`.
        """
        conn = Qubole.agent(version="v2")
        return conn.put(cls.element_path(cluster_id_label), data=cluster_info)

    @classmethod
    def clone(cls, cluster_id_label, cluster_info):
        """
        Update the cluster with id/label `cluster_id_label` using information provided in
        `cluster_info`.
        """
        conn = Qubole.agent(version="v2")
        return conn.post(cls.element_path(cluster_id_label) + '/clone', data=cluster_info)

    @classmethod
    def list(cls, label=None, cluster_id=None, state=None, page=None, per_page=None):
        """
        List existing clusters present in your account.

        Kwargs:
            `state`: list only those clusters which are in this state
            `page`: page number
            `per_page`: number of clusters to be retrieved per page

        Returns:
            List of clusters satisfying the given criteria
        """
        if cluster_id is not None:
            return cls.show(cluster_id)
        if label is not None:
            return cls.show(label)
        params = {}
        if page:
            params['page'] = page
        if per_page:
            params['per_page'] = per_page
        params = None if not params else params
        conn = Qubole.agent(version="v2")
        cluster_list = conn.get(cls.rest_entity_path)
        if state is None:
            # return the complete list since state is None
            return conn.get(cls.rest_entity_path, params=params)
        # filter clusters based on state
        result = []
        if 'clusters' in cluster_list:
            for cluster in cluster_list['clusters']:
                if state.lower() == cluster['state'].lower():
                    result.append(cluster)
        return result

    @classmethod
    def show(cls, cluster_id_label):
        """
        Show information about the cluster with id/label `cluster_id_label`.
        """
        conn = Qubole.agent(version="v2")
        return conn.get(cls.element_path(cluster_id_label))
