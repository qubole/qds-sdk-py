import json

from qds_sdk import util
from qds_sdk.qubole import Qubole


def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")


class ClusterInfoV22(object):
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
        self.cluster_info = {'label': label}
        self.monitoring = {}
        self.internal = {}  # right now not supported

    def set_cluster_info_from_arguments(self, arguments):
        customer_ssh_key = util._read_file(arguments.customer_ssh_key_file)
        self.set_cluster_info(disallow_cluster_termination=arguments.disallow_cluster_termination,
                              enable_ganglia_monitoring=arguments.enable_ganglia_monitoring,
                              datadog_api_token=arguments.datadog_api_token,
                              datadog_app_token=arguments.datadog_app_token,
                              node_bootstrap=arguments.node_bootstrap_file,
                              master_instance_type=arguments.master_instance_type,
                              slave_instance_type=arguments.slave_instance_type,
                              min_nodes=arguments.initial_nodes,
                              max_nodes=arguments.max_nodes,
                              node_base_cooldown_period=arguments.node_base_cooldown_period,
                              node_spot_cooldown_period=arguments.node_spot_cooldown_period,
                              custom_tags=arguments.custom_tags,
                              heterogeneous_config=arguments.heterogeneous_config,
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
                              paused_autoscale_node_timeout_mins=arguments.paused_autoscale_node_timeout_mins,
                              parent_cluster_id=arguments.parent_cluster_id,
                              image_version=arguments.image_version)
        if Qubole.get_cloud_name() == "aws":
            # Need to move to aws cloud.
            self.set_composition(
                master_type=arguments.master_type,
                master_spot_block_duration=arguments.master_spot_block_duration,
                master_maximum_bid_price_percentage=arguments.master_maximum_bid_price_percentage,
                master_timeout_for_request=arguments.master_timeout_for_request,
                master_spot_fallback=arguments.master_spot_fallback,
                min_ondemand_percentage=arguments.min_ondemand_percentage,
                min_spot_block_percentage=arguments.min_spot_block_percentage,
                min_spot_block_duration=arguments.min_spot_block_duration,
                min_spot_percentage=arguments.min_spot_percentage,
                min_maximum_bid_price_percentage=arguments.min_maximum_bid_price_percentage,
                min_timeout_for_request=arguments.min_timeout_for_request,
                min_spot_allocation_strategy=arguments.min_spot_allocation_strategy,
                min_spot_fallback=arguments.min_spot_fallback,
                autoscaling_ondemand_percentage=arguments.autoscaling_ondemand_percentage,
                autoscaling_spot_block_percentage=arguments.autoscaling_spot_block_percentage,
                autoscaling_spot_percentage=arguments.autoscaling_spot_percentage,
                autoscaling_spot_block_duration=arguments.autoscaling_spot_block_duration,
                autoscaling_maximum_bid_price_percentage=arguments.autoscaling_maximum_bid_price_percentage,
                autoscaling_timeout_for_request=arguments.autoscaling_timeout_for_request,
                autoscaling_spot_allocation_strategy=arguments.autoscaling_spot_allocation_strategy,
                autoscaling_spot_fallback=arguments.autoscaling_spot_fallback,
                autoscaling_spot_block_fallback=arguments.autoscaling_spot_block_fallback)
        else:
            self.set_composition_from_cloud_using_parser(arguments)

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
                         node_base_cooldown_period=None,
                         node_spot_cooldown_period=None,
                         custom_tags=None,
                         heterogeneous_config=None,
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
                         paused_autoscale_node_timeout_mins=None,
                         parent_cluster_id=None,
                         image_version=None):
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

            `node_base_cooldown_period`: Time for which an on-demand node waits before termination (Unit: minutes)

            `node_spot_cooldown_period`: Time for which a spot node waits before termination (Unit: minutes)

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

            `parent_cluster_id`: parent cluster id for HS2 cluster

            `image_version`: cluster image version

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
        self.cluster_info['node_base_cooldown_period'] = node_base_cooldown_period
        self.cluster_info['node_volatile_cooldown_period'] = node_spot_cooldown_period
        self.cluster_info['customer_ssh_key'] = customer_ssh_key
        if custom_tags and custom_tags.strip():
            try:
                self.cluster_info['custom_tags'] = json.loads(custom_tags.strip())
            except Exception as e:
                raise Exception(
                    "Invalid JSON string for custom ec2 tags: %s" % e.message)

        self.cluster_info['heterogeneous_config'] = heterogeneous_config
        self.cluster_info['idle_cluster_timeout'] = idle_cluster_timeout
        self.cluster_info['rootdisk'] = {}
        self.cluster_info['rootdisk']['size'] = root_disk_size
        self.cluster_info['parent_cluster_id'] = parent_cluster_id
        self.cluster_info['cluster_image_version'] = image_version
        self.set_data_disk(disk_size, disk_count, disk_type,
                           upscaling_config, enable_encryption)
        self.set_monitoring(enable_ganglia_monitoring,
                            datadog_api_token, datadog_app_token)
        self.set_internal(image_uri_overrides)
        self.set_env_settings(env_name, python_version, r_version)
        self.set_start_stop_settings(disable_cluster_pause, paused_cluster_timeout_mins,
                                     disable_autoscale_node_pause, paused_autoscale_node_timeout_mins)

    def set_composition_from_cloud_using_parser(self, arguments):
        self.set_composition_for_cluster(**{k: v for k, v in arguments.__dict__.items()
                                            if v is not None})

    def set_composition_for_cluster(self, **kwargs):
        cloud = Qubole.get_cloud()
        composition = cloud.get_composition(**kwargs)
        if composition is not None:
            self.cluster_info["composition"] = composition

    def set_composition(self,
                        master_type="ondemand",
                        master_spot_block_duration=None,
                        master_maximum_bid_price_percentage=None,
                        master_timeout_for_request=None,
                        master_spot_fallback=None,
                        min_ondemand_percentage=None,
                        min_spot_block_percentage=None,
                        min_spot_block_duration=None,
                        min_spot_percentage=None,
                        min_maximum_bid_price_percentage=None,
                        min_timeout_for_request=None,
                        min_spot_allocation_strategy=None,
                        min_spot_fallback=None,
                        autoscaling_ondemand_percentage=None,
                        autoscaling_spot_block_percentage=None,
                        autoscaling_spot_percentage=None,
                        autoscaling_spot_block_duration=None,
                        autoscaling_maximum_bid_price_percentage=None,
                        autoscaling_timeout_for_request=None,
                        autoscaling_spot_allocation_strategy=None,
                        autoscaling_spot_fallback=None,
                        autoscaling_spot_block_fallback=None):

        self.cluster_info["composition"] = {}

        self.set_master_config(master_type,
                               master_spot_block_duration,
                               master_maximum_bid_price_percentage,
                               master_timeout_for_request,
                               master_spot_fallback)

        self.set_min_config(min_ondemand_percentage,
                            min_spot_block_percentage,
                            min_spot_block_duration,
                            min_spot_percentage,
                            min_maximum_bid_price_percentage,
                            min_timeout_for_request,
                            min_spot_allocation_strategy,
                            min_spot_fallback)

        self.set_autoscaling_config(autoscaling_ondemand_percentage,
                                    autoscaling_spot_block_percentage,
                                    autoscaling_spot_block_duration,
                                    autoscaling_spot_percentage,
                                    autoscaling_maximum_bid_price_percentage,
                                    autoscaling_timeout_for_request,
                                    autoscaling_spot_allocation_strategy,
                                    autoscaling_spot_fallback,
                                    autoscaling_spot_block_fallback)

    def set_master_config(self,
                          master_type,
                          master_spot_block_duration,
                          master_maximum_bid_price_percentage,
                          master_timeout_for_request,
                          master_spot_fallback):
        self.cluster_info["composition"]["master"] = {"nodes": []}
        if master_type == "ondemand":
            self.set_master_ondemand(100)
        elif master_type == "spot":
            self.set_master_spot(100, master_maximum_bid_price_percentage,
                                 master_timeout_for_request, master_spot_fallback)
        elif master_type == "spotblock":
            self.set_master_spot_block(
                100, master_spot_block_duration)

    def set_min_config(self,
                       min_ondemand_percentage,
                       min_spot_block_percentage,
                       min_spot_block_duration,
                       min_spot_percentage,
                       min_maximum_bid_price_percentage,
                       min_timeout_for_request,
                       min_spot_allocation_strategy,
                       min_spot_fallback):
        self.cluster_info["composition"]["min_nodes"] = {"nodes": []}
        if not min_ondemand_percentage and not min_spot_block_percentage and not min_spot_percentage:
            self.set_min_ondemand(100)
        else:
            if min_ondemand_percentage:
                self.set_min_ondemand(min_ondemand_percentage)
            if min_spot_block_percentage:
                self.set_min_spot_block(
                    min_spot_block_percentage, min_spot_block_duration)
            if min_spot_percentage:
                self.set_min_spot(min_spot_percentage, min_maximum_bid_price_percentage,
                                  min_timeout_for_request, min_spot_allocation_strategy,
                                  min_spot_fallback)

    def set_autoscaling_config(self,
                               autoscaling_ondemand_percentage,
                               autoscaling_spot_block_percentage,
                               autoscaling_spot_block_duration,
                               autoscaling_spot_percentage,
                               autoscaling_maximum_bid_price_percentage,
                               autoscaling_timeout_for_request,
                               autoscaling_spot_allocation_strategy,
                               autoscaling_spot_fallback,
                               autoscaling_spot_block_fallback):
        self.cluster_info["composition"]["autoscaling_nodes"] = {"nodes": []}
        if not autoscaling_ondemand_percentage and not autoscaling_spot_block_percentage and not autoscaling_spot_percentage:
            self.set_autoscaling_ondemand(50)
            self.set_autoscaling_spot(50, 100, 1, None, 'ondemand')
        else:
            if autoscaling_ondemand_percentage:
                self.set_autoscaling_ondemand(autoscaling_ondemand_percentage)
            if autoscaling_spot_block_percentage:
                self.set_autoscaling_spot_block(autoscaling_spot_block_percentage,
                                                autoscaling_spot_block_duration,
                                                autoscaling_spot_block_fallback)
            if autoscaling_spot_percentage:
                self.set_autoscaling_spot(autoscaling_spot_percentage,
                                          autoscaling_maximum_bid_price_percentage,
                                          autoscaling_timeout_for_request,
                                          autoscaling_spot_allocation_strategy,
                                          autoscaling_spot_fallback)

    def set_master_ondemand(self, master_ondemand_percentage=None):
        ondemand = {"percentage": master_ondemand_percentage, "type": "ondemand"}
        self.cluster_info["composition"]["master"]["nodes"].append(ondemand)

    def set_master_spot_block(self, master_spot_block_percentage=None, master_spot_block_duration=120):
        spot_block = {"percentage": master_spot_block_percentage,
                      "type": "spotblock",
                      "timeout": master_spot_block_duration}
        self.cluster_info["composition"]["master"]["nodes"].append(spot_block)

    def set_master_spot(self, master_spot_percentage=None, master_maximum_bid_price_percentage=100,
                        master_timeout_for_request=1, master_spot_fallback=None):
        spot = {"percentage": master_spot_percentage,
                "type": "spot",
                "maximum_bid_price_percentage": master_maximum_bid_price_percentage,
                "timeout_for_request": master_timeout_for_request,
                "fallback": master_spot_fallback
                }
        self.cluster_info["composition"]["master"]["nodes"].append(spot)

    def set_min_ondemand(self, min_ondemand_percentage=None):
        ondemand = {"percentage": min_ondemand_percentage, "type": "ondemand"}
        self.cluster_info["composition"]["min_nodes"]["nodes"].append(ondemand)

    def set_min_spot_block(self, min_spot_block_percentage=None, min_spot_block_duration=120):
        spot_block = {"percentage": min_spot_block_percentage,
                      "type": "spotblock",
                      "timeout": min_spot_block_duration}
        self.cluster_info["composition"]["min_nodes"]["nodes"].append(spot_block)

    def set_min_spot(self, min_spot_percentage=None, min_maximum_bid_price_percentage=100,
                     min_timeout_for_request=1, min_spot_allocation_strategy=None,
                     min_spot_fallback=None):
        spot = {"percentage": min_spot_percentage,
                "type": "spot",
                "maximum_bid_price_percentage": min_maximum_bid_price_percentage,
                "timeout_for_request": min_timeout_for_request,
                "allocation_strategy": min_spot_allocation_strategy,
                "fallback": min_spot_fallback
                }
        self.cluster_info["composition"]["min_nodes"]["nodes"].append(spot)

    def set_autoscaling_ondemand(self, autoscaling_ondemand_percentage=None):
        ondemand = {
            "percentage": autoscaling_ondemand_percentage, "type": "ondemand"}
        self.cluster_info["composition"]["autoscaling_nodes"]["nodes"].append(ondemand)

    def set_autoscaling_spot_block(self, autoscaling_spot_block_percentage=None,
                                   autoscaling_spot_block_duration=120,
                                   autoscaling_spot_block_fallback=None):
        spot_block = {"percentage": autoscaling_spot_block_percentage,
                      "type": "spotblock",
                      "timeout": autoscaling_spot_block_duration,
                      "fallback": autoscaling_spot_block_fallback}
        self.cluster_info["composition"]["autoscaling_nodes"]["nodes"].append(spot_block)

    def set_autoscaling_spot(self, autoscaling_spot_percentage=None,
                             autoscaling_maximum_bid_price_percentage=100,
                             autoscaling_timeout_for_request=1,
                             autoscaling_spot_allocation_strategy=None,
                             autoscaling_spot_fallback=None):
        spot = {"percentage": autoscaling_spot_percentage,
                "type": "spot",
                "maximum_bid_price_percentage": autoscaling_maximum_bid_price_percentage,
                "timeout_for_request": autoscaling_timeout_for_request,
                "allocation_strategy": autoscaling_spot_allocation_strategy,
                "fallback": autoscaling_spot_fallback
                }
        self.cluster_info["composition"]["autoscaling_nodes"]["nodes"].append(spot)

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
                               choices=['invalid', 'up', 'down',
                                        'pending', 'terminating'],
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
        cluster_info.add_argument("--parent-cluster-id",
                                  dest="parent_cluster_id",
                                  type=int,
                                  help="Id of the parent cluster this hs2 cluster is attached to")
        cluster_info.add_argument("--image-version",
                                  dest="image_version",
                                  help="cluster image version")
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

        node_cooldown_period_group = argparser.add_argument_group(
            "node cooldown period settings")
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

        composition_group = argparser.add_argument_group("Cluster composition settings")
        Qubole.get_cloud().set_composition_arguments(composition_group)

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
