from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from qds_sdk.cloud.cloud import Cloud
from qds_sdk.engine import Engine
import argparse

import sys

def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")

class ClusterCmdLine:

    @staticmethod
    def parsers():
        cloud = Qubole.cloud
        argparser = argparse.ArgumentParser(
            prog="qds.py cluster",
            description="Cluster Operations for Qubole Data Service.")
        subparsers = argparser.add_subparsers(title="Cluster operations")

        create = subparsers.add_parser("create", help="Create a new cluster")
        ClusterCmdLine.create_update_clone_arguments(create, action="create")
        #create.set_defaults(func=ClusterCmdLine.create_update, action="update")

        update = subparsers.add_parser("update", help="Update the settings of an existing cluster")
        ClusterCmdLine.create_update_clone_arguments(update, action="update") #check this

        update = subparsers.add_parser("clone", help="Clone a cluster from an existing one")
        ClusterCmdLine.create_update_clone_arguments(update, action="update") # check this

        return argparser


    @staticmethod
    def create_update_clone_arguments(subparser, action=None):

        create_required = False
        label_required = False
        if action == "create":
            create_required = True
        elif action == "update":
            subparser.add_argument("cluster_id_label",
                                   help="id/label of the cluster to update")
        elif action == "clone":
            subparser.add_argument("cluster_id_label",
                                   help="id/label of the cluster to update")
            label_required = True

        # cloud config settings
        Cloud.get_cloud_object().cloud_config_parser(subparser)

        # cluster info settings
        # add this in cluster info
        subparser.add_argument("--label", dest="label",
                               nargs="+", required=(create_required or label_required),
                               help="list of labels for the cluster" +
                                    " (atleast one label is required)")
        cluster_info = subparser.add_argument_group("cluster_info")
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
                                       " cluster with",)
        cluster_info.add_argument("--max-nodes",
                                  dest="max_nodes",
                                  type=int,
                                  help="maximum number of nodes the cluster" +
                                       " may be auto-scaled up to")
        cluster_info.add_argument("--idle-cluster-timeout",
                                  dest = "idle_cluster_timeout",
                                  help = "cluster termination timeout for idle cluster")
        cluster_info.add_argument("--node-bootstrap-file",
                      dest="node_bootstrap_file",
                      help="""name of the node bootstrap file for this cluster. It
                           should be in stored in S3 at
                           <account-default-location>/scripts/hadoop/NODE_BOOTSTRAP_FILE
                           """, )
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
        cluster_info.add_argument("--customer-ssh-key",
                                  dest="customer_ssh_key_file",
                                  help="location for ssh key to use to" +
                                       " login to the instance")
        cluster_info.add_argument("--custom-ec2-tags",
                                  dest = "custom_ec2_tags",
                                  help = """Custom ec2 tags to be set on all instances
                                         of the cluster. Specified as JSON object (key-value pairs)
                                         e.g. --custom-ec2-tags '{"key1":"value1", "key2":"value2"}'
                                         """, )

        # datadisk settings
        datadisk_group = subparser.add_argument_group("data disk settings")
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
                      choices=["ondemand", "spot", "hybrid"],
                      help="purchasing option for slave instaces", )

        # spot settings

        spot_instance_group = subparser.add_argument_group("spot instance settings" +
                    " (valid only when slave-request-type is hybrid or spot)")
        spot_instance_group.add_argument("--maximum-bid-price-percentage",
                                dest="maximum_bid_price_percentage",
                                type=float,
                                help="maximum value to bid for spot instances" +
                                     " expressed as a percentage of the base" +
                                     " price for the slave node instance type",)
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


        stable_spot_group = subparser.add_argument_group("stable spot instance settings")
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
                                       dest="stable_allow_fallback", default=None,
                                       type=str2bool,
                                       help="whether to fallback to on-demand instances for stable nodes" +
                                            " if spot instances aren't available")
        # monitoring settings
        monitoring_group  = subparser.add_argument_group("monitoring settings")
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

        datadog_group = subparser.add_argument_group("datadog settings")
        datadog_group.add_argument("--datadog-api-token",
                                   dest="datadog_api_token",
                                   default=None,
                                   help="fernet key for airflow cluster", )
        datadog_group.add_argument("--datadog-app-token",
                                   dest="datadog_app_token",
                                   default=None,
                                   help="overrides for airflow cluster", )

        # engine settings
        Engine().engine_parser(subparser)



    @staticmethod
    def run(args):
        parser = ClusterCmdLine.parsers()
        arguments = parser.parse_args(args)
        print ("cluster v2 argumetns====")
        print (arguments)
        custom_config = ClusterCmdLine._read_file(arguments.custom_hadoop_config_file, "custom config file")
        presto_custom_config = ClusterCmdLine._read_file(arguments.presto_custom_config_file, "presto custom config file")
        fairscheduler_config_xml = ClusterCmdLine._read_file(arguments.fairscheduler_config_xml_file, "config xml file")
        customer_ssh_key = ClusterCmdLine._read_file(arguments.customer_ssh_key_file, "customer ssh key file")

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
                                      custom_tags=arguments.custom_tags,
                                      heterogeneous_config=arguments.heterogeneous_config,
                                      maximum_bid_price_percentage=arguments.maximum_bid_price_percentage,
                                      timeout_for_request=arguments.timeout_for_request,
                                      maximum_spot_instance_percentage=arguments.maximum_spot_instance_percentage,
                                      stable_maximum_bid_price_percentage=arguments.stable_maximum_bid_price_percentage,
                                      stable_timeout_for_request=arguments.stable_timeout_for_request,
                                      stable_allow_fallback=arguments.stable_allow_fallback,
                                      idle_cluster_timeout=arguments.idle_cluster_timeout,
                                      disk_count=arguments.count,
                                      disk_type=arguments.disk_type,
                                      disk_size=arguments.size,
                                      upscaling_config=arguments.upscaling_config,
                                      enable_encryption=arguments.encrypted_ephemerals,
                                      customer_ssh_key=customer_ssh_key
                                      )

        cloud_config = Cloud.get_cloud_object()
        cloud_config.get_cloud_config(compute_access_key=arguments.compute_access_key,
                                      compute_secret_key=arguments.compute_secret_key,
                                      compute_client_id=arguments.compute_client_id,
                                      compute_client_secret=arguments.compute_client_secret,
                                      compute_subscription_id=arguments.compute_subscription_id,
                                      compute_tenant_id=arguments.compute_tenant_id,
                                      compute_user_id=arguments.compute_user_id,
                                      compute_key_finger_print=arguments.compute_key_finger_print,
                                      compute_api_private_rsa_key=arguments.compute_api_private_rsa_key,
                                      use_account_compute_creds=arguments.use_account_compute_creds,
                                      location=arguments.location,
                                      aws_region=arguments.aws_region,
                                      aws_availability_zone=arguments.aws_availability_zone,
                                      storage_access_key=arguments.storage_access_key,
                                      storage_account_name=arguments.storage_account_name,
                                      disk_storage_account_name=arguments.disk_storage_account_name,
                                      disk_storage_account_resource_group_name=arguments.disk_storage_account_resource_group_name,
                                      role_instance_profile=arguments.role_instance_profile,
                                      vpc_id=arguments.vpc_id,
                                      subnet_id=arguments.subnet_id,
                                      persistent_security_groups=arguments.persistent_security_groups,
                                      bastion_node_public_dns=arguments.bastion_node_public_dns,
                                      vnet_name=arguments.vnet_name,
                                      subnet_name=arguments.subnet_name,
                                      vnet_resource_group_name=arguments.vnet_resource_group_name,
                                      master_elastic_ip=arguments.master_elastic_ip,
                                      oracle_region=arguments.region,
                                      oracle_availability_domain=arguments.availability_domain,
                                      compartment_id=arguments.compartment_id,
                                      image_id=arguments.image_id,
                                      vcn_id=arguments.vcn_id,
                                      storage_tenant_id= storage_tenant_id
                                      storage_user_id=

                                    storage_key_finger_print



                                      )
        engine_config = Engine(flavour=arguments.flavour)






        print ("parsed cluster ======")
        print (arguments)


    @staticmethod
    def create_update():
        pass

    @staticmethod
    def _read_file(file_path, file_name):
        file_content = None
        if file_path is not None:
            try:
                file_content = open(file_path).read()
            except IOError as e:
                sys.stderr.write("Unable to read %s: %s\n" % (file_name, str(e)))
        return file_content


class ClusterInfoV2(object):
    def __init__(self, label):
        self.label = label
        self.cluster_info = {}
        self.monitoring = {}
        self.internal = {} # not added right now from command line

    def set_cluster_info(self):
        pass








class ClusterV2(Resource):

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
        print ("data===")
        print (cluster_info)
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







