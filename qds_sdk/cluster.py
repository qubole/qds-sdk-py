"""
The cluster module contains the definitions for retrieving and manipulating
cluster information.
"""

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser

import logging
import json

log = logging.getLogger("qds_cluster")


def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")


class Cluster(Resource):
    """
    qds_sdk.Cluster is the class for retrieving and manipulating cluster
    information.
    """

    rest_entity_path = "clusters"
    api_version = "v1.2"

    @classmethod
    def _parse_list(cls, args):
        """
        Parse command line arguments to construct a dictionary of cluster
        parameters that can be used to determine which clusters to list.

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used to determine which clusters to list
        """
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
    def list(cls, state=None, per_page=None):
        """
        List existing clusters present in your account.

        Kwargs:
            `state`: list only those clusters which are in this state

        Returns:
            List of clusters satisfying the given criteria
        """
        conn = Qubole.agent(version=Cluster.api_version)
        params = {"per_page": per_page, "state": state}
        if state is None:
            return conn.get(cls.rest_entity_path, params=params)
        elif state is not None:
            cluster_list = conn.get(cls.rest_entity_path, params=params)
            result = []
            if Cluster.api_version == 'v1.2':
                for cluster in cluster_list:
                    current_state = cluster['cluster']['state']
                    if state.lower() == current_state.lower():
                        result.append(cluster)

            else:
                cluster_list = cluster_list['clusters']
                for cluster in cluster_list:
                    current_state = cluster['state']
                    if state.lower() == current_state.lower():
                        result.append(cluster)
            return result

    @classmethod
    def show(cls, cluster_id_label):
        """
        Show information about the cluster with id/label `cluster_id_label`.
        """
        conn = Qubole.agent(version=Cluster.api_version)
        return conn.get(cls.element_path(cluster_id_label))

    @classmethod
    def status(cls, cluster_id_label):
        """
        Show the status of the cluster with id/label `cluster_id_label`.
        """
        conn = Qubole.agent(version=Cluster.api_version)
        return conn.get(cls.element_path(cluster_id_label) + "/state")

    @classmethod
    def start(cls, cluster_id_label):
        """
        Start the cluster with id/label `cluster_id_label`.
        """
        conn = Qubole.agent(version=Cluster.api_version)
        data = {"state": "start"}
        return conn.put(cls.element_path(cluster_id_label) + "/state", data)

    @classmethod
    def terminate(cls, cluster_id_label):
        """
        Terminate the cluster with id/label `cluster_id_label`.
        """
        conn = Qubole.agent(version=Cluster.api_version)
        data = {"state": "terminate"}
        return conn.put(cls.element_path(cluster_id_label) + "/state", data)

    @classmethod
    def instance_info(cls, region=None, cluster_type=None):
       """ Returns the possible instance type information for master and slave of the cluster
       """
       conn = Qubole.agent(version=Cluster.api_version)
       params = {"type": cluster_type, "region": region}
       return conn.get(cls.rest_entity_path + "/instance_info", params=params)

    @classmethod
    def _parse_create_update(cls, args, action, api_version):
        """
        Parse command line arguments to determine cluster parameters that can
        be used to create or update a cluster.

        Args:
            `args`: sequence of arguments

            `action`: "create", "update" or "clone"

        Returns:
            Object that contains cluster parameters
        """
        argparser = ArgumentParser(prog="cluster %s" % action)

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

        # set of parameters common for all apis
        hadoop_group = argparser.add_argument_group("hadoop settings")
        if (api_version == 1.3):
            node_config_group = argparser.add_argument_group("node configuration")
        elif api_version == 2.0:
            node_config_group = argparser.add_argument_group("cluster_info")
        else:
            node_config_group = hadoop_group
        argparser.add_argument("--label", dest="label",
                               nargs="+", required=(create_required or label_required),
                               help="list of labels for the cluster" +
                                    " (atleast one label is required)")

        node_config_group.add_argument("--master-instance-type",
                                       dest="master_instance_type",
                                       help="instance type to use for the hadoop" +
                                            " master node", )
        node_config_group.add_argument("--slave-instance-type",
                                       dest="slave_instance_type",
                                       help="instance type to use for the hadoop" +
                                            " slave nodes", )
        node_config_group.add_argument("--initial-nodes",
                                       dest="initial_nodes",
                                       type=int,
                                       help="number of nodes to start the" +
                                            " cluster with", )
        node_config_group.add_argument("--max-nodes",
                                       dest="max_nodes",
                                       type=int,
                                       help="maximum number of nodes the cluster" +
                                            " may be auto-scaled up to")
        node_config_group.add_argument("--slave-request-type",
                                       dest="slave_request_type",
                                       choices=["ondemand", "spot", "hybrid"],
                                       help="purchasing option for slave instaces", )
        hadoop_group.add_argument("--custom-config",
                                  dest="custom_config_file",
                                  help="location of file containg custom" +
                                       " hadoop configuration overrides")
        hadoop_group.add_argument("--use-hbase", dest="use_hbase",
                                  action="store_true", default=None,
                                  help="Use hbase on this cluster", )

        spot_group = argparser.add_argument_group("spot instance settings" +
                                                  " (valid only when slave-request-type is hybrid or spot)")

        spot_group.add_argument("--maximum-bid-price-percentage",
                                dest="maximum_bid_price_percentage",
                                type=float,
                                help="maximum value to bid for spot instances" +
                                     " expressed as a percentage of the base" +
                                     " price for the slave node instance type", )
        spot_group.add_argument("--timeout-for-spot-request",
                                dest="timeout_for_request",
                                type=int,
                                help="timeout for a spot instance request" +
                                     " unit: minutes")
        spot_group.add_argument("--maximum-spot-instance-percentage",
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
                                       dest="stable_allow_fallback", default=None,
                                       type=str2bool,
                                       help="whether to fallback to on-demand instances for stable nodes" +
                                            " if spot instances aren't available")

        fairscheduler_group = argparser.add_argument_group(
            "fairscheduler configuration options")
        fairscheduler_group.add_argument("--fairscheduler-config-xml",
                                         dest="fairscheduler_config_xml_file",
                                         help="location for file containing" +
                                              " xml with custom configuration" +
                                              " for the fairscheduler", )
        fairscheduler_group.add_argument("--fairscheduler-default-pool",
                                         dest="default_pool",
                                         help="default pool for the" +
                                              " fairscheduler", )
        security_group = argparser.add_argument_group("security setttings")
        ephemerals = security_group.add_mutually_exclusive_group()
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

        termination = argparser.add_mutually_exclusive_group()
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

        ganglia = argparser.add_mutually_exclusive_group()
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

        argparser.add_argument("--node-bootstrap-file",
                               dest="node_bootstrap_file",
                               help="""name of the node bootstrap file for this cluster. It
                                    should be in stored in S3 at
                                    <account-default-location>/scripts/hadoop/NODE_BOOTSTRAP_FILE
                                    """, )
        if api_version <= 1.3:
            # set of parameters common for v1.2 and v1.3
            ec2_group = argparser.add_argument_group("ec2 settings")
            ec2_group.add_argument("--access-key-id",
                                   dest="aws_access_key_id",
                                   help="access key id for customer's aws" +
                                        " account. This is required while" +
                                        " creating the cluster",)
            ec2_group.add_argument("--secret-access-key",
                                   dest="aws_secret_access_key",
                                   help="secret access key for customer's aws" +
                                        " account. This is required while" +
                                        " creating the cluster",)
            ec2_group.add_argument("--aws-region",
                                   dest="aws_region",
                                   choices=["us-east-1", "us-west-2", "ap-northeast-1", "sa-east-1",
                                            "eu-west-1", "ap-southeast-1", "us-west-1"],
                                   help="aws region to create the cluster in",)
            ec2_group.add_argument("--aws-availability-zone",
                                   dest="aws_availability_zone",
                                   help="availability zone to" +
                                        " create the cluster in", )
            ec2_group.add_argument("--subnet-id",
                                   dest="subnet_id",
                                   help="subnet to create the cluster in", )
            ec2_group.add_argument("--vpc-id",
                                   dest="vpc_id",
                                   help="vpc to create the cluster in", )
            ec2_group.add_argument("--bastion-node-public-dns",
                                   dest="bastion_node_public_dns",
                                   help="public dns name of the bastion node. Required only if cluster is in private subnet of a EC2-VPC", )
            ec2_group.add_argument("--role-instance-profile",
                                   dest="role_instance_profile",
                                   help="IAM Role instance profile to attach on cluster",)
        if api_version >=1.3:
            # set of parameters common for v1.3 and v2
            qubole_placement_policy_group = hadoop_group.add_mutually_exclusive_group()
            qubole_placement_policy_group.add_argument("--use-qubole-placement-policy",
                                                       dest="use_qubole_placement_policy",
                                                       action="store_true",
                                                       default=None,
                                                       help="Use Qubole Block Placement policy" +
                                                            " for clusters with spot nodes", )
            qubole_placement_policy_group.add_argument("--no-use-qubole-placement-policy",
                                                       dest="use_qubole_placement_policy",
                                                       action="store_false",
                                                       default=None,
                                                       help="Do not use Qubole Block Placement policy" +
                                                            " for clusters with spot nodes", )
            enable_rubix_group = hadoop_group.add_mutually_exclusive_group()
            enable_rubix_group.add_argument("--enable-rubix",
                                            dest="enable_rubix",
                                            action="store_true",
                                            default=None,
                                            help="Enable rubix for cluster", )
            enable_rubix_group.add_argument("--no-enable-rubix",
                                            dest="enable_rubix",
                                            action="store_false",
                                            default=None,
                                            help="Do not enable rubix for cluster", )
            fallback_to_ondemand_group = node_config_group.add_mutually_exclusive_group()
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

        if api_version == 1.3:
            # set of parameters for v1.3
            ebs_volume_group = argparser.add_argument_group("ebs volume settings")
            ebs_volume_group.add_argument("--ebs-volume-count",
                                          dest="ebs_volume_count",
                                          type=int,
                                          help="Number of EBS volumes to attach to" +
                                               " each instance of the cluster", )
            ebs_volume_group.add_argument("--ebs-volume-type",
                                          dest="ebs_volume_type",
                                          choices=["standard", "gp2"],
                                          help=" of the EBS volume. Valid values are " +
                                               "'standard' (magnetic) and 'gp2' (ssd).", )
            ebs_volume_group.add_argument("--ebs-volume-size",
                                          dest="ebs_volume_size",
                                          type=int,
                                          help="Size of each EBS volume, in GB", )

        if api_version == 2.0:
            # set of new parameters for v2 api version
            compute_config = argparser.add_argument_group("compute config")
            compute_config.add_argument("--compute-subscription-id",
                                        dest="compute_subscription_id",
                                        default=None,
                                        help="Subscription id for azure cluster")
            compute_config.add_argument("--compute-client-id",
                                        dest="compute_client_id",
                                        default=None,
                                        help="client id for azure cluster")
            compute_config.add_argument("--compute-client-secret",
                                        dest="compute_client_secret",
                                        default=None,
                                        help="client id for azure cluster")
            compute_config.add_argument("--compute-tenant-id",
                                        dest="compute_tenant_id",
                                        default=None,
                                        help="tenant id for azure cluster")
            compute_config.add_argument("--compute-access-key",
                                        dest="compute_access_key",
                                        default=None,
                                        help="access key for aws cluster")
            compute_config.add_argument("--compute-secret-key",
                                        dest="compute_secret_key",
                                        default=None,
                                        help="secret key for aws cluster")
            compute_config.add_argument("--compute-external-id",
                                        dest="compute_external_id",
                                        default=None,
                                        help="external id for aws cluster")
            compute_config.add_argument("--compute-role-arn",
                                        dest="compute_role_arn",
                                        default=None,
                                        help="role arn for aws cluster")
            compute_config.add_argument("--use-account-compute-creds",
                                        dest="use_account_compute_creds",
                                        default=None,
                                        help="secret key for aws cluster")
            compute_config.add_argument("--compute-user-id",
                                        dest="compute_user_id",
                                        default=None,
                                        help="compute user id for oracle cluster")
            compute_config.add_argument("--compute-key-finger-print",
                                        dest="compute_key_finger_print",
                                        default=None,
                                        help="compute key fingerprint for oracle cluster")
            compute_config.add_argument("--compute-api-private-rsa-key",
                                        dest="compute_api_private_rsa_key",
                                        default=None,
                                        help="compute api private rsa key for oracle cluster")
            compute_config.add_argument("--role-instance-profile",
                                   dest="role_instance_profile",
                                   help="IAM Role instance profile to attach on cluster", )
            location_group = argparser.add_argument_group("location config")
            location_group.add_argument("--location",
                                        dest="location",
                                        default=None,
                                        help="location for azure cluster")
            location_group.add_argument("--aws-region",
                                   dest="aws_region",
                                   choices=["us-east-1", "us-west-2", "ap-northeast-1", "sa-east-1",
                                            "eu-west-1", "ap-southeast-1", "us-west-1"],
                                   help="aws region to create the cluster in", )
            location_group.add_argument("--aws-availability-zone",
                               dest="aws_availability_zone",
                               help="availability zone to" +
                                    " create the cluster in", )
            storage_config = argparser.add_argument_group("storage config")
            storage_config.add_argument("--storage-access-key",
                                        dest="storage_access_key",
                                        default=None,
                                        help="storage access key for azure cluster")
            storage_config.add_argument("--storage-account-name",
                                        dest="storage_account_name",
                                        default=None,
                                        help="storage account name for azure cluster")
            storage_config.add_argument("--disk-storage-account-name",
                                        dest="disk_storage_account_name",
                                        default=None,
                                        help="disk storage account name for azure cluster")
            storage_config.add_argument("--disk-storage-account-resource-group-name",
                                        dest="disk_storage_account_resource_group_name",
                                        default=None,
                                        help="disk storage account resource group for azure cluster")
            storage_config.add_argument("--storage-tenant-id",
                                        dest="storage_tenant_id",
                                        default=None,
                                        help="storage tenant id for oracle cluster")
            storage_config.add_argument("--storage-user-id",
                                        dest="storage_user_id",
                                        default=None,
                                        help="storage user id for oracle cluster")
            storage_config.add_argument("--storage-key-finger-print",
                                        dest="storage_key_finger_print",
                                        default=None,
                                        help="storage key fingerprint for oracle cluster")
            storage_config.add_argument("--storage-api-private-rsa-key",
                                        dest="storage_api_private_rsa_key",
                                        default=None,
                                        help="storage api private rds key for oracle cluster")

            engine_config = argparser.add_argument_group("engine config")
            engine_config.add_argument("--flavour",
                                       dest="flavour",
                                       default=None,
                                       help="secret key for aws cluster")

            node_config_group.add_argument("--customer-ssh-key",
                                           dest="customer_ssh_key",
                                           help="public ssh key which needs to be added to the cluster")
            node_config_group.add_argument("--heterogeneous-config",
                                           dest="heterogeneous_config",
                                           help="heterogeneous config for the cluster")
            node_config_group.add_argument("--idle-cluster-timeout",
                                           dest="idle_cluster_timeout",
                                           help="cluster termination timeout for idle cluster")
            node_config_group.add_argument("--custom-tags",
                                           dest="custom_tags",
                                           help="""Custom  tags to be set on all instances
                                             of the cluster. Specified as JSON object (key-value pairs)
                                             e.g. --custom-ec2-tags '{"key1":"value1", "key2":"value2"}'
                                             """,)

            datadisk_group = node_config_group.add_argument_group("data disk settings")
            datadisk_group.add_argument("--count",
                                        dest="count",
                                        type=int,
                                        help="Number of volumes to attach to" +
                                             " each instance of the cluster",)

            datadisk_group.add_argument("--disk-type",
                                        dest="disk_type",
                                        help="Type of the  volume attached to the instances.",)
            datadisk_group.add_argument("--size",
                                        dest="size",
                                        type=int,
                                        help="Size of each EBS volume, in GB",)
            datadisk_group.add_argument("--upscaling-config",
                                        dest="upscaling_config",
                                        help="Upscaling config to be attached with the instances.", )

            network_config_group = argparser.add_argument_group("network config settings")
            network_config_group.add_argument("--persistent-security-groups",
                                              dest="persistent_security_groups",
                                              help="a security group to associate with each" +
                                              " node of the cluster. Typically used" +
                                              " to provide access to external hosts",)
            network_config_group.add_argument("--vnet-name",
                                                    dest="vnet_name",
                                                    help="vnet name for azure",)
            network_config_group.add_argument("--subnet-name",
                                                    dest="subnet_name",
                                                    help="subnet name for azure")
            network_config_group.add_argument("--vnet-resource-group-name",
                                                    dest="vnet_resource_group_name",
                                                    help="vnet resource group name for azure")
            network_config_group.add_argument("--master-elastic-ip",
                                              dest="master_elastic_ip",
                                              help="master elastic ip for cluster")
            network_config_group.add_argument("--vcn-id",
                                              dest="vcn_id",
                                              help="vcn for oracle", )
            network_config_group.add_argument("--compartment-id",
                                              dest="compartment_id",
                                              help="compartment id for oracle cluster")
            network_config_group.add_argument("--image-id",
                                              dest="image_id",
                                              help="image id for oracle cloud")
            network_config_group.add_argument("--subnet-id",
                                   dest="subnet_id",
                                   help="subnet to create the cluster in", )
            network_config_group.add_argument("--vpc-id",
                               dest="vpc_id",
                               help="vpc to create the cluster in", )
            network_config_group.add_argument("--bastion-node-public-dns",
                               dest="bastion_node_public_dns",
                               help="public dns name of the bastion node. Required only if cluster is in private subnet of a EC2-VPC", )

            engine_config_group = argparser.add_argument_group("engine config settings")
            engine_config_group.add_argument("--custom-presto-config",
                                       dest="custom_presto_config",
                                       default=None,
                                       help="Custom config presto for this cluster",)

            engine_config_group.add_argument("--presto-version",
                                             dest="presto_version",
                                             default=None,
                                             help="Version of presto for this cluster", )
            engine_config_group.add_argument("--custom-spark-config",
                                             dest="custom_spark_config",
                                             default=None,
                                             help="Custom config spark for this cluster", )

            engine_config_group.add_argument("--spark-version",
                                             dest="spark_version",
                                             default=None,
                                             help="Version of spark for the cluster", )
            engine_config_group.add_argument("--custom-hadoop-config",
                                             dest="custom_hadoop_config",
                                             default=None,
                                             help="Custom config presto for the cluster", )

            engine_config_group.add_argument("--dbtap-id",
                                             dest="dbtap_id",
                                             default=None,
                                             help="dbtap id for airflow cluster", )
            engine_config_group.add_argument("--fernet-key",
                                             dest="fernet_key",
                                             default=None,
                                             help="fernet key for airflow cluster", )
            engine_config_group.add_argument("--overrides",
                                             dest="overrides",
                                             default=None,
                                             help="overrides for airflow cluster", )

            datadog_group = argparser.add_argument_group("datadog settings")
            datadog_group.add_argument("--datadog-api-token",
                                            dest="datadog_api_token",
                                            default=None,
                                            help="fernet key for airflow cluster", )
            datadog_group.add_argument("--datadog-app-token",
                                         dest="datadog_app_token",
                                         default=None,
                                         help="overrides for airflow cluster", )

        if api_version <= 1.3:
            # set of parameters common for v1.2 and v1.3
            hadoop2 = hadoop_group.add_mutually_exclusive_group()
            hadoop2.add_argument("--use-hadoop2",
                                 dest="use_hadoop2",
                                 action="store_true",
                                 default=None,
                                 help="Use hadoop2 instead of hadoop1")
            hadoop2.add_argument("--use-hadoop1",
                                 dest="use_hadoop2",
                                 action="store_false",
                                 default=None,
                                 help="Use hadoop1 instead of hadoop2. This is the default.")
            hadoop2.add_argument("--use-spark",
                                 dest="use_spark",
                                 action="store_true",
                                 default=None,
                                 help="Turn on spark for this cluster")
            security_group = argparser.add_argument_group("security setttings")
            security_group.add_argument("--customer-ssh-key",
                                        dest="customer_ssh_key_file",
                                        help="location for ssh key to use to" +
                                             " login to the instance")

            security_group.add_argument("--persistent-security-group",
                                        dest="persistent_security_group",
                                        help="a security group to associate with each" +
                                        " node of the cluster. Typically used" +
                                        " to provide access to external hosts")
            presto_group = argparser.add_argument_group("presto settings")
            enabling_presto = presto_group.add_mutually_exclusive_group()
            enabling_presto.add_argument("--enable-presto",
                                         dest="enable_presto",
                                         action="store_true",
                                         default=None,
                                         help="Enable presto for this cluster", )
            enabling_presto.add_argument("--disable-presto",
                                         dest="enable_presto",
                                         action="store_false",
                                         default=None,
                                         help="Disable presto for this cluster", )
            presto_group.add_argument("--presto-custom-config",
                                      dest="presto_custom_config_file",
                                      help="location of file containg custom" +
                                           " presto configuration overrides")
            argparser.add_argument("--custom-ec2-tags",
                                   dest="custom_ec2_tags",
                                   help="""Custom ec2 tags to be set on all instances
                    of the cluster. Specified as JSON object (key-value pairs)
                    e.g. --custom-ec2-tags '{"key1":"value1", "key2":"value2"}'
                    """, )

        arguments = argparser.parse_args(args)
        return arguments

    @classmethod
    def create(cls, cluster_info, version=None):
        """
        Create a new cluster using information provided in `cluster_info`.

        Optionally provide the version (eg: v1.3) to use the new version of the
        API. If None we default to v1.2
        """
        conn = Qubole.agent(version=version)
        return conn.post(cls.rest_entity_path, data=cluster_info)

    @classmethod
    def update(cls, cluster_id_label, cluster_info, version=None):
        """
        Update the cluster with id/label `cluster_id_label` using information provided in
        `cluster_info`.

        Optionally provide the version (eg: v1.3) to use the new version of the
        API. If None we default to v1.2
        """
        conn = Qubole.agent(version=version)
        return conn.put(cls.element_path(cluster_id_label), data=cluster_info)

    @classmethod
    def clone(cls, cluster_id_label, cluster_info, version=None):
        """
        Update the cluster with id/label `cluster_id_label` using information provided in
        `cluster_info`.

        Optionally provide the version (eg: v1.3) to use the new version of the
        API. If None we default to v1.2
        """
        conn = Qubole.agent(version=version)
        return conn.post(cls.element_path(cluster_id_label) + '/clone', data=cluster_info)

    @classmethod
    def _parse_cluster_manage_command(cls, args, action):
      """
      Parse command line arguments for cluster manage commands.
      """

      argparser = ArgumentParser(prog="cluster_manage_command")

      group = argparser.add_mutually_exclusive_group(required=True)

      group.add_argument("--id", dest="cluster_id",
                           help="execute on cluster with this id")

      group.add_argument("--label", dest="label",
                           help="execute on cluster with this label")

      if action == "remove" or action == "update":
        argparser.add_argument("--private_dns",
                           help="the private_dns of the machine to be updated/removed", required=True)
      if action == "update":
        argparser.add_argument("--command",
                           help="the update command to be executed", required=True, choices=["replace"])

      arguments = argparser.parse_args(args)
      return arguments

    @classmethod
    def _parse_reassign_label(cls, args):
        """
        Parse command line arguments for reassigning label.
        """
        argparser = ArgumentParser(prog="cluster reassign_label")

        argparser.add_argument("destination_cluster",
                metavar="destination_cluster_id_label",
                help="id/label of the cluster to move the label to")

        argparser.add_argument("label",
                help="label to be moved from the source cluster")

        arguments = argparser.parse_args(args)
        return arguments

    @classmethod
    def reassign_label(cls, destination_cluster, label):
        """
        Reassign a label from one cluster to another.

        Args:
            `destination_cluster`: id/label of the cluster to move the label to

            `label`: label to be moved from the source cluster
        """
        conn = Qubole.agent(version=Cluster.api_version)
        data = {
                    "destination_cluster": destination_cluster,
                    "label": label
                }
        return conn.put(cls.rest_entity_path + "/reassign-label", data)

    @classmethod
    def delete(cls, cluster_id_label):
        """
        Delete the cluster with id/label `cluster_id_label`.
        """
        conn = Qubole.agent(version=Cluster.api_version)
        return conn.delete(cls.element_path(cluster_id_label))

    @classmethod
    def _parse_snapshot_restore_command(cls, args, action):
        """
        Parse command line arguments for snapshot command.
        """
        argparser = ArgumentParser(prog="cluster %s" % action)

        group = argparser.add_mutually_exclusive_group(required=True)
        group.add_argument("--id", dest="cluster_id",
                          help="execute on cluster with this id")
        group.add_argument("--label", dest="label",
                          help="execute on cluster with this label")
        argparser.add_argument("--s3_location",
                          help="s3_location where backup is stored", required=True)
        if action == "snapshot":
            argparser.add_argument("--backup_type",
                          help="backup_type: full/incremental, default is full")
        elif action == "restore_point":
            argparser.add_argument("--backup_id",
                          help="back_id from which restoration will be done", required=True)
            argparser.add_argument("--table_names",
                          help="table(s) which are to be restored", required=True)
            argparser.add_argument("--no-overwrite", action="store_false",
                          help="With this option, restore overwrites to the existing table if theres any in restore target")
            argparser.add_argument("--no-automatic", action="store_false",
                          help="With this option, all the dependencies are automatically restored together with this backup image following the correct order")
        arguments = argparser.parse_args(args)

        return arguments

    @classmethod
    def _parse_get_snapshot_schedule(cls, args):
        """
        Parse command line arguments for updating hbase snapshot schedule or to get details.
        """
        argparser = ArgumentParser(prog="cluster snapshot_schedule")

        group = argparser.add_mutually_exclusive_group(required=True)
        group.add_argument("--id", dest="cluster_id",
                          help="execute on cluster with this id")
        group.add_argument("--label", dest="label",
                          help="execute on cluster with this label")
        arguments = argparser.parse_args(args)

        return arguments

    @classmethod
    def _parse_update_snapshot_schedule(cls, args):
        """
        Parse command line arguments for updating hbase snapshot schedule or to get details.
        """
        argparser = ArgumentParser(prog="cluster snapshot_schedule")

        group = argparser.add_mutually_exclusive_group(required=True)
        group.add_argument("--id", dest="cluster_id",
                          help="execute on cluster with this id")
        group.add_argument("--label", dest="label",
                          help="execute on cluster with this label")

        argparser.add_argument("--frequency-num",
                          help="frequency number")
        argparser.add_argument("--frequency-unit",
                          help="frequency unit")
        argparser.add_argument("--s3-location",
                          help="s3_location about where to store snapshots")
        argparser.add_argument("--status",
                          help="status of periodic job you want to change to", choices = ["RUNNING", "SUSPENDED"])

        arguments = argparser.parse_args(args)

        return arguments

    @classmethod
    def snapshot(cls, cluster_id_label, s3_location, backup_type):
        """
        Create hbase snapshot full/incremental
        """
        conn = Qubole.agent()
        parameters = {}
        parameters['s3_location'] = s3_location
        if backup_type:
            parameters['backup_type'] = backup_type
        return conn.post(cls.element_path(cluster_id_label) + "/snapshots", data=parameters)

    @classmethod
    def restore_point(cls, cluster_id_label, s3_location, backup_id, table_names, overwrite=True, automatic=True):
        """
        Restoring cluster from a given hbase snapshot id
        """
        conn = Qubole.agent()
        parameters = {}
        parameters['s3_location'] = s3_location
        parameters['backup_id'] = backup_id
        parameters['table_names'] = table_names
        parameters['overwrite'] = overwrite
        parameters['automatic'] = automatic
        return conn.post(cls.element_path(cluster_id_label) + "/restore_point", data=parameters)

    @classmethod
    def get_snapshot_schedule(cls, cluster_id_label):
        """
        Get details for snapshot schedule
        """
        conn = Qubole.agent()
        return conn.get(cls.element_path(cluster_id_label) + "/snapshot_schedule")

    @classmethod
    def update_snapshot_schedule(cls, cluster_id_label, s3_location=None, frequency_unit=None, frequency_num=None, status=None):
        """
        Update for snapshot schedule
        """
        conn = Qubole.agent()

        data = {}
        if s3_location is not None:
            data["s3_location"] = s3_location
        if frequency_unit is not None:
            data["frequency_unit"] = frequency_unit
        if frequency_num is not None:
            data["frequency_num"] = frequency_num
        if status is not None:
            data["status"] = status
        return conn.put(cls.element_path(cluster_id_label) + "/snapshot_schedule", data)



    @classmethod
    def add_node(cls, cluster_id_label, parameters=None):
      """
      Add a node to an existing cluster
      """
      conn = Qubole.agent()
      parameters = {} if not parameters else parameters
      return conn.post(cls.element_path(cluster_id_label) + "/nodes", data={"parameters" : parameters})

    @classmethod
    def remove_node(cls, cluster_id_label, private_dns, parameters=None):
        """
        Add a node to an existing cluster
        """
        conn = Qubole.agent()
        parameters = {} if not parameters else parameters
        data = {"private_dns" : private_dns, "parameters" : parameters}
        return conn.delete(cls.element_path(cluster_id_label) + "/nodes", data)

    @classmethod
    def update_node(cls, cluster_id_label, command, private_dns, parameters=None):
        """
        Add a node to an existing cluster
        """
        conn = Qubole.agent()
        parameters = {} if not parameters else parameters
        data = {"command" : command, "private_dns" : private_dns, "parameters" : parameters}
        return conn.put(cls.element_path(cluster_id_label) + "/nodes", data)

class ClusterInfo():
    """
    qds_sdk.ClusterInfo is the class which stores information about a cluster.
    You can use objects of this class to create or update a cluster.
    """

    def __init__(self, label, aws_access_key_id, aws_secret_access_key,
                 disallow_cluster_termination=None,
                 enable_ganglia_monitoring=None,
                 node_bootstrap_file=None):
        """
        Args:

        `label`: A list of labels that identify the cluster. At least one label
            must be provided when creating a cluster.

        `aws_access_key_id`: The access key id for customer's aws account. This
            is required for creating the cluster.

        `aws_secret_access_key`: The secret access key for customer's aws
            account. This is required for creating the cluster.

        `disallow_cluster_termination`: Set this to True if you don't want
            qubole to auto-terminate idle clusters. Use this option with
            extreme caution.

        `enable_ganglia_monitoring`: Set this to True if you want to enable
            ganglia monitoring for the cluster.

        `node_bootstrap_file`: name of the node bootstrap file for this
            cluster. It should be in stored in S3 at
            <your-default-location>/scripts/hadoop/
        """
        self.label = label
        self.ec2_settings = {}
        self.ec2_settings['compute_access_key'] = aws_access_key_id
        self.ec2_settings['compute_secret_key'] = aws_secret_access_key
        self.disallow_cluster_termination = disallow_cluster_termination
        self.enable_ganglia_monitoring = enable_ganglia_monitoring
        self.node_bootstrap_file = node_bootstrap_file
        self.hadoop_settings = {}
        self.security_settings = {}
        self.presto_settings = {}

    def set_ec2_settings(self,
                         aws_region=None,
                         aws_availability_zone=None,
                         vpc_id=None,
                         subnet_id=None,
                         role_instance_profile=None,
                         bastion_node_public_dns=None):
        """
        Kwargs:

        `aws_region`: AWS region to create the cluster in.

        `aws_availability_zone`: The availability zone to create the cluster
            in.

        `vpc_id`: The vpc to create the cluster in.

        `subnet_id`: The subnet to create the cluster in.

        `bastion_node_public_dns`: Public dns name of the bastion host. Required only if
            cluster is in private subnet.
        """
        self.ec2_settings['aws_region'] = aws_region
        self.ec2_settings['aws_preferred_availability_zone'] = aws_availability_zone
        self.ec2_settings['vpc_id'] = vpc_id
        self.ec2_settings['subnet_id'] = subnet_id
        self.ec2_settings['role_instance_profile'] = role_instance_profile
        self.ec2_settings['bastion_node_public_dns'] = bastion_node_public_dns

    def set_hadoop_settings(self, master_instance_type=None,
                            slave_instance_type=None,
                            initial_nodes=None,
                            max_nodes=None,
                            custom_config=None,
                            slave_request_type=None,
                            use_hbase=None,
                            custom_ec2_tags=None,
                            use_hadoop2=None,
                            use_spark=None):
        """
        Kwargs:

        `master_instance_type`: The instance type to use for the Hadoop master
            node.

        `slave_instance_type`: The instance type to use for the Hadoop slave
            nodes.

        `initial_nodes`: Number of nodes to start the cluster with.

        `max_nodes`: Maximum number of nodes the cluster may be auto-scaled up
            to.

        `custom_config`: Custom Hadoop configuration overrides.

        `slave_request_type`: Purchasing option for slave instances.
            Valid values: "ondemand", "hybrid", "spot".

        `use_hbase`: Start hbase daemons on the cluster. Uses Hadoop2

        `use_hadoop2`: Use hadoop2 in this cluster

        `use_spark`: Use spark in this cluster
        """
        self.hadoop_settings['master_instance_type'] = master_instance_type
        self.hadoop_settings['slave_instance_type'] = slave_instance_type
        self.hadoop_settings['initial_nodes'] = initial_nodes
        self.hadoop_settings['max_nodes'] = max_nodes
        self.hadoop_settings['custom_config'] = custom_config
        self.hadoop_settings['slave_request_type'] = slave_request_type
        self.hadoop_settings['use_hbase'] = use_hbase
        self.hadoop_settings['use_hadoop2'] = use_hadoop2
        self.hadoop_settings['use_spark'] = use_spark

        if custom_ec2_tags and custom_ec2_tags.strip():
            try:
                self.hadoop_settings['custom_ec2_tags'] = json.loads(custom_ec2_tags.strip())
            except Exception as e:
                raise Exception("Invalid JSON string for custom ec2 tags: %s" % e.message)

    def set_spot_instance_settings(self, maximum_bid_price_percentage=None,
                                   timeout_for_request=None,
                                   maximum_spot_instance_percentage=None):
        """
        Purchase options for spot instances. Valid only when
        `slave_request_type` is hybrid or spot.

        `maximum_bid_price_percentage`: Maximum value to bid for spot
            instances, expressed as a percentage of the base price for the
            slave node instance type.

        `timeout_for_request`: Timeout for a spot instance request (Unit:
            minutes)

        `maximum_spot_instance_percentage`: Maximum percentage of instances
            that may be purchased from the AWS Spot market. Valid only when
            slave_request_type is "hybrid".
        """
        self.hadoop_settings['spot_instance_settings'] = {
               'maximum_bid_price_percentage': maximum_bid_price_percentage,
               'timeout_for_request': timeout_for_request,
               'maximum_spot_instance_percentage': maximum_spot_instance_percentage}


    def set_stable_spot_instance_settings(self, maximum_bid_price_percentage=None,
                                          timeout_for_request=None,
                                          allow_fallback=True):
        """
        Purchase options for stable spot instances.

        `maximum_bid_price_percentage`: Maximum value to bid for stable node spot
            instances, expressed as a percentage of the base price
            (applies to both master and slave nodes).

        `timeout_for_request`: Timeout for a stable node spot instance request (Unit:
            minutes)

        `allow_fallback`: Whether to fallback to on-demand instances for
            stable nodes if spot instances are not available
        """
        self.hadoop_settings['stable_spot_instance_settings'] = {
               'maximum_bid_price_percentage': maximum_bid_price_percentage,
               'timeout_for_request': timeout_for_request,
               'allow_fallback': allow_fallback}


    def set_fairscheduler_settings(self, fairscheduler_config_xml=None,
                                   default_pool=None):
        """
        Fair scheduler configuration options.

        `fairscheduler_config_xml`: XML string with custom configuration
            parameters for the fair scheduler.

        `default_pool`: The default pool for the fair scheduler.
        """
        self.hadoop_settings['fairscheduler_settings'] = {
               'fairscheduler_config_xml': fairscheduler_config_xml,
               'default_pool': default_pool}

    def set_security_settings(self,
                              encrypted_ephemerals=None,
                              customer_ssh_key=None,
                              persistent_security_group=None):
        """
        Kwargs:

        `encrypted_ephemerals`: Encrypt the ephemeral drives on the instance.

        `customer_ssh_key`: SSH key to use to login to the instances.
        """
        self.security_settings['encrypted_ephemerals'] = encrypted_ephemerals
        self.security_settings['customer_ssh_key'] = customer_ssh_key
        self.security_settings['persistent_security_group'] = persistent_security_group

    def set_presto_settings(self, enable_presto=None, presto_custom_config=None):
        """
        Kwargs:

        `enable_presto`: Enable Presto on the cluster.

        `presto_custom_config`: Custom Presto configuration overrides.
        """
        self.presto_settings['enable_presto'] = enable_presto
        self.presto_settings['custom_config'] = presto_custom_config

    def minimal_payload(self):
        """
        This method can be used to create the payload which is sent while
        creating or updating a cluster.
        """
        payload = {"cluster": self.__dict__}
        return _make_minimal(payload)

class ClusterInfoV13(object):
    """
    qds_sdk.ClusterInfo is the class which stores information about a cluster.
    You can use objects of this class to create or update a cluster.
    """

    def __init__(self, label, api_version=1.3):
        """
        Args:

        `label`: A list of labels that identify the cluster. At least one label
            must be provided when creating a cluster.

        `api_version`: api version to use

        """
        self.label = label
        self.api_version = api_version
        self.ec2_settings = {}
        self.hadoop_settings = {}
        self.security_settings = {}
        self.presto_settings = {}
        self.spark_settings = {}
        self.node_configuration = {}

    def set_cluster_info(self, aws_access_key_id=None,
                         aws_secret_access_key=None,
                         aws_region=None,
                         aws_availability_zone=None,
                         vpc_id=None,
                         subnet_id=None,
                         disallow_cluster_termination=None,
                         enable_ganglia_monitoring=None,
                         node_bootstrap_file=None,
                         master_instance_type=None,
                         slave_instance_type=None,
                         initial_nodes=None,
                         max_nodes=None,
                         slave_request_type=None,
                         fallback_to_ondemand=None,
                         custom_config=None,
                         use_hbase=None,
                         custom_ec2_tags=None,
                         use_hadoop2=None,
                         use_spark=None,
                         use_qubole_placement_policy=None,
                         enable_rubix=None,
                         maximum_bid_price_percentage=None,
                         timeout_for_request=None,
                         maximum_spot_instance_percentage=None,
                         stable_maximum_bid_price_percentage=None,
                         stable_timeout_for_request=None,
                         stable_allow_fallback=True,
                         ebs_volume_count=None,
                         ebs_volume_type=None,
                         ebs_volume_size=None,
                         fairscheduler_config_xml=None,
                         default_pool=None,
                         encrypted_ephemerals=None,
                         ssh_public_key=None,
                         persistent_security_group=None,
                         enable_presto=None,
                         bastion_node_public_dns=None,
                         role_instance_profile=None,
                         presto_custom_config=None):
        """
        Kwargs:

        `aws_access_key_id`: The access key id for customer's aws account. This
            is required for creating the cluster.

        `aws_secret_access_key`: The secret access key for customer's aws
            account. This is required for creating the cluster.

        `aws_region`: AWS region to create the cluster in.

        `aws_availability_zone`: The availability zone to create the cluster
            in.

        `vpc_id`: The vpc to create the cluster in.

        `subnet_id`: The subnet to create the cluster in.

        `disallow_cluster_termination`: Set this to True if you don't want
            qubole to auto-terminate idle clusters. Use this option with
            extreme caution.

        `enable_ganglia_monitoring`: Set this to True if you want to enable
            ganglia monitoring for the cluster.

        `node_bootstrap_file`: name of the node bootstrap file for this
            cluster. It should be in stored in S3 at
            <your-default-location>/scripts/hadoop/

        `master_instance_type`: The instance type to use for the Hadoop master
            node.

        `slave_instance_type`: The instance type to use for the Hadoop slave
            nodes.

        `initial_nodes`: Number of nodes to start the cluster with.

        `max_nodes`: Maximum number of nodes the cluster may be auto-scaled up
            to.

        `slave_request_type`: Purchasing option for slave instances.
            Valid values: "ondemand", "hybrid", "spot".

        `fallback_to_ondemand`: Fallback to on-demand nodes if spot nodes could not be
            obtained. Valid only if slave_request_type is 'spot'.

        `custom_config`: Custom Hadoop configuration overrides.

        `use_hbase`: Start hbase daemons on the cluster. Uses Hadoop2

        `use_hadoop2`: Use hadoop2 in this cluster

        `use_spark`: Use spark in this cluster

        `use_qubole_placement_policy`: Use Qubole Block Placement policy for 
            clusters with spot nodes.

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

        `stable_allow_fallback`: Whether to fallback to on-demand instances for
            stable nodes if spot instances are not available

        `ebs_volume_count`: Number of EBS volumes to attach 
            to each instance of the cluster.

        `ebs_volume_type`: Type of the EBS volume. Valid 
            values are 'standard' (magnetic) and 'ssd'.

        `ebs_volume_size`: Size of each EBS volume, in GB. 

        `fairscheduler_config_xml`: XML string with custom configuration
            parameters for the fair scheduler.

        `default_pool`: The default pool for the fair scheduler.

        `encrypted_ephemerals`: Encrypt the ephemeral drives on the instance.

        `ssh_public_key`: SSH key to use to login to the instances.

        `persistent_security_group`: Comma-separated list of persistent 
            security groups for the cluster.

        `enable_presto`: Enable Presto on the cluster.

        `presto_custom_config`: Custom Presto configuration overrides.

        `bastion_node_public_dns`: Public dns name of the bastion node. Required only if cluster is in private subnet.

        `enable_rubix`: Enable rubix_caching on the cluster.

        """

        self.disallow_cluster_termination = disallow_cluster_termination
        self.enable_ganglia_monitoring = enable_ganglia_monitoring
        self.node_bootstrap_file = node_bootstrap_file
        self.set_node_configuration(master_instance_type, slave_instance_type, initial_nodes, max_nodes,
                                    slave_request_type, fallback_to_ondemand)
        self.set_ec2_settings(aws_access_key_id, aws_secret_access_key, aws_region, aws_availability_zone,
                              vpc_id, subnet_id, bastion_node_public_dns, role_instance_profile)
        self.set_hadoop_settings(custom_config, use_hbase, custom_ec2_tags, use_hadoop2,
                                 use_spark, use_qubole_placement_policy, enable_rubix)
        self.set_spot_instance_settings(maximum_bid_price_percentage, timeout_for_request,
                                        maximum_spot_instance_percentage)
        self.set_stable_spot_instance_settings(stable_maximum_bid_price_percentage, stable_timeout_for_request,
                                               stable_allow_fallback)
        self.set_ebs_volume_settings(ebs_volume_count, ebs_volume_type, ebs_volume_size)
        self.set_fairscheduler_settings(fairscheduler_config_xml, default_pool)
        self.set_security_settings(encrypted_ephemerals, ssh_public_key, persistent_security_group)
        self.set_presto_settings(enable_presto, presto_custom_config)

    def set_ec2_settings(self, aws_access_key_id=None,
                           aws_secret_access_key=None,
                           aws_region=None,
                           aws_availability_zone=None,
                           vpc_id=None,
                           subnet_id=None,
                           bastion_node_public_dns=None,
                           role_instance_profile=None):
        self.ec2_settings['compute_access_key'] = aws_access_key_id
        self.ec2_settings['compute_secret_key'] = aws_secret_access_key
        self.ec2_settings['aws_region'] = aws_region
        self.ec2_settings['aws_preferred_availability_zone'] = aws_availability_zone
        self.ec2_settings['vpc_id'] = vpc_id
        self.ec2_settings['subnet_id'] = subnet_id
        self.ec2_settings['bastion_node_public_dns'] = bastion_node_public_dns
        self.ec2_settings['role_instance_profile'] = role_instance_profile

    def set_node_configuration(self, master_instance_type=None,
                            slave_instance_type=None,
                            initial_nodes=None,
                            max_nodes=None,
                            slave_request_type=None,
                            fallback_to_ondemand=None):
        self.node_configuration['master_instance_type'] = master_instance_type
        self.node_configuration['slave_instance_type'] = slave_instance_type
        self.node_configuration['initial_nodes'] = initial_nodes
        self.node_configuration['max_nodes'] = max_nodes
        self.node_configuration['slave_request_type'] = slave_request_type
        self.node_configuration['fallback_to_ondemand'] = fallback_to_ondemand

    def set_hadoop_settings(self, custom_config=None,
                            use_hbase=None,
                            custom_ec2_tags=None,
                            use_hadoop2=None,
                            use_spark=None,
                            use_qubole_placement_policy=None,
                            enable_rubix=None,):
        self.hadoop_settings['custom_config'] = custom_config
        self.hadoop_settings['use_hbase'] = use_hbase
        self.hadoop_settings['use_hadoop2'] = use_hadoop2
        self.hadoop_settings['use_spark'] = use_spark
        self.hadoop_settings['use_qubole_placement_policy'] = use_qubole_placement_policy
        self.hadoop_settings['enable_rubix'] = enable_rubix

        if custom_ec2_tags and custom_ec2_tags.strip():
            try:
                self.hadoop_settings['custom_ec2_tags'] = json.loads(custom_ec2_tags.strip())
            except Exception as e:
                raise Exception("Invalid JSON string for custom ec2 tags: %s" % e.message)

    def set_spot_instance_settings(self, maximum_bid_price_percentage=None,
                                   timeout_for_request=None,
                                   maximum_spot_instance_percentage=None):
        self.node_configuration['spot_instance_settings'] = {
               'maximum_bid_price_percentage': maximum_bid_price_percentage,
               'timeout_for_request': timeout_for_request,
               'maximum_spot_instance_percentage': maximum_spot_instance_percentage}

    def set_stable_spot_instance_settings(self, maximum_bid_price_percentage=None,
                                          timeout_for_request=None,
                                          allow_fallback=True):
        self.node_configuration['stable_spot_instance_settings'] = {
               'maximum_bid_price_percentage': maximum_bid_price_percentage,
               'timeout_for_request': timeout_for_request,
               'allow_fallback': allow_fallback}

    def set_ebs_volume_settings(self, ebs_volume_count=None,
                                 ebs_volume_type=None,
                                 ebs_volume_size=None):
      self.node_configuration['ebs_volume_count'] = ebs_volume_count
      self.node_configuration['ebs_volume_type'] = ebs_volume_type
      self.node_configuration['ebs_volume_size'] = ebs_volume_size


    def set_fairscheduler_settings(self, fairscheduler_config_xml=None,
                                   default_pool=None):
        self.hadoop_settings['fairscheduler_settings'] = {
               'fairscheduler_config_xml': fairscheduler_config_xml,
               'default_pool': default_pool}

    def set_security_settings(self,
                              encrypted_ephemerals=None,
                              ssh_public_key=None,
                              persistent_security_group=None):
        self.security_settings['encrypted_ephemerals'] = encrypted_ephemerals
        self.security_settings['ssh_public_key'] = ssh_public_key
        self.security_settings['persistent_security_group'] = persistent_security_group

    def set_presto_settings(self, enable_presto=None, presto_custom_config=None):
        self.presto_settings['enable_presto'] = enable_presto
        self.presto_settings['custom_config'] = presto_custom_config

    def set_spark_settings(self, use_spark=True, custom_config=None):
        self.spark_settings['use_spark'] = use_spark
        self.spark_settings['custom_config'] = custom_config

    def minimal_payload(self):
        """
        This method can be used to create the payload which is sent while
        creating or updating a cluster.
        """
        payload_dict = self.__dict__
        payload_dict.pop("api_version", None)
        return _make_minimal(payload_dict)


class ClusterInfoV2(object):
    """ This class should be entirely temporary to deal with the junk API for cluster creation
    """

    def __init__(self, label, api_version=2):
        """
        Args:

        `label`: A list of labels that identify the cluster. At least one label
            must be provided when creating a cluster.

        `api_version`: api version to use

        """
        self.cluster_info = {}
        self.cluster_info['label'] = label
        self.api_version = api_version
        self.cloud_config = {}
        self.engine_config = {}
        self.monitoring = {}
        self.internal = {}

    def minimal_payload(self):
        """
        This method can be used to create the payload which is sent while
        creating or updating a cluster.
        """
        payload_dict = self.__dict__
        payload_dict.pop("api_version", None)
        return _make_minimal(payload_dict)

    def set_compute_config(self, compute_validated=None,
                            use_account_compute_creds=None,
                            compute_access_key=None,
                            compute_secret_key=None,
                            compute_external_id=None,
                            compute_role_arn = None,
                            role_instance_profile=None,
                            compute_tenant_id=None,
                            compute_subscription_id=None,
                            compute_client_id=None,
                            compute_client_secret=None,
                            compute_user_id=None,
                            compute_key_finger_print=None,
                            compute_api_private_rsa_key=None):
        self.cloud_config['compute_config'] = {}
        self.cloud_config['compute_config']['compute_validated'] = compute_validated
        self.cloud_config['compute_config']['use_account_compute_creds'] = use_account_compute_creds

        self.cloud_config['compute_config']['compute_access_key'] = compute_access_key
        self.cloud_config['compute_config']['compute_secret_key'] = compute_secret_key
        self.cloud_config['compute_config']['role_instance_profile'] = role_instance_profile
        self.cloud_config['compute_config']['compute_external_id'] = compute_external_id
        self.cloud_config['compute_config']['compute_role_arn'] = compute_role_arn

        self.cloud_config['compute_config']['compute_tenant_id'] = compute_tenant_id
        self.cloud_config['compute_config']['compute_subscription_id'] = compute_subscription_id
        self.cloud_config['compute_config']['compute_client_id'] = compute_client_id
        self.cloud_config['compute_config']['compute_client_secret'] = compute_client_secret

        self.cloud_config['compute_config']['compute_user_id'] = compute_user_id
        self.cloud_config['compute_config']['compute_key_finger_print'] = compute_key_finger_print
        self.cloud_config['compute_config']['compute_api_private_rsa_key'] = compute_api_private_rsa_key

    def set_location(self, location=None, aws_region=None, aws_availability_zone=None,
                     availability_domain=None, region=None):
        self.cloud_config['location'] = {}
        self.cloud_config['location']['location'] = location

        self.cloud_config['location']['aws_region'] = aws_region
        self.cloud_config['location']['aws_availability_zone'] = aws_availability_zone

        self.cloud_config['location']['region'] = region
        self.cloud_config['location']['availability_domain'] = availability_domain

    def set_network_config(self, vpc_id=None,
                            subnet_id=None,
                            bastion_node_public_dns=None,
                            persistent_security_groups=None,
                            persistent_security_group_resource_group_name=None,
                            persistent_security_group_name=None,
                            master_elastic_ip=None,
                            vnet_name =None,
                            subnet_name=None,
                            vnet_resource_group_name=None,
                            vcn_id=None,
                            compartment_id=None,
                            image_id=None):
        self.cloud_config['network_config'] = {}
        self.cloud_config['network_config']['bastion_node_public_dns'] = bastion_node_public_dns
        self.cloud_config['network_config']['persistent_security_groups'] = persistent_security_groups
        self.cloud_config['network_config']['bastion_node_public_dns'] = bastion_node_public_dns
        self.cloud_config['network_config']['persistent_security_group_resource_group_name'] = persistent_security_group_resource_group_name
        self.cloud_config['network_config']['persistent_security_group_name'] = persistent_security_group_name
        self.cloud_config['network_config']['master_elastic_ip'] = master_elastic_ip

        self.cloud_config['network_config']['vpc_id'] = vpc_id
        self.cloud_config['network_config']['subnet_id'] = subnet_id

        self.cloud_config['network_config']['vnet_name'] = vnet_name
        self.cloud_config['network_config']['subnet_name'] = subnet_name
        self.cloud_config['network_config']['vnet_resource_group_name'] = vnet_resource_group_name

        self.cloud_config['network_config']['vcn_id'] = vcn_id
        self.cloud_config['network_config']['compartment_id'] = compartment_id
        self.cloud_config['network_config']['image_id'] = image_id

    def set_storage_config(self, storage_access_key=None,
                            storage_account_name=None,
                            disk_storage_account_name=None,
                            disk_storage_account_resource_group_name=None,
                            data_disk_count=None,
                            data_disk_size=None,
                            managed_disk_account_type=None,
                            storage_tenant_id=None,
                            storage_user_id=None,
                            storage_key_finger_print=None,
                            storage_api_private_rsa_key=None):
        self.cloud_config['storage_config'] = {}
        self.cloud_config['storage_config']['storage_access_key'] = storage_access_key
        self.cloud_config['storage_config']['storage_account_name'] = storage_account_name
        self.cloud_config['storage_config']['disk_storage_account_name'] = disk_storage_account_name
        self.cloud_config['storage_config']['disk_storage_account_resource_group_name'] \
            = disk_storage_account_resource_group_name
        self.cloud_config['storage_config']['data_disk_count'] = data_disk_count
        self.cloud_config['storage_config']['data_disk_size'] = data_disk_size
        self.cloud_config['storage_config']['managed_disk_account_type'] = managed_disk_account_type
        self.cloud_config['storage_config']['storage_tenant_id'] = storage_tenant_id
        self.cloud_config['storage_config']['storage_user_id'] = storage_user_id
        self.cloud_config['storage_config']['storage_key_finger_print'] = storage_key_finger_print
        self.cloud_config['storage_config']['storage_api_private_rsa_key'] = storage_api_private_rsa_key

    def set_engine_config(self, flavour=None,
                            custom_hadoop_config =None,
                            use_qubole_placement_policy=None,
                            enable_rubix=None,
                            node_bootstrap_timeout=None,
                            presto_version=None,
                            custom_presto_config=None,
                            spark_version=None,
                            custom_spark_config=None,
                            dbtap_id=None,
                            fernet_key=None,
                            overrides=None,
                            kafka_brokers=None,
                            kafka_version=None
                            ):

        self.set_hadoop_settings(flavour, custom_hadoop_config, use_qubole_placement_policy, node_bootstrap_timeout, enable_rubix)
        self.set_presto_settings(flavour, presto_version, custom_presto_config)
        self.set_spark_settings(flavour, spark_version, custom_spark_config)
        self.set_airflow_settings(flavour, dbtap_id, fernet_key, overrides)
        self.set_streamx_settings(flavour, kafka_brokers, kafka_version)

    def set_hadoop_settings(self,
                            flavour,
                            custom_hadoop_config=None,
                            use_qubole_placement_policy=None,
                            enable_rubix=None,
                            node_bootstrap_timeout=None):
        self.engine_config['flavour'] = flavour
        self.engine_config['hadoop_settings'] = {}
        self.engine_config['hadoop_settings']['custom_hadoop_config'] = custom_hadoop_config
        self.engine_config['hadoop_settings']['use_qubole_placement_policy'] = use_qubole_placement_policy
        self.engine_config['hadoop_settings']['enable_rubix'] = enable_rubix
        self.engine_config['hadoop_settings']['node_bootstrap_timeout'] = node_bootstrap_timeout

    def set_fairscheduler_settings(self, fairscheduler_config_xml=None, default_pool=None):
        self.engine_config['hadoop_settings']['fairscheduler_settings'] = {}
        self.engine_config['hadoop_settings']['fairscheduler_settings']['fairscheduler_config_xml'] = \
            fairscheduler_config_xml
        self.engine_config['hadoop_settings']['fairscheduler_settings']['default_pool'] = default_pool

    def set_presto_settings(self, flavour=None,
                            presto_version=None,
                            custom_presto_config=None):
        self.engine_config['flavour'] = flavour
        self.engine_config['presto_settings'] = {}
        self.engine_config['presto_settings']['presto_version'] = presto_version
        self.engine_config['presto_settings']['custom_presto_config'] = custom_presto_config

    def set_spark_settings(self, flavour=None, spark_version=None, custom_spark_config=None):
        self.engine_config['flavour'] = flavour
        self.engine_config['spark_settings'] = {}
        self.engine_config['spark_settings']['spark_version'] = spark_version
        self.engine_config['spark_settings']['custom_spark_config'] = custom_spark_config

    def set_airflow_settings(self, flavour=None,
                             dbtap_id=None,
                             fernet_key=None,
                             overrides=None):
        self.engine_config['flavour'] = flavour
        self.engine_config['airflow_settings'] = {}
        self.engine_config['airflow_settings']['dbtap_id'] = dbtap_id
        self.engine_config['airflow_settings']['fernet_key'] = fernet_key
        self.engine_config['airflow_settings']['overrides'] = overrides

    def set_streamx_settings(self,
                                flavour,
                                kafka_brokers=None,
                                kafka_version=None):

        self.engine_config['flavour'] = flavour
        self.engine_config['streamx_settings'] = {}
        self.engine_config['streamx_settings']['kafka_brokers'] = kafka_brokers
        self.engine_config['streamx_settings']['kafka_version'] = kafka_version

    def set_monitoring(self, enable_ganglia_monitoring=None, datadog_api_token=None, datadog_app_token=None):
        self.monitoring['ganglia'] = enable_ganglia_monitoring
        self.set_datadog_settings(datadog_api_token, datadog_app_token)

    def set_provider(self, provider=None):
        self.cloud_config['provider'] = provider

    def set_datadog_settings(self, datadog_api_token=None, datadog_app_token=None):
        self.monitoring['datadog'] = {}
        self.monitoring['datadog']['datadog_api_token'] = datadog_api_token
        self.monitoring['datadog']['datadog_app_token'] = datadog_app_token

    def set_cluster_information(self, master_instance_type=None,
                                slave_instance_type=None,
                                min_nodes=1,
                                max_nodes=1,
                                cluster_name=None,
                                node_bootstrap=None,
                                disallow_cluster_termination=None,
                                force_tunnel=None,
                                fallback_to_ondemand=None,
                                customer_ssh_key=None,
                                custom_tags=None,
                                heterogeneous_config=None,
                                slave_request_type=None,
                                idle_cluster_timeout=None):
        self.cluster_info['master_instance_type'] = master_instance_type
        self.cluster_info['slave_instance_type']  = slave_instance_type
        self.cluster_info['min_nodes'] = min_nodes
        self.cluster_info['max_nodes'] = max_nodes
        self.cluster_info['cluster_name'] = cluster_name
        self.cluster_info['node_bootstrap'] = node_bootstrap
        self.cluster_info['disallow_cluster_termination'] = disallow_cluster_termination
        self.cluster_info['force_tunnel'] = force_tunnel
        self.cluster_info['fallback_to_ondemand'] = fallback_to_ondemand
        self.cluster_info['customer_ssh_key'] = customer_ssh_key
        if custom_tags and custom_tags.strip():
            try:
                self.cluster_info['custom_tags'] = json.loads(custom_tags.strip())
            except Exception as e:
                raise Exception("Invalid JSON string for custom ec2 tags: %s" % e.message)

        self.cluster_info['heterogeneous_config'] = heterogeneous_config
        self.cluster_info['slave_request_type'] = slave_request_type
        self.cluster_info['idle_cluster_timeout'] = idle_cluster_timeout

    def set_spot_block_settings(self, spot_block_duration=None):
        self.cluster_info['spot_block_settings'] = {}
        self.cluster_info['spot_block_settings']['duration'] = spot_block_duration



    def set_spot_instance_settings(self, maximum_bid_price_percentage=100,
                                   timeout_for_request=10,
                                    maximum_spot_instance_percentage=50):
        self.cluster_info['spot_settings']['spot_instance_settings'] = {}
        self.cluster_info['spot_settings']['spot_instance_settings']['maximum_bid_price_percentage'] = \
            maximum_bid_price_percentage
        self.cluster_info['spot_settings']['spot_instance_settings']['timeout_for_request'] = timeout_for_request
        self.cluster_info['spot_settings']['spot_instance_settings']['maximum_spot_instance_percentage'] = \
            maximum_spot_instance_percentage

    def set_stable_spot_bid_settings(self, stable_maximum_bid_price_percentage=150,
                                    stable_timeout_for_request=10,
                                    stable_allow_fallback=None):
        self.cluster_info['spot_settings']['stable_spot_bid_settings'] = {}
        self.cluster_info['spot_settings']['stable_spot_bid_settings']['maximum_bid_price_percentage'] = \
            stable_maximum_bid_price_percentage
        self.cluster_info['spot_settings']['stable_spot_bid_settings']['timeout_for_request'] = \
            stable_timeout_for_request

    def set_data_disk(self, size=0,
                      count=0,
                      disk_type=None,
                      upscaling_config=None,
                      enable_encryption=False):
        self.cluster_info['datadisk'] = {}
        self.cluster_info['datadisk']['size'] = size
        self.cluster_info['datadisk']['count'] = count
        self.cluster_info['datadisk']['type'] = disk_type
        self.cluster_info['datadisk']['upscaling_config'] = upscaling_config
        self.cluster_info['datadisk']['encryption'] = enable_encryption

    def set_cluster_info(self, compute_validated=None,
                        use_account_compute_creds=None,
                        compute_client_id=None,
                        compute_client_secret=None,
                        compute_tenant_id=None,
                        compute_access_key=None,
                        compute_secret_key=None,
                        compute_external_id=None,
                        compute_role_arn=None,
                        compute_user_id=None,
                        compute_key_finger_print=None,
                        compute_api_private_rsa_key=None,
                        role_instance_profile=None,
                        compute_subscription_id=None,
                        location=None,
                        aws_region=None,
                        aws_availability_zone=None,
                        availability_domain=None,
                        region=None,
                        vpc_id=None,
                        subnet_id=None,
                        bastion_node_public_dns=None,
                        persistent_security_groups=None,
                        persistent_security_group_resource_group_name=None,
                        persistent_security_group_name=None,
                        master_elastic_ip=None,
                        subnet_name=None,
                        vnet_resource_group_name=None,
                        vcn_id=None,
                        compartment_id=None,
                        image_id=None,
                        vnet_name=None,
                        storage_access_key=None,
                        storage_account_name=None,
                        disk_storage_account_name=None,
                        disk_storage_account_resource_group_name=None,
                        data_disk_count=None,
                        data_disk_size=None,
                        managed_disk_account_type=None,
                        storage_tenant_id=None,
                        storage_user_id=None,
                        storage_key_finger_print=None,
                        storage_api_private_rsa_key=None,
                        flavour=None,
                        custom_hadoop_config=None,
                        use_qubole_placement_policy=None,
                        enable_rubix=None,
                        presto_version=None,
                        custom_presto_config=None,
                        spark_version=None,
                        custom_spark_config=None,
                        provider=None,
                        dbtap_id=None,
                        fernet_key=None,
                        overrides=None,
                        fairscheduler_config_xml=None,
                        default_pool=None,
                        enable_ganglia_monitoring=None,
                        datadog_api_token=None,
                        datadog_app_token=None,
                        master_instance_type=None,
                        slave_instance_type=None,
                        min_nodes=1,
                        max_nodes=1,
                        cluster_name=None,
                        node_bootstrap=None,
                        disallow_cluster_termination=None,
                        force_tunnel=None,
                        fallback_to_ondemand=None,
                        customer_ssh_key=None,
                        custom_tags=None,
                        heterogeneous_config=None,
                        slave_request_type=None,
                        idle_cluster_timeout=None,
                        size=0,
                        count=0,
                        disk_type=None,
                        upscaling_config=None,
                        enable_encryption=False,
                        maximum_bid_price_percentage=100,
                        timeout_for_request=10,
                        maximum_spot_instance_percentage=50,
                        stable_maximum_bid_price_percentage=150,
                        stable_timeout_for_request=10,
                        kafka_brokers=None,
                        kafka_version=None,
                        spot_block_duration=None,
                        node_bootstrap_timeout=None,
                         **kwargs):
        self.set_compute_config(compute_validated,
                                use_account_compute_creds,
                                compute_access_key,
                                compute_secret_key,
                                compute_external_id,
                                compute_role_arn,
                                role_instance_profile,
                                compute_tenant_id,
                                compute_subscription_id,
                                compute_client_id,
                                compute_client_secret,
                                compute_user_id,
                                compute_key_finger_print,
                                compute_api_private_rsa_key)
        self.set_location(location, aws_region, aws_availability_zone, availability_domain, region)
        self.set_provider(provider)
        self.set_network_config(vpc_id,
                                subnet_id,
                                bastion_node_public_dns,
                                persistent_security_groups,
                                persistent_security_group_resource_group_name,
                                persistent_security_group_name,
                                master_elastic_ip,
                                vnet_name,
                                subnet_name,
                                vnet_resource_group_name,
                                vcn_id,
                                compartment_id,
                                image_id)
        self.set_storage_config(storage_access_key,
                                storage_account_name,
                                disk_storage_account_name,
                                disk_storage_account_resource_group_name,
                                data_disk_count,
                                data_disk_size,
                                managed_disk_account_type,
                                storage_tenant_id,
                                storage_user_id,
                                storage_key_finger_print,
                                storage_api_private_rsa_key)
        self.set_engine_config(flavour,
                                custom_hadoop_config,
                                use_qubole_placement_policy,
                                enable_rubix,
                                node_bootstrap_timeout,
                                presto_version,
                                custom_presto_config,
                                spark_version,
                                custom_spark_config,
                                dbtap_id,
                                fernet_key,
                                overrides,
                                kafka_brokers,
                                kafka_version)
        self.set_fairscheduler_settings(fairscheduler_config_xml,
                                        default_pool)
        self.set_monitoring(enable_ganglia_monitoring,
                            datadog_api_token,
                            datadog_app_token)
        self.set_cluster_information(master_instance_type,
                                    slave_instance_type,
                                    min_nodes,
                                    max_nodes,
                                    cluster_name,
                                    node_bootstrap,
                                    disallow_cluster_termination,
                                    force_tunnel,
                                    fallback_to_ondemand,
                                    customer_ssh_key,
                                    custom_tags,
                                    heterogeneous_config,
                                    slave_request_type,
                                    idle_cluster_timeout)
        self.set_data_disk(size,
                           count,
                           disk_type,
                           upscaling_config,
                           enable_encryption)
        self.set_spot_block_settings(spot_block_duration)
        self.cluster_info['spot_settings'] = {}
        self.set_spot_instance_settings(maximum_bid_price_percentage,
                                        timeout_for_request,
                                        maximum_spot_instance_percentage)
        self.set_stable_spot_bid_settings(stable_maximum_bid_price_percentage,
                                        stable_timeout_for_request)


def _make_minimal(dictionary):
    """
    This function removes all the keys whose value is either None or an empty
    dictionary.
    """
    new_dict = {}
    for key, value in dictionary.items():
        if value is not None:
            if isinstance(value, dict):
                new_value = _make_minimal(value)
                if new_value:
                    new_dict[key] = new_value
            else:
                new_dict[key] = value
    return new_dict

