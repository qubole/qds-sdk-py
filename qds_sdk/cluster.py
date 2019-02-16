"""
The cluster module contains the definitions for retrieving and manipulating
cluster information.
"""

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser
from qds_sdk import util

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
        pagination_group = group.add_argument_group()
        pagination_group.add_argument("--page", dest="page", action="store", type=int,
                           help="page number")
        pagination_group.add_argument("--per-page", dest="per_page", action="store", type=int,
                           help="number of clusters to be retrieved per page")

        arguments = argparser.parse_args(args)
        return vars(arguments)

    @classmethod
    def list(cls, state=None, page=None, per_page=None):
        """
        List existing clusters present in your account.

        Kwargs:
            `state`: list only those clusters which are in this state

        Returns:
            List of clusters satisfying the given criteria
        """
        conn = Qubole.agent()
        params = {}
        if page:
            params['page'] = page
        if per_page:
            params['per_page'] = per_page
        if (params.get('page') or params.get('per_page')) and Qubole.version == 'v1.2':
            log.warn("Pagination is not supported with API v1.2. Fetching all clusters.")
        params = None if not params else params
        cluster_list = conn.get(cls.rest_entity_path, params=params)
        if state is None:
            return cluster_list
        elif state is not None:
            result = []
            if Qubole.version == 'v1.2':
                for cluster in cluster_list:
                    if state.lower() == cluster['cluster']['state'].lower():
                        result.append(cluster)
            elif Qubole.version == 'v1.3':
                cluster_list = cluster_list['clusters']
                for cluster in cluster_list:
                    if state.lower() == cluster['state'].lower():
                        result.append(cluster)
            return result

    @classmethod
    def show(cls, cluster_id_label):
        """
        Show information about the cluster with id/label `cluster_id_label`.
        """
        conn = Qubole.agent()
        return conn.get(cls.element_path(cluster_id_label))

    @classmethod
    def status(cls, cluster_id_label):
        """
        Show the status of the cluster with id/label `cluster_id_label`.
        """
        conn = Qubole.agent(version=Cluster.api_version)
        return conn.get(cls.element_path(cluster_id_label) + "/state")

    @classmethod
    def master(cls, cluster_id_label):
        """
        Show the details of the master of the cluster with id/label `cluster_id_label`.
        """
        cluster_status = cls.status(cluster_id_label)
        if cluster_status.get("state") == 'UP':
            return list(filter(lambda x: x["role"] == "master", cluster_status.get("nodes")))[0]
        else:
            return cluster_status

    @classmethod
    def start(cls, cluster_id_label, api_version=None):
        """
        Start the cluster with id/label `cluster_id_label`.
        """
        conn = Qubole.agent(version=api_version)
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

        argparser.add_argument("--label", dest="label",
                               nargs="+", required=(create_required or label_required),
                               help="list of labels for the cluster" +
                                    " (atleast one label is required)")

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
                                    " create the cluster in",)
        ec2_group.add_argument("--subnet-id",
                               dest="subnet_id",
                               help="subnet to create the cluster in",)
        ec2_group.add_argument("--vpc-id",
                               dest="vpc_id",
                               help="vpc to create the cluster in",)
        ec2_group.add_argument("--master-elastic-ip",
                               dest="master_elastic_ip",
                               help="elastic ip to attach to master",)
        ec2_group.add_argument("--bastion-node-public-dns",
                               dest="bastion_node_public_dns",
                               help="public dns name of the bastion node. Required only if cluster is in private subnet of a EC2-VPC",)
        ec2_group.add_argument("--role-instance-profile",
                               dest="role_instance_profile",
                               help="IAM Role instance profile to attach on cluster",)

        hadoop_group = argparser.add_argument_group("hadoop settings")
        node_config_group = argparser.add_argument_group("node configuration") if (api_version >= 1.3) else hadoop_group

        node_config_group.add_argument("--master-instance-type",
                                  dest="master_instance_type",
                                  help="instance type to use for the hadoop" +
                                       " master node",)
        node_config_group.add_argument("--slave-instance-type",
                                  dest="slave_instance_type",
                                  help="instance type to use for the hadoop" +
                                       " slave nodes",)
        node_config_group.add_argument("--initial-nodes",
                                  dest="initial_nodes",
                                  type=int,
                                  help="number of nodes to start the" +
                                       " cluster with",)
        node_config_group.add_argument("--max-nodes",
                                  dest="max_nodes",
                                  type=int,
                                  help="maximum number of nodes the cluster" +
                                       " may be auto-scaled up to")
        node_config_group.add_argument("--slave-request-type",
                                  dest="slave_request_type",
                                  choices=["ondemand", "spot", "hybrid", "spotblock"],
                                  help="purchasing option for slave instaces",)
        node_config_group.add_argument("--root-volume-size",
                                       dest="root_volume_size",
                                       type=int,
                                       help="size of root volume in GB")
        hadoop_group.add_argument("--custom-config",
                                  dest="custom_config_file",
                                  help="location of file containg custom" +
                                       " hadoop configuration overrides")
        hadoop_group.add_argument("--use-hbase", dest="use_hbase",
                                  action="store_true", default=None,
                                  help="Use hbase on this cluster",)
        hadoop_group.add_argument("--is-ha", dest="is_ha",
                                  action="store_true", default=None,
                                  help="Enable HA config for cluster")
        if api_version >= 1.3:
          qubole_placement_policy_group = hadoop_group.add_mutually_exclusive_group()
          qubole_placement_policy_group.add_argument("--use-qubole-placement-policy",
                                              dest="use_qubole_placement_policy",
                                              action="store_true",
                                              default=None,
                                              help="Use Qubole Block Placement policy" +
                                                   " for clusters with spot nodes",)
          qubole_placement_policy_group.add_argument("--no-use-qubole-placement-policy",
                                              dest="use_qubole_placement_policy",
                                              action="store_false",
                                              default=None,
                                              help="Do not use Qubole Block Placement policy" +
                                                   " for clusters with spot nodes",)
          fallback_to_ondemand_group = node_config_group.add_mutually_exclusive_group()
          fallback_to_ondemand_group.add_argument("--fallback-to-ondemand",
                                 dest="fallback_to_ondemand",
                                 action="store_true",
                                 default=None,
                                 help="Fallback to on-demand nodes if spot nodes" +
                                 " could not be obtained. Valid only if slave_request_type is spot",)
          fallback_to_ondemand_group.add_argument("--no-fallback-to-ondemand",
                                 dest="fallback_to_ondemand",
                                 action="store_false",
                                 default=None,
                                 help="Dont Fallback to on-demand nodes if spot nodes" +
                                 " could not be obtained. Valid only if slave_request_type is spot",)
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
          ebs_volume_group = argparser.add_argument_group("ebs volume settings")
          ebs_volume_group.add_argument("--ebs-volume-count",
                                  dest="ebs_volume_count",
                                  type=int,
                                  help="Number of EBS volumes to attach to" +
                                       " each instance of the cluster",)
          ebs_volume_group.add_argument("--ebs-volume-type",
                                  dest="ebs_volume_type",
                                  choices=["standard", "gp2"],
                                  help=" of the EBS volume. Valid values are " +
                                       "'standard' (magnetic) and 'gp2' (ssd).",)
          ebs_volume_group.add_argument("--ebs-volume-size",
                                  dest="ebs_volume_size",
                                  type=int,
                                  help="Size of each EBS volume, in GB",)
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

        spot_group = argparser.add_argument_group("spot instance settings" +
                    " (valid only when slave-request-type is hybrid or spot)")
        spot_group.add_argument("--maximum-bid-price-percentage",
                                dest="maximum_bid_price_percentage",
                                type=float,
                                help="maximum value to bid for spot instances" +
                                     " expressed as a percentage of the base" +
                                     " price for the slave node instance type",)
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
                                     " is 'hybrid'",)

        stable_spot_group = argparser.add_argument_group("stable spot instance settings")
        stable_spot_group.add_argument("--stable-maximum-bid-price-percentage",
                                       dest="stable_maximum_bid_price_percentage",
                                       type=float,
                                       help="maximum value to bid for stable node spot instances" +
                                       " expressed as a percentage of the base" +
                                       " price for the master and slave node instance types",)
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

        spot_block_group = argparser.add_argument_group("spot block settings")
        spot_block_group.add_argument("--spot-block-duration",
                                      dest="spot_block_duration",
                                      type=int,
                                      help="spot block duration" +
                                           " unit: minutes")

        fairscheduler_group = argparser.add_argument_group(
                              "fairscheduler configuration options")
        fairscheduler_group.add_argument("--fairscheduler-config-xml",
                                         dest="fairscheduler_config_xml_file",
                                         help="location for file containing" +
                                              " xml with custom configuration" +
                                              " for the fairscheduler",)
        fairscheduler_group.add_argument("--fairscheduler-default-pool",
                                         dest="default_pool",
                                         help="default pool for the" +
                                              " fairscheduler",)

        security_group = argparser.add_argument_group("security setttings")
        ephemerals = security_group.add_mutually_exclusive_group()
        ephemerals.add_argument("--encrypted-ephemerals",
                                 dest="encrypted_ephemerals",
                                 action="store_true",
                                 default=None,
                                 help="encrypt the ephemeral drives on" +
                                      " the instance",)
        ephemerals.add_argument("--no-encrypted-ephemerals",
                                 dest="encrypted_ephemerals",
                                 action="store_false",
                                 default=None,
                                 help="don't encrypt the ephemeral drives on" +
                                      " the instance",)

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
                                  help="Enable presto for this cluster",)
        enabling_presto.add_argument("--disable-presto",
                                  dest="enable_presto",
                                  action="store_false",
                                  default=None,
                                  help="Disable presto for this cluster",)
        presto_group.add_argument("--presto-custom-config",
                                  dest="presto_custom_config_file",
                                  help="location of file containg custom" +
                                       " presto configuration overrides")

        termination = argparser.add_mutually_exclusive_group()
        termination.add_argument("--disallow-cluster-termination",
                                 dest="disallow_cluster_termination",
                                 action="store_true",
                                 default=None,
                                 help="don't auto-terminate idle clusters," +
                                      " use this with extreme caution",)
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
                                  " cluster",)
        ganglia.add_argument("--disable-ganglia-monitoring",
                             dest="enable_ganglia_monitoring",
                             action="store_false",
                             default=None,
                             help="disable ganglia monitoring for the" +
                                  " cluster",)

        argparser.add_argument("--node-bootstrap-file",
                dest="node_bootstrap_file",
                help="""name of the node bootstrap file for this cluster. It
                should be in stored in S3 at
                <account-default-location>/scripts/hadoop/NODE_BOOTSTRAP_FILE
                """,)

        argparser.add_argument("--custom-ec2-tags",
                               dest="custom_ec2_tags",
                               help="""Custom ec2 tags to be set on all instances
                               of the cluster. Specified as JSON object (key-value pairs)
                               e.g. --custom-ec2-tags '{"key1":"value1", "key2":"value2"}'
                               """,)
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
        conn = Qubole.agent(version=Cluster.api_version)
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
        conn = Qubole.agent(version=Cluster.api_version)
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
        conn = Qubole.agent(version=Cluster.api_version)
        return conn.get(cls.element_path(cluster_id_label) + "/snapshot_schedule")

    @classmethod
    def update_snapshot_schedule(cls, cluster_id_label, s3_location=None, frequency_unit=None, frequency_num=None, status=None):
        """
        Update for snapshot schedule
        """
        conn = Qubole.agent(version=Cluster.api_version)

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
      conn = Qubole.agent(version=Cluster.api_version)
      parameters = {} if not parameters else parameters
      return conn.post(cls.element_path(cluster_id_label) + "/nodes", data={"parameters" : parameters})

    @classmethod
    def remove_node(cls, cluster_id_label, private_dns, parameters=None):
        """
        Add a node to an existing cluster
        """
        conn = Qubole.agent(version=Cluster.api_version)
        parameters = {} if not parameters else parameters
        data = {"private_dns" : private_dns, "parameters" : parameters}
        return conn.delete(cls.element_path(cluster_id_label) + "/nodes", data)

    @classmethod
    def update_node(cls, cluster_id_label, command, private_dns, parameters=None):
        """
        Add a node to an existing cluster
        """
        conn = Qubole.agent(version=Cluster.api_version)
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
                         master_elastic_ip=None,
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
        self.ec2_settings['master_elastic_ip'] = master_elastic_ip
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
                            use_spark=None,
                            is_ha=None):
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

        `is_ha` : enable HA config for cluster

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
        self.hadoop_settings['is_ha'] = is_ha

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
        return util._make_minimal(payload)

class ClusterInfoV13():
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
        self.node_configuration = {}

    def set_cluster_info(self, aws_access_key_id=None,
                         aws_secret_access_key=None,
                         aws_region=None,
                         aws_availability_zone=None,
                         vpc_id=None,
                         subnet_id=None,
                         master_elastic_ip=None,
                         disallow_cluster_termination=None,
                         enable_ganglia_monitoring=None,
                         node_bootstrap_file=None,
                         master_instance_type=None,
                         slave_instance_type=None,
                         initial_nodes=None,
                         max_nodes=None,
                         slave_request_type=None,
                         fallback_to_ondemand=None,
                         node_base_cooldown_period=None,
                         node_spot_cooldown_period=None,
                         custom_config=None,
                         use_hbase=None,
                         custom_ec2_tags=None,
                         use_hadoop2=None,
                         use_spark=None,
                         use_qubole_placement_policy=None,
                         maximum_bid_price_percentage=None,
                         timeout_for_request=None,
                         maximum_spot_instance_percentage=None,
                         stable_maximum_bid_price_percentage=None,
                         stable_timeout_for_request=None,
                         stable_allow_fallback=True,
                         spot_block_duration=None,
                         ebs_volume_count=None,
                         ebs_volume_type=None,
                         ebs_volume_size=None,
                         root_volume_size=None,
                         fairscheduler_config_xml=None,
                         default_pool=None,
                         encrypted_ephemerals=None,
                         ssh_public_key=None,
                         persistent_security_group=None,
                         enable_presto=None,
                         bastion_node_public_dns=None,
                         role_instance_profile=None,
                         presto_custom_config=None,
                         is_ha=None,
                         env_name=None,
                         python_version=None,
                         r_version=None,
                         enable_rubix=None):
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

        `master_elastic_ip`: Elastic IP to attach to master node

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

        `node_base_cooldown_period`: Time for which an on-demand node waits before termination (Unit: minutes)

        `node_spot_cooldown_period`: Time for which a spot node waits before termination (Unit: minutes)

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

        `spot_block_duration`: Time for which the spot block instance is provisioned (Unit:
            minutes)

        `ebs_volume_count`: Number of EBS volumes to attach 
            to each instance of the cluster.

        `ebs_volume_type`: Type of the EBS volume. Valid 
            values are 'standard' (magnetic) and 'ssd'.

        `ebs_volume_size`: Size of each EBS volume, in GB. 

        `root_volume_size`: Size of root volume, in GB.

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

        `is_ha`: Enabling HA config for cluster

        `env_name`: Name of python and R environment. (For Spark clusters)

        `python_version`: Version of Python for environment. (For Spark clusters)

        `r_version`: Version of R for environment. (For Spark clusters)

        `enable_rubix`: Enable rubix on the cluster (For Presto clusters)
        """

        self.disallow_cluster_termination = disallow_cluster_termination
        self.enable_ganglia_monitoring = enable_ganglia_monitoring
        self.node_bootstrap_file = node_bootstrap_file
        self.set_node_configuration(master_instance_type, slave_instance_type, initial_nodes, max_nodes,
                                    slave_request_type, fallback_to_ondemand, custom_ec2_tags,
                                    node_base_cooldown_period, node_spot_cooldown_period, root_volume_size)
        self.set_ec2_settings(aws_access_key_id, aws_secret_access_key, aws_region, aws_availability_zone, vpc_id, subnet_id,
                              master_elastic_ip, bastion_node_public_dns, role_instance_profile)
        self.set_hadoop_settings(custom_config, use_hbase, use_hadoop2, use_spark, use_qubole_placement_policy, is_ha, enable_rubix)
        self.set_spot_instance_settings(maximum_bid_price_percentage, timeout_for_request, maximum_spot_instance_percentage)
        self.set_stable_spot_instance_settings(stable_maximum_bid_price_percentage, stable_timeout_for_request, stable_allow_fallback)
        self.set_spot_block_settings(spot_block_duration)
        self.set_ebs_volume_settings(ebs_volume_count, ebs_volume_type, ebs_volume_size)
        self.set_fairscheduler_settings(fairscheduler_config_xml, default_pool)
        self.set_security_settings(encrypted_ephemerals, ssh_public_key, persistent_security_group)
        self.set_presto_settings(enable_presto, presto_custom_config)
        self.set_env_settings(env_name, python_version, r_version)

    def set_ec2_settings(self,
                         aws_access_key_id=None,
                         aws_secret_access_key=None,
                         aws_region=None,
                         aws_availability_zone=None,
                         vpc_id=None,
                         subnet_id=None,
                         master_elastic_ip=None,
                         bastion_node_public_dns=None,
                         role_instance_profile=None):
        self.ec2_settings['compute_access_key'] = aws_access_key_id
        self.ec2_settings['compute_secret_key'] = aws_secret_access_key
        self.ec2_settings['aws_region'] = aws_region
        self.ec2_settings['aws_preferred_availability_zone'] = aws_availability_zone
        self.ec2_settings['vpc_id'] = vpc_id
        self.ec2_settings['subnet_id'] = subnet_id
        self.ec2_settings['master_elastic_ip'] = master_elastic_ip
        self.ec2_settings['bastion_node_public_dns'] = bastion_node_public_dns
        self.ec2_settings['role_instance_profile'] = role_instance_profile

    def set_node_configuration(self, master_instance_type=None,
                            slave_instance_type=None,
                            initial_nodes=None,
                            max_nodes=None,
                            slave_request_type=None,
                            fallback_to_ondemand=None,
                            custom_ec2_tags=None,
                            node_base_cooldown_period=None,
                            node_spot_cooldown_period=None,
                            root_volume_size=None):
        self.node_configuration['master_instance_type'] = master_instance_type
        self.node_configuration['slave_instance_type'] = slave_instance_type
        self.node_configuration['initial_nodes'] = initial_nodes
        self.node_configuration['max_nodes'] = max_nodes
        self.node_configuration['slave_request_type'] = slave_request_type
        self.node_configuration['fallback_to_ondemand'] = fallback_to_ondemand
        self.node_configuration['node_base_cooldown_period'] = node_base_cooldown_period
        self.node_configuration['node_spot_cooldown_period'] = node_spot_cooldown_period
        self.node_configuration['root_volume_size'] = root_volume_size

        if custom_ec2_tags and custom_ec2_tags.strip():
            try:
                self.node_configuration['custom_ec2_tags'] = json.loads(custom_ec2_tags.strip())
            except Exception as e:
                raise Exception("Invalid JSON string for custom ec2 tags: %s" % e.message)

    def set_hadoop_settings(self, custom_config=None,
                            use_hbase=None,
                            use_hadoop2=None,
                            use_spark=None,
                            use_qubole_placement_policy=None,
                            is_ha=None,
                            enable_rubix=None):
        self.hadoop_settings['custom_config'] = custom_config
        self.hadoop_settings['use_hbase'] = use_hbase
        self.hadoop_settings['use_hadoop2'] = use_hadoop2
        self.hadoop_settings['use_spark'] = use_spark
        self.hadoop_settings['use_qubole_placement_policy'] = use_qubole_placement_policy
        self.hadoop_settings['is_ha'] = is_ha
        self.hadoop_settings['enable_rubix'] = enable_rubix

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

    def set_spot_block_settings(self, spot_block_duration=None):
        self.node_configuration['spot_block_settings'] = {'duration': spot_block_duration}

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

    def set_env_settings(self, env_name=None, python_version=None, r_version=None):
        self.node_configuration['env_settings'] = {}
        self.node_configuration['env_settings']['name'] = env_name
        self.node_configuration['env_settings']['python_version'] = python_version
        self.node_configuration['env_settings']['r_version'] = r_version

    def minimal_payload(self):
        """
        This method can be used to create the payload which is sent while
        creating or updating a cluster.
        """
        payload_dict = self.__dict__
        payload_dict.pop("api_version", None)
        return util._make_minimal(payload_dict)
