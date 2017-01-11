#import qds_sdk.cloud.cloud
class AwsCloud():
    def __init__(self):
        f =5

    def cloud_config(self, argparse):

        # compute settings
        compute_config = argparse.add_argument_group("compute config settings")
        compute_config.add_argument("--use-account-compute-creds",
                                    dest="use_account_compute_creds",
                                    default=None,
                                    help="secret key for aws cluster")
        compute_config.add_argument("--compute-access-key",
                                    dest="compute_access_key",
                                    default=None,
                                    help="access key for aws cluster")
        compute_config.add_argument("--compute-secret-key",
                                    dest="compute_secret_key",
                                    default=None,
                                    help="secret key for aws cluster")
        compute_config.add_argument("--role-instance-profile",
                                    dest="role_instance_profile",
                                    help="IAM Role instance profile to attach on cluster", )

        # location settings
        location_group = argparse.add_argument_group("location config")
        location_group.add_argument("--aws-region",
                                    dest="aws_region",
                                    choices=["us-east-1", "us-west-2", "ap-northeast-1", "sa-east-1",
                                             "eu-west-1", "ap-southeast-1", "us-west-1"],
                                    help="aws region to create the cluster in", )
        location_group.add_argument("--aws-availability-zone",
                                    dest="aws_availability_zone",
                                    help="availability zone to" +
                                         " create the cluster in", )

        # network settings
        network_config_group = argparse.add_argument_group("network config settings")
        network_config_group.add_argument("--vpc-id",
                                          dest="vpc_id",
                                          help="vpc to create the cluster in", )
        network_config_group.add_argument("--subnet-id",
                                          dest="subnet_id",
                                          help="subnet to create the cluster in", )
        network_config_group.add_argument("--bastion-node-public-dns",
                                          dest="bastion_node_public_dns",
                                          help="public dns name of the bastion node. Required only if cluster is in private subnet of a EC2-VPC", )
        network_config_group.add_argument("--persistent-security-groups",
                                          dest="persistent_security_groups",
                                          help="a security group to associate with each" +
                                               " node of the cluster. Typically used" +
                                               " to provide access to external hosts", )
        network_config_group.add_argument("--master-elastic-ip",
                                          dest="master_elastic_ip",
                                          help="master elastic ip for cluster")




