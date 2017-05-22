from qds_sdk.cloud.cloud import Cloud
class AwsCloud(Cloud):
    '''
    qds_sdk.cloud.AwsCloud is the class which stores information about aws cloud config settings.
    The objects of this class can be used to set aws cloud_config settings while create/update/clone a cluster.
    '''

    def __init__(self):
        self.compute_config = {}
        self.location = {}
        self.network_config = {}
        self.storage_config = {}

    def set_cloud_config(self,
                         compute_access_key=None,
                         compute_secret_key=None,
                         use_account_compute_creds=None,
                         aws_region=None,
                         aws_availability_zone=None,
                         role_instance_profile=None,
                         vpc_id=None,
                         subnet_id=None,
                         persistent_security_groups=None,
                         bastion_node_public_dns=None,
                         master_elastic_ip=None):
        '''

        Args:
            compute_access_key: The access key for customer's aws account. This
                is required for creating the cluster.

            compute_secret_key: The secret access key for customer's aws
                account. This is required for creating the cluster.

            use_account_compute_creds: Set it to true to use the account's compute
                credentials for all clusters of the account.The default value is false

            aws_region: The AWS region in which the cluster is created. The default value is, us-east-1.
                Valid values are, us-east-1, us-west-1, us-west-2, eu-west-1, sa-east1, ap-southeast-1,
                and ap-northeast-1.
                Doc: http://docs.qubole.com/en/latest/rest-api/cluster_api/create-new-cluster.html#ec2-settings

            aws_availability_zone: The preferred availability zone in which the cluster must be created. The default value is Any.

            role_instance_profile: IAM Role instance profile to attach on cluster

            vpc_id: The ID of the vpc in which the cluster is created.
                In this vpc, the enableDnsHostnames parameter must be set to true.

            subnet_id: The ID of the subnet in which the cluster is created. This subnet must belong to the
                above VPC and it can be a public/private subnet

            persistent_security_groups: security group to associate with each node of the cluster.
                Typically used to provide access to external hosts

            bastion_node_public_dns: Specify the Bastion host public DNS name if private subnet is provided.
                Do not specify this value for a public subnet.

            master_elastic_ip: It is the Elastic IP address for attaching to the cluster master

        '''

        self.set_compute_config(use_account_compute_creds, compute_access_key,
                                compute_secret_key, role_instance_profile)
        self.set_location(aws_region, aws_availability_zone)
        self.set_network_config(bastion_node_public_dns, persistent_security_groups,
                                master_elastic_ip, vpc_id, subnet_id)

    def set_compute_config(self,
                           use_account_compute_creds=None,
                           compute_access_key=None,
                           compute_secret_key=None,
                           role_instance_profile=None):
        self.compute_config['use_account_compute_creds'] = use_account_compute_creds
        self.compute_config['compute_access_key'] = compute_access_key
        self.compute_config['compute_secret_key'] = compute_secret_key
        self.compute_config['role_instance_profile'] = role_instance_profile

    def set_location(self,
                     aws_region=None,
                     aws_availability_zone=None):
        self.location['aws_region'] = aws_region
        self.location['aws_availability_zone'] = aws_availability_zone

    def set_network_config(self,
                           bastion_node_public_dns=None,
                           persistent_security_groups=None,
                           master_elastic_ip=None,
                           vpc_id=None,
                           subnet_id=None):
        self.network_config['bastion_node_public_dns'] = bastion_node_public_dns
        self.network_config['persistent_security_groups'] = persistent_security_groups
        self.network_config['master_elastic_ip'] = master_elastic_ip
        self.network_config['vpc_id'] = vpc_id
        self.network_config['subnet_id'] = subnet_id

    def set_cloud_config_from_arguments(self, arguments):
        self.set_cloud_config(compute_access_key=arguments.compute_access_key,
                              compute_secret_key=arguments.compute_secret_key,
                              use_account_compute_creds=arguments.use_account_compute_creds,
                              aws_region=arguments.aws_region,
                              aws_availability_zone=arguments.aws_availability_zone,
                              role_instance_profile=arguments.role_instance_profile,
                              vpc_id=arguments.vpc_id,
                              subnet_id=arguments.subnet_id,
                              persistent_security_groups=arguments.persistent_security_groups,
                              bastion_node_public_dns=arguments.bastion_node_public_dns,
                              master_elastic_ip=arguments.master_elastic_ip)

    def create_parser(self, argparser):
        # compute settings parser
        compute_config = argparser.add_argument_group("compute config settings")
        compute_creds = compute_config.add_mutually_exclusive_group()
        compute_creds.add_argument("--enable-account-compute-creds",
                                   dest="use_account_compute_creds",
                                   action="store_true",
                                   default=None,
                                   help="to use account compute credentials")
        compute_creds.add_argument("--disable-account-compute-creds",
                                   dest="use_account_compute_creds",
                                   action="store_false",
                                   default=None,
                                   help="to disable account compute credentials")
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

        # location settings parser
        location_group = argparser.add_argument_group("location config setttings")
        location_group.add_argument("--aws-region",
                                    dest="aws_region",
                                    choices=["us-east-1", "us-west-2", "ap-northeast-1", "sa-east-1",
                                             "eu-west-1", "ap-southeast-1", "us-west-1"],
                                    help="aws region to create the cluster in", )
        location_group.add_argument("--aws-availability-zone",
                                    dest="aws_availability_zone",
                                    help="availability zone to" +
                                         " create the cluster in", )

        # network settings parser
        network_config_group = argparser.add_argument_group("network config settings")
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
                                          help="a security group to attach with each" +
                                               " node of the cluster. Typically used" +
                                               " to provide access to external hosts", )
        network_config_group.add_argument("--master-elastic-ip",
                                          dest="master_elastic_ip",
                                          help="master elastic ip for cluster")