from qds_sdk.cloud.cloud import Cloud
from qds_sdk.commands import Command,PrestoCommand

import boto
import logging
import sys
import json
import time
import re

log = logging.getLogger("qds_commands")

# Pattern matcher for s3 path
_URI_RE = re.compile(r's3://([^/]+)/?(.*)')

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


    def get_results(self, fp=sys.stdout, storage_credentials=None, r={}, delim=None, qlog=None, include_header=None):
        boto_conn = boto.connect_s3(aws_access_key_id=storage_credentials['storage_access_key'],
                                    aws_secret_access_key=storage_credentials['storage_secret_key'],
                                    security_token=storage_credentials['session_token'])

        log.info("Starting download from result locations: [%s]" % ",".join(r['result_location']))
        # fetch latest value of num_result_dir
        num_result_dir = Command.find(self.id).num_result_dir

        for s3_path in r['result_location']:

            # If column/header names are not able to fetch then use include header as true
            if include_header.lower() == "true" and qlog is not None:
                self.write_headers(qlog, fp)

            # In Python 3,
            # If the delim is None, fp should be in binary mode because
            # boto expects it to be.
            # If the delim is not None, then both text and binary modes
            # work.

            _download_to_local(boto_conn, s3_path, fp, num_result_dir, delim=delim,
                               skip_data_avail_check=isinstance(self, PrestoCommand))

    def write_headers(self, qlog, fp):
        col_names = []
        qlog = json.loads(qlog)
        if qlog["QBOL-QUERY-SCHEMA"] is not None:
            qlog_hash = qlog["QBOL-QUERY-SCHEMA"]["-1"] if qlog["QBOL-QUERY-SCHEMA"]["-1"] is not None else \
            qlog["QBOL-QUERY-SCHEMA"][qlog["QBOL-QUERY-SCHEMA"].keys[0]]

            for qlog_item in qlog_hash:
                col_names.append(qlog_item["ColumnName"])

            col_names = "\t".join(col_names)
            col_names += "\n"

        fp.write(col_names)


def _read_iteratively(key_instance, fp, delim):
    key_instance.open_read()
    while True:
        try:
            # Default buffer size is 8192 bytes
            data = next(key_instance)
            if sys.version_info < (3, 0, 0):
                fp.write(str(data).replace(chr(1), delim))
            else:
                import io
                if isinstance(fp, io.TextIOBase):
                    fp.buffer.write(data.decode('utf-8').replace(chr(1), delim).encode('utf8'))
                elif isinstance(fp, io.BufferedIOBase) or isinstance(fp, io.RawIOBase):
                    fp.write(data.decode('utf8').replace(chr(1), delim).encode('utf8'))
                else:
                    # Can this happen? Don't know what's the right thing to do in this case.
                    pass
        except StopIteration:
            # Stream closes itself when the exception is raised
            return

def _download_to_local(boto_conn, s3_path, fp, num_result_dir, delim=None, skip_data_avail_check=False):
    '''
    Downloads the contents of all objects in s3_path into fp

    Args:
        `boto_conn`: S3 connection object

        `s3_path`: S3 path to be downloaded

        `fp`: The file object where data is to be downloaded
    '''
    #Progress bar to display download progress
    def _callback(downloaded, total):
        '''
        Call function for upload.

        `downloaded`: File size already downloaded (int)

        `total`: Total file size to be downloaded (int)
        '''
        if (total is 0) or (downloaded == total):
            return
        progress = downloaded*100/total
        sys.stderr.write('\r[{0}] {1}%'.format('#'*progress, progress))
        sys.stderr.flush()

    def _is_complete_data_available(bucket_paths, num_result_dir):
        if num_result_dir == -1:
            return True
        unique_paths = set()
        files = {}
        for one_path in bucket_paths:
            name = one_path.name.replace(key_prefix, "", 1)
            if name.startswith('_tmp.'):
                continue
            path = name.split("/")
            dir = path[0].replace("_$folder$", "", 1)
            unique_paths.add(dir)
            if len(path) > 1:
                file = int(path[1])
                if dir not in files:
                    files[dir] = []
                files[dir].append(file)
        if len(unique_paths) < num_result_dir:
            return False
        for k in files:
            v = files.get(k)
            if len(v) > 0 and max(v) + 1 > len(v):
                return False
        return True

    m = _URI_RE.match(s3_path)
    bucket_name = m.group(1)
    bucket = boto_conn.get_bucket(bucket_name)
    retries = 6
    if s3_path.endswith('/') is False:
        #It is a file
        key_name = m.group(2)
        key_instance = bucket.get_key(key_name)
        while key_instance is None and retries > 0:
            retries = retries - 1
            log.info("Results file is not available on s3. Retry: " + str(6-retries))
            time.sleep(10)
            key_instance = bucket.get_key(key_name)
        if key_instance is None:
            raise Exception("Results file not available on s3 yet. This can be because of s3 eventual consistency issues.")
        log.info("Downloading file from %s" % s3_path)
        if delim is None:
            try:
                key_instance.get_contents_to_file(fp)  # cb=_callback
            except boto.exception.S3ResponseError as e:
                if (e.status == 403):
                    # SDK-191, boto gives an error while fetching the objects using versions which happens by default
                    # in the get_contents_to_file() api. So attempt one without specifying version.
                    log.warn("Access denied while fetching the s3 object. Retrying without specifying the version....")
                    key_instance.open()
                    fp.write(key_instance.read())
                    key_instance.close()
                else:
                    raise
        else:
            # Get contents as string. Replace parameters and write to file.
            _read_iteratively(key_instance, fp, delim=delim)

    else:
        #It is a folder
        key_prefix = m.group(2)
        bucket_paths = bucket.list(key_prefix)
        if not skip_data_avail_check:
            complete_data_available = _is_complete_data_available(bucket_paths, num_result_dir)
            while complete_data_available is False and retries > 0:
                retries = retries - 1
                log.info("Results dir is not available on s3. Retry: " + str(6-retries))
                time.sleep(10)
                complete_data_available = _is_complete_data_available(bucket_paths, num_result_dir)
            if complete_data_available is False:
                raise Exception("Results file not available on s3 yet. This can be because of s3 eventual consistency issues.")

        for one_path in bucket_paths:
            name = one_path.name

            # Eliminate _tmp_ files which ends with $folder$
            if name.endswith('$folder$'):
                continue

            log.info("Downloading file from %s" % name)
            if delim is None:
                one_path.get_contents_to_file(fp)  # cb=_callback
            else:
                _read_iteratively(one_path, fp, delim=delim)



