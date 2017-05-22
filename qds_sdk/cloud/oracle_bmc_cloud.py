from qds_sdk.cloud.cloud import Cloud
from qds_sdk.commands import Command,PrestoCommand

import sys
import logging
import re
import uuid
import os

log = logging.getLogger("qds_commands")
Oracle_URI_RE = re.compile(r'oraclebmc://([^@]+)([^/]+)/?(.*)')

class OracleBmcCloud(Cloud):
    '''
    qds_sdk.cloud.OracleBmcCloud is the class which stores information about oracle bmc cloud config settings.
    The objects of this class can be used to set oracle_bmc cloud_config settings while create/update/clone a cluster.
    '''

    def __init__(self):
        self.compute_config = {}
        self.location = {}
        self.network_config = {}
        self.storage_config = {}

    def set_cloud_config(self,
                         compute_tenant_id=None,
                         compute_user_id=None,
                         compute_key_finger_print=None,
                         compute_api_private_rsa_key=None,
                         use_account_compute_creds=None,
                         subnet_id=None,
                         oracle_region=None,
                         oracle_availability_domain=None,
                         compartment_id=None,
                         image_id=None,
                         vcn_id=None,
                         storage_tenant_id=None,
                         storage_user_id=None,
                         storage_key_finger_print=None,
                         storage_api_private_rsa_key=None):
        '''

        Args:
            compute_tenant_id: compute tenant id for oracle cluster

            compute_user_id: compute user id for oracle cluster

            compute_key_finger_print: compute key fingerprint for oracle cluster

            compute_api_private_rsa_key: compute api private rsa key for oracle cluster

            use_account_compute_creds: Set it to true to use the account's compute
                credentials for all clusters of the account.The default value is false

            subnet_id: subnet id for oracle

            oracle_region: region to create the cluster in

            oracle_availability_domain: availability zone to create the cluster in

            compartment_id: compartment id for oracle cluster

            image_id: image id for oracle cloud

            vcn_id: vcn to create the cluster in

            storage_tenant_id: tenant id for oracle cluster

            storage_user_id: storage user id for oracle cluster

            storage_key_finger_print: storage key fingerprint for oracle cluster

            storage_api_private_rsa_key: storage api private rsa key for oracle cluster

        '''

        self.set_compute_config(use_account_compute_creds, compute_tenant_id,
                                compute_user_id, compute_key_finger_print,
                                compute_api_private_rsa_key)
        self.set_location(oracle_region, oracle_availability_domain)
        self.set_network_config(vcn_id, subnet_id,
                                compartment_id, image_id)
        self.set_storage_config(storage_tenant_id, storage_user_id,
                                storage_key_finger_print, storage_api_private_rsa_key)

    def  set_compute_config(self,
                            use_account_compute_creds=None,
                            compute_tenant_id=None,
                            compute_user_id=None,
                            compute_key_finger_print=None,
                            compute_api_private_rsa_key=None):
        self.compute_config['use_account_compute_creds'] = use_account_compute_creds
        self.compute_config['compute_tenant_id'] = compute_tenant_id
        self.compute_config['compute_user_id'] = compute_user_id
        self.compute_config['compute_key_finger_print'] = compute_key_finger_print
        self.compute_config['compute_api_private_rsa_key'] = compute_api_private_rsa_key

    def set_location(self,
                     oracle_region=None,
                     oracle_availability_domain=None):
        self.location['region'] = oracle_region
        self.location['availability_domain'] = oracle_availability_domain

    def set_network_config(self,
                           vcn_id=None,
                           subnet_id=None,
                           compartment_id=None,
                           image_id=None):
        self.network_config['vcn_id'] = vcn_id
        self.network_config['subnet_id'] = subnet_id
        self.network_config['compartment_id'] = compartment_id
        self.network_config['image_id'] = image_id

    def set_storage_config(self,
                           storage_tenant_id=None,
                           storage_user_id=None,
                           storage_key_finger_print=None,
                           storage_api_private_rsa_key=None):
        self.storage_config['storage_tenant_id'] = storage_tenant_id
        self.storage_config['storage_user_id'] = storage_user_id
        self.storage_config['storage_key_finger_print'] = storage_key_finger_print
        self.storage_config['storage_api_private_rsa_key'] = storage_api_private_rsa_key

    def set_cloud_config_from_arguments(self, arguments):
        self.set_cloud_config(compute_tenant_id=arguments.compute_tenant_id,
                              compute_user_id=arguments.compute_user_id,
                              compute_key_finger_print=arguments.compute_key_finger_print,
                              compute_api_private_rsa_key=arguments.compute_api_private_rsa_key,
                              use_account_compute_creds=arguments.use_account_compute_creds,
                              subnet_id=arguments.subnet_id,
                              oracle_region=arguments.region,
                              oracle_availability_domain=arguments.availability_domain,
                              compartment_id=arguments.compartment_id,
                              image_id=arguments.image_id,
                              vcn_id=arguments.vcn_id,
                              storage_tenant_id=arguments.storage_tenant_id,
                              storage_user_id=arguments.storage_user_id,
                              storage_key_finger_print=arguments.storage_key_finger_print,
                              storage_api_private_rsa_key=arguments.storage_api_private_rsa_key)

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
        compute_config.add_argument("--compute-tenant-id",
                                    dest="compute_tenant_id",
                                    default=None,
                                    help="tenant id for oracle cluster")
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

        # location settings parser
        location_group = argparser.add_argument_group("location config settings")
        location_group.add_argument("--oracle-region",
                                    dest="region",
                                    help="region to create the cluster in", )
        location_group.add_argument("--oracle-availability-zone",
                                    dest="availability_domain",
                                    help="availability zone to" +
                                         " create the cluster in", )

        # network settings parser
        network_config_group = argparser.add_argument_group("network config settings")
        network_config_group.add_argument("--compartment-id",
                                          dest="compartment_id",
                                          help="compartment id for oracle cluster")
        network_config_group.add_argument("--image-id",
                                          dest="image_id",
                                          help="image id for oracle cloud")
        network_config_group.add_argument("--vcn-id",
                                          dest="vcn_id",
                                          help="vcn to create the cluster in", )
        network_config_group.add_argument("--subnet-id",
                                          dest="subnet_id",
                                          help="subnet id for oracle")

        # storage config settings parser
        storage_config = argparser.add_argument_group("storage config settings")
        storage_config.add_argument("--storage-tenant-id",
                                    dest="storage_tenant_id",
                                    default=None,
                                    help="tenant id for oracle cluster")
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
                                    help="storage api private rsa key for oracle cluster")


    def get_results(self, fp=sys.stdout, storage_credentials=None, r={}, delim=None, qlog=None, include_header=None):
        import oraclebmc
        private_key = storage_credentials.get('storage_config').get('api_private_rsa_key')
        private_key_file_path = "/tmp/" + uuid.uuid4().hex
        with open(private_key_file_path, 'w+') as private_key_file:
            private_key_file.write(private_key)
        os.chmod(private_key_file_path, 0o744)
        oracle_config = {"user": storage_credentials.get('storage_config').get('user_id'),
                         "key_file": private_key_file_path,
                         "fingerprint": storage_credentials.get('storage_config').get('key_finger_print'),
                         "tenancy": storage_credentials.get('storage_config').get('tenant_id'),
                         "region": "us-phoenix-1",  # just now added us-phoenix-1 , have to change api to get this param
                         "pass_phrase": None,
                         "additional_user_agent": "",
                         "log_requests": False
                         }
        object_storage = oraclebmc.object_storage.ObjectStorageClient(oracle_config)
        log.info("Starting download from result locations: [%s]" % ",".join(r['result_location']))
        num_result_dir = Command.find(self.id).num_result_dir
        for oracle_path in r['result_location']:
            _download_to_local_oracle(object_storage, oracle_path, fp, delim=delim,
                                      skip_data_avail_check=isinstance(self, PrestoCommand))


def _download_to_local_oracle(storage_conn, oracle_path, fp, delim=None, skip_data_avail_check=False):

    def get_contents_to_file_using_delim():
        if sys.version_info < (3, 0, 0):
            fp.write(str(content).replace(chr(1), delim))
        else:
            import io
            if isinstance(fp, io.TextIOBase):
                fp.buffer.write(content.decode('utf-8').replace(chr(1), delim).encode('utf8'))
            elif isinstance(fp, io.BufferedIOBase) or isinstance(fp, io.RawIOBase):
                fp.write(content.decode('utf8').replace(chr(1), delim).encode('utf8'))
            else:
                # Can this happen? Don't know what's the right thing to do in this case.
                pass

    def get_contents_to_file_oracle():
        if sys.version_info < (3, 0, 0):
            fp.write(content.encode('utf8'))
        else:
            import io
            if isinstance(fp, io.TextIOBase):
                fp.buffer.write(content.encode('utf8'))
            elif isinstance(fp, io.BufferedIOBase) or isinstance(fp, io.RawIOBase):
                fp.write(content.encode('utf8'))
            else:
                # Can this happen? Don't know what's the right thing to do in this case.
                pass


    try:
        m = Oracle_URI_RE.match(oracle_path)
        bucket_name = m.group(1)
        object_name = m.group(3)
        namespace_name = storage_conn.get_namespace().data
        if oracle_path.endswith('/') is False:

            response = storage_conn.get_object(namespace_name, bucket_name, object_name)
            content = response.data.content if response.data is not None else ""

            if delim is not None:
                get_contents_to_file_using_delim()
            else:
                get_contents_to_file_oracle()
        else:
            # Write check for results if incomplete data available
            # For folders
            list = storage_conn.list_objects(namespace_name,bucket_name,prefix=object_name)
            objects = list.data.objects
            for one_path in objects:
                name = one_path.name
                if name.endswith('/') is False:
                    log.info("Downloading file from %s" % name)
                    response = storage_conn.get_object(namespace_name, bucket_name, name)
                    content = response.data.content if response.data is not None else ""
                    if delim is not None:
                        get_contents_to_file_using_delim()
                    else:
                        get_contents_to_file_oracle()
    except Exception as e:
        raise Exception("Not able to download results: %s" % e.message)