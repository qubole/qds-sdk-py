from qds_sdk.cloud.cloud import Cloud
class OracleOpcCloud(Cloud):
    '''
    qds_sdk.cloud.OracleOpcCloud is the class which stores information about oracle opc cloud config settings.
    The objects of this class can be use to set oracle opc cloud_config settings while create/update/clone a cluster.
    '''

    def __init__(self):
        self.compute_config = {}
        self.network_config = {}
        self.storage_config = {}

    def set_cloud_config(self,
                         username=None,
                         password=None,
                         rest_api_endpoint=None,
                         use_account_compute_creds=None,
                         storage_rest_api_endpoint=None,
                         storage_username=None,
                         storage_password=None,
                         acl=None,
                         ip_network=None,
                         data_disk_count=None,
                         data_disk_size=None):
        '''

        Args:
            username: Username for customers oracle opc account. This
                is required for creating the cluster.

            password: Password for customers oracle opc account. This
                is required for creating the cluster.

            rest_api_endpoint: Rest API endpoint for customers oracle opc cloud.

            use_account_compute_creds: Set it to true to use the accounts compute
                credentials for all clusters of the account.The default value is false

            storage_rest_api_endpoint: Rest API endpoint for storage related operations.

            storage_username: Username for customers oracle opc account. This
                is required for creating the cluster.

            storage_password: Password for customers oracle opc account. This
                is required for creating the cluster.

            acl: acl for oracle opc.

            ip_network: subnet name for oracle opc

        '''

        self.set_compute_config(use_account_compute_creds, username,
                                password, rest_api_endpoint)
        self.set_network_config(acl, ip_network)
        self.set_storage_config(storage_username, storage_password,
                                storage_rest_api_endpoint, data_disk_count, data_disk_size)

    def set_compute_config(self,
                           use_account_compute_creds=None,
                           username=None,
                           password=None,
                           rest_api_endpoint=None):
        self.compute_config['use_account_compute_creds'] = use_account_compute_creds
        self.compute_config['username'] = username
        self.compute_config['password'] = password
        self.compute_config['rest_api_endpoint'] = rest_api_endpoint

    
    def set_network_config(self,
                           acl=None,
                           ip_network=None):
        self.network_config['acl'] = acl
        self.network_config['ip_network'] = ip_network

    def set_storage_config(self,
                           storage_username=None,
                           storage_password=None,
                           storage_rest_api_endpoint=None,
                           data_disk_count=None,
                           data_disk_size=None):
        self.storage_config['storage_username'] = storage_username
        self.storage_config['storage_password'] = storage_password
        self.storage_config['storage_rest_api_endpoint'] = storage_rest_api_endpoint
        self.storage_config['data_disk_count'] = data_disk_count
        self.storage_config['data_disk_size'] =data_disk_size

    def set_cloud_config_from_arguments(self, arguments):
        self.set_cloud_config(username=arguments.username,
                              password=arguments.password,
                              rest_api_endpoint=arguments.rest_api_endpoint,
                              use_account_compute_creds=arguments.use_account_compute_creds,
                              storage_rest_api_endpoint=arguments.storage_rest_api_endpoint,
                              storage_username=arguments.storage_username,
                              storage_password=arguments.storage_password,
                              acl=arguments.acl,
                              ip_network=arguments.ip_network,
                              data_disk_count=arguments.count,
                              data_disk_size=arguments.size)

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
        compute_config.add_argument("--username",
                                    dest="username",
                                    default=None,
                                    help="username for opc cloud account")
        compute_config.add_argument("--password",
                                    dest="password",
                                    default=None,
                                    help="password for opc cloud account")
        compute_config.add_argument("--rest-api-endpoint",
                                    dest="rest_api_endpoint",
                                    default=None,
                                    help="Rest API endpoint for oracle opc account")

        # network settings parser
        network_config_group = argparser.add_argument_group("network config settings")
        network_config_group.add_argument("--acl",
                                          dest="acl",
                                          help="acl for opc", )
        network_config_group.add_argument("--ip-network",
                                          dest="ip_network",
                                          help="subnet name for opc")
        
        # storage config settings parser
        storage_config = argparser.add_argument_group("storage config settings")
        storage_config.add_argument("--storage-rest-api-endpoint",
                                    dest="storage_rest_api_endpoint",
                                    default=None,
                                    help="REST API endpoint for storage cloud")
        storage_config.add_argument("--storage-username",
                                    dest="storage_username",
                                    default=None,
                                    help="username for opc cloud account")
        storage_config.add_argument("--storage-password",
                                    dest="storage_password",
                                    default=None,
                                    help="password for opc cloud account")
