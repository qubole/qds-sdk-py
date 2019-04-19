from qds_sdk import util


class Engine:
    '''
    Use this class to set engine config settings of cluster
    qds_sdk.engine.Engine is the class which stores information about engine config settings.
    You can use objects of this class to set engine_config settings while create/update/clone a cluster.
    '''

    def __init__(self, flavour=None):
        self.flavour = flavour
        self.hadoop_settings = {}
        self.presto_settings = {}
        self.spark_settings = {}
        self.airflow_settings ={}
        self.engine_config = {}

    def set_engine_config(self,
                          custom_hadoop_config=None,
                          use_qubole_placement_policy=None,
                          fairscheduler_config_xml=None,
                          default_pool=None,
                          presto_version=None,
                          custom_presto_config=None,
                          spark_version=None,
                          custom_spark_config=None,
                          dbtap_id=None,
                          fernet_key=None,
                          overrides=None,
                          airflow_version=None,
                          airflow_python_version=None,
                          is_ha=None,
                          enable_rubix=None):
        '''

        Args:
            custom_hadoop_config: Custom Hadoop configuration overrides.

            use_qubole_placement_policy: Use Qubole Block Placement policy for
                clusters with spot nodes.

            fairscheduler_config_xml: XML string with custom configuration
                parameters for the fair scheduler.

            default_pool: The default pool for the fair scheduler.

            presto_version: Version of presto to be used in cluster

            custom_presto_config: Custom Presto configuration overrides.

            spark_version: Version of spark to be used in cluster

            custom_spark_config: Specify the custom Spark configuration overrides

            dbtap_id: ID of the data store inside QDS

            fernet_key: Encryption key for sensitive information inside airflow database.
                For example, user passwords and connections. It must be a 32 url-safe base64 encoded bytes.

            overrides: Airflow configuration to override the default settings.Use the following syntax for overrides:
                <section>.<property>=<value>\n<section>.<property>=<value>...

            airflow_version: The airflow version.

            airflow_python_version: The python version for the environment on the cluster.

            is_ha: Enabling HA config for cluster
            is_deeplearning : this is a deeplearning cluster config
            enable_rubix: Enable rubix on the cluster

        '''

        self.set_hadoop_settings(custom_hadoop_config, use_qubole_placement_policy, is_ha, fairscheduler_config_xml, default_pool, enable_rubix)
        self.set_presto_settings(presto_version, custom_presto_config)
        self.set_spark_settings(spark_version, custom_spark_config)
        self.set_airflow_settings(dbtap_id, fernet_key, overrides, airflow_version, airflow_python_version)

    def set_fairscheduler_settings(self,
                                   fairscheduler_config_xml=None,
                                   default_pool=None):
        self.hadoop_settings['fairscheduler_settings'] = {}
        self.hadoop_settings['fairscheduler_settings']['fairscheduler_config_xml'] = \
            fairscheduler_config_xml
        self.hadoop_settings['fairscheduler_settings']['default_pool'] = default_pool

    def set_hadoop_settings(self,
                            custom_hadoop_config=None,
                            use_qubole_placement_policy=None,
                            is_ha=None,
                            fairscheduler_config_xml=None,
                            default_pool=None,
                            enable_rubix=None):
        self.hadoop_settings['custom_hadoop_config'] = custom_hadoop_config
        self.hadoop_settings['use_qubole_placement_policy'] = use_qubole_placement_policy
        self.hadoop_settings['is_ha'] = is_ha
        self.set_fairscheduler_settings(fairscheduler_config_xml, default_pool)
        self.hadoop_settings['enable_rubix'] = enable_rubix

    def set_presto_settings(self,
                            presto_version=None,
                            custom_presto_config=None):
        self.presto_settings['presto_version'] = presto_version
        self.presto_settings['custom_presto_config'] = custom_presto_config

    def set_spark_settings(self,
                           spark_version=None,
                           custom_spark_config=None):
        self.spark_settings['spark_version'] = spark_version
        self.spark_settings['custom_spark_config'] = custom_spark_config

    def set_airflow_settings(self,
                             dbtap_id=None,
                             fernet_key=None,
                             overrides=None,
                             airflow_version="1.10.0",
                             airflow_python_version="2.7"):
        self.airflow_settings['dbtap_id'] = dbtap_id
        self.airflow_settings['fernet_key'] = fernet_key
        self.airflow_settings['overrides'] = overrides
        self.airflow_settings['version'] = airflow_version
        self.airflow_settings['airflow_python_version'] = airflow_python_version

    def set_engine_config_settings(self, arguments):
        custom_hadoop_config = util._read_file(arguments.custom_hadoop_config_file)
        fairscheduler_config_xml = util._read_file(arguments.fairscheduler_config_xml_file)
        custom_presto_config = util._read_file(arguments.presto_custom_config_file)
        is_deeplearning=False

        self.set_engine_config(custom_hadoop_config=custom_hadoop_config,
                               use_qubole_placement_policy=arguments.use_qubole_placement_policy,
                               fairscheduler_config_xml=fairscheduler_config_xml,
                               default_pool=arguments.default_pool,
                               presto_version=arguments.presto_version,
                               custom_presto_config=custom_presto_config,
                               spark_version=arguments.spark_version,
                               custom_spark_config=arguments.custom_spark_config,
                               dbtap_id=arguments.dbtap_id,
                               fernet_key=arguments.fernet_key,
                               overrides=arguments.overrides,
                               airflow_version=arguments.airflow_version,
                               airflow_python_version=arguments.airflow_python_version,
                               enable_rubix=arguments.enable_rubix)

    @staticmethod
    def engine_parser(argparser):
        engine_group = argparser.add_argument_group("engine settings")
        engine_group.add_argument("--flavour",
                                  dest="flavour",
                                  choices=["hadoop", "hadoop2", "presto", "spark", "hbase", "airflow", "deeplearning"],
                                  default=None,
                                  help="Set engine flavour")

        hadoop_settings_group = argparser.add_argument_group("hadoop settings")
        hadoop_settings_group.add_argument("--custom-hadoop-config",
                                           dest="custom_hadoop_config_file",
                                           default=None,
                                           help="location of file containing custom" +
                                                " hadoop configuration overrides")
        qubole_placement_policy_group = hadoop_settings_group.add_mutually_exclusive_group()
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
        enable_rubix_group = hadoop_settings_group.add_mutually_exclusive_group()
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

        presto_settings_group = argparser.add_argument_group("presto settings")
        presto_settings_group.add_argument("--presto-version",
                                           dest="presto_version",
                                           default=None,
                                           help="Version of presto for this cluster", )
        presto_settings_group.add_argument("--presto-custom-config",
                                           dest="presto_custom_config_file",
                                           help="location of file containg custom" +
                                                " presto configuration overrides")

        spark_settings_group = argparser.add_argument_group("spark settings")
        spark_settings_group.add_argument("--spark-version",
                                          dest="spark_version",
                                          default=None,
                                          help="Version of spark for the cluster", )
        spark_settings_group.add_argument("--custom-spark-config",
                                          dest="custom_spark_config",
                                          default=None,
                                          help="Custom config spark for this cluster", )

        airflow_settings_group = argparser.add_argument_group("airflow settings")
        airflow_settings_group.add_argument("--dbtap-id",
                                            dest="dbtap_id",
                                            default=None,
                                            help="dbtap id for airflow cluster", )
        airflow_settings_group.add_argument("--fernet-key",
                                            dest="fernet_key",
                                            default=None,
                                            help="fernet key for airflow cluster", )
        airflow_settings_group.add_argument("--overrides",
                                            dest="overrides",
                                            default=None,
                                            help="overrides for airflow cluster", )
        airflow_settings_group.add_argument("--airflow-version",
                                            dest="airflow_version",
                                            default=None,
                                            help="airflow version for airflow cluster", )
        airflow_settings_group.add_argument("--airflow-python-version",
                                            dest="airflow_python_version",
                                            default=None,
                                            help="python environment version for airflow cluster", )

