from qds_sdk import util
class Engine:

    def __init__(self, flavour=None):
        self.flavour = flavour
        self.hadoop_settings = {}
        self.presto_settings = {}
        self.spark_settings = {}
        self.airflow_settings ={}

    @staticmethod
    def engine_parser(argparser):
        engine_group = argparser.add_argument_group("engine settings")
        engine_group.add_argument("--flavour",
                                  dest="flavour",
                                  choices=["hadoop", "hadoop2", "presto", "spark", "hbase", "airflow"],
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


    def set_engine_config_settings(self, arguments):
        custom_hadoop_config = util._read_file(arguments.custom_hadoop_config_file, "custom config file")
        fairscheduler_config_xml = util._read_file(arguments.fairscheduler_config_xml_file,
                                                   "config xml file")
        custom_presto_config = util._read_file(arguments.presto_custom_config_file,
                                               "presto custom config file")

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
                               overrides=arguments.overrides)

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
                          overrides=None):

        def set_fairscheduler_settings():
            self.hadoop_settings['fairscheduler_settings'] = {}
            self.hadoop_settings['fairscheduler_settings']['fairscheduler_config_xml'] = \
                fairscheduler_config_xml
            self.hadoop_settings['fairscheduler_settings']['default_pool'] = default_pool

        def set_hadoop_settings():
            self.hadoop_settings['custom_hadoop_config'] = custom_hadoop_config
            self.hadoop_settings['use_qubole_placement_policy'] = use_qubole_placement_policy
            set_fairscheduler_settings

        def set_presto_settings():
            self.presto_settings['presto_version'] = presto_version
            self.presto_settings['custom_presto_config'] = custom_presto_config

        def set_spark_settings():
            self.spark_settings['spark_version'] = spark_version
            self.spark_settings['custom_spark_config'] = custom_spark_config

        def set_airflow_settings():
            self.airflow_settings['dbtap_id'] = dbtap_id
            self.airflow_settings['fernet_key'] = fernet_key
            self.airflow_settings['overrides'] = overrides

        set_hadoop_settings()
        set_presto_settings()
        set_spark_settings()
        set_airflow_settings()






