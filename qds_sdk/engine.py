class Engine:
    def __init__(self, flavour=None):
        self.flavour = flavour
        self.hadoop_setting = {}
        self.presto_setting = {}
        self.spark_setting = {}
        self.airflow_setting ={}

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


