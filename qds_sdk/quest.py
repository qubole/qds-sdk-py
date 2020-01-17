"""
The quest module contains the base definition for
a generic quest commands.
"""
from qds_sdk.actions import *
import json
from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser

log = logging.getLogger("qds_quest")

# Pattern matcher for s3 path
_URI_RE = re.compile(r's3://([^/]+)/?(.*)')


class QuestCmdLine:
    """
    qds_sdk.QuestCmdLine is the interface used by qds.py.
    """

    @staticmethod
    def parsers():
        argparser = ArgumentParser(prog="qds.py quest",
                                   description="Quest client for Qubole Data Service.")
        subparsers = argparser.add_subparsers()

        # Create
        create = subparsers.add_parser("create", help="Create a new pipeline")
        create.add_argument("--create-type", dest="create_type", required=True,
                            help="create_type=1 for assisted, create_type=2 for jar, create_type=3 for code")
        create.add_argument("--pipeline-name", dest="name", required=True,
                            help="Name of pipeline")
        create.add_argument("--description", dest="description", default=None,
                            help="Pipeline description"),
        create.add_argument("--source", dest="source",
                            help="Pipeline source for assisted mode only"),
        create.add_argument("--source-path", dest="source_path",
                            help="Pipeline source, this option is applicable on assisted mode only."),
        create.add_argument("--format", dest="format", default="json",
                            help="Data format"),
        create.add_argument("--source-name", dest="source_name",
                            help="name of source node in Pipeline, this option is applicable on assisted mode only"),
        create.add_argument("--sink-name", dest="sink_name",
                            help="name of sink node in Pipeline, this option is applicable on assisted mode only"),
        create.add_argument("--schema", dest="schema",
                            help="schema of the data, in dictionary format eg. {id:IntegerType}, this option is applicable on assisted mode only"),
        create.add_argument("--other_settings", dest="other_settings", help="other_settings")
        create.add_argument("--data-store", dest="data_store",
                            help="data stores s3 or karfka or kinesis, this option is applicable on assisted mode only")
        create.add_argument("--cluster-label", dest="cluster_label", default="default", help="Cluster label")
        create.add_argument("--checkpoint-location", dest="checkpoint_location",
                            help="checkpoint location, this option is applicable on assisted mode")
        create.add_argument("--output-mode", dest="output_mode",
                            help="output mode append or latest, this option is applicable on assisted mode")
        create.add_argument("--trigger-interval", dest="trigger_interval",
                            help="trigger_interval, this option is applicable on assisted mode")
        create.add_argument("-c", "--code", dest="code", help="query string")
        create.add_argument("-f", "--script-location", dest="script_location",
                            help="Path where code to run is stored. Can be S3 URI or local file path")
        create.add_argument("-l", "--language", dest="language",
                            help="Language for bring your own code, valid values are python and scala")
        create.set_defaults(func=QuestCmdLine.create)
        # List
        index = subparsers.add_parser("list", help="List all pipelines")
        index.add_argument("--status", dest="status", required=True,
                           help='List pipeline with given status [active, archive, draft]')
        index.set_defaults(func=QuestCmdLine.index)
        # Utility for start/pause/clone/edit/delete/archive
        start = subparsers.add_parser("ops", help="List all pipelines")
        start.add_argument("--start", dest="start", action="store_true",
                           help='Start pipeline')
        start.add_argument("--pause", dest="pause", action="store_true",
                           help='Pause pipeline')
        start.add_argument("--delete", dest="delete", action="store_true",
                           help='Pause pipeline')
        start.add_argument("--clone", dest="clone", action="store_true",
                           help='Pause pipeline')
        start.add_argument("--edit", dest="edit", action="store_true", help="edit pipeline")
        start.add_argument("--archive", dest="archive", action="store_true", help="Archive Pipeline")
        start.add_argument("--status", dest="status", action="store_true", help="Status of Pipeline")
        start.add_argument("--pipeline-id", dest="pipeline_id", required=True,
                           help='Id of pipeline which need to be started')
        start.set_defaults(func=QuestCmdLine.start_pause)
        return argparser

    @staticmethod
    def run(args):
        parser = QuestCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)

    @staticmethod
    def start_pause(args):
        # if args.pause and args.start or not args.pause and not args.start:
        #     raise ParseError("Please select only one param out of --start and --pause")
        if args.start:
            response = Quest.start(args.pipeline_id)
        elif args.pause:
            response = Quest.pause(args.pipeline_id)
        elif args.delete:
            response = Quest.delete(args.pipeline_id)
        elif args.edit:
            response = Quest.edit(args.pipeline_id)
        elif args.clone:
            response = Quest.clone(args.pipeline_id)
        elif args.archive:
            response = Quest.archive(args.pipeline_id)
        elif args.status:
            response = Quest.status(args.pipeline_id)
        else:
            raise ParseError(
                "Please select only one param out of --start, --pause, --delete, --archive, --clone and --edit.")
        return json.dumps(response, default=lambda o: o.attributes, sort_keys=True, indent=4)

    @staticmethod
    def index(args):
        pipelinelist = Quest.index(args.status)
        return json.dumps(pipelinelist, default=lambda o: o.attributes, sort_keys=True, indent=4)

    @staticmethod
    def create(args):
        pipeline = None
        # print "create type = ", type(args.create_type)
        if args.create_type == "1":
            pipeline = QuestAssisted.create(args.name, args.create_type, args)
        elif args.create_type == "2":
            pipeline = QuestJar.create(args.name, args.create_type, args)
        elif args.create_type == "3":
            c = QuestCode()
            c.create(args.name, args)
        return json.dumps(pipeline, default=lambda o: o.attributes, sort_keys=True, indent=4)


class Quest(Resource):
    """
    qds_sdk.Quest is the base Qubole Quest class.
    """

    """ all commands use the /pipelines endpoint"""

    rest_entity_path = "pipelines"
    pipeline_id = None
    pipeline_name = None
    pipeline_code = None
    jar_path = None

    @staticmethod
    def get_pipline_id(response):
        return str(response.get('data').get('id'))

    @staticmethod
    def list(status=None):
        if status is None or status == 'all':
            params = {"filter": "draft,archive,active"}
        else:
            params = {"filter": status}
        conn = Qubole.agent()
        url_path = Quest.rest_entity_path
        questjson = conn.get(url_path, params)
        return questjson

    @staticmethod
    def create(pipeline_name, create_type, **kwargs):
        """
        Create a pipeline object by issuing a POST request to the /pipelin?mode=wizard endpoint
        Note - this creates pipeline in draft mode

        Args:
            pipeline_name: Name to be given.
            create_type: 1->Assisted, 2->Code, 3->Jar
            **kwargs: keyword arguments specific to create type

        Returns:
            response
        """
        conn = Qubole.agent()
        if create_type is None:
            raise ParseError("Please enter create_type. 1:Assisted Mode, 2:BYOJ, 3:BYOC")
        if pipeline_name is None:
            raise ParseError("Enter pipeline name")
        data = {"data": {
            "attributes":
                {"name": pipeline_name, "status": "DRAFT", "create_type": create_type},
            "type": "pipelines"}
        }
        url = Quest.rest_entity_path + "?mode=wizard"
        response = conn.post(url, data)
        Quest.pipeline_id = Quest.get_pipline_id(response)
        Quest.pipeline_name = pipeline_name
        return Quest

    @staticmethod
    def start(pipeline_id):
        """
        Method to start Pipeline
        :param pipeline_id: id of pipeline to be deleted
        :return: reponse
        """
        conn = Qubole.agent()
        url = Quest.rest_entity_path + "/" + pipeline_id + "/start"
        response = conn.put(url)
        pipeline_status = Quest.get_status(pipeline_id)
        while pipeline_status == 'waiting':
            log.info("Pipeline is in waiting state....")
            time.sleep(10)
            response = conn.put(url)
            pipeline_status = response.get('data').get('pipeline_instance_status')
        log.info("State of pipeline is %s " % pipeline_status)
        return response

    @staticmethod
    def add_property(pipeline_id, cluster_label, checkpoint_location, output_mode, trigger_interval=None,
                     can_retry=True, command_line_options=None):
        """
        Method to add properties in pipeline
        :param can_retry:
        :param pipeline_id:
        :param cluster_label:
        :param checkpoint_location:
        :param trigger_interval:
        :param output_mode:
        :param command_line_options:
        :return:
        """
        conn = Qubole.agent()
        if command_line_options is None:
            command_line_options = "--conf spark.driver.extraLibraryPath=/usr/lib/hadoop2/lib/native\n--conf spark.eventLog.compress=true\n--conf spark.eventLog.enabled=true\n--conf spark.sql.streaming.qubole.enableStreamingEvents=true\n--conf spark.qubole.event.enabled=true"
        data = {"data": {"attributes": {
            "cluster_label": cluster_label,
            "can_retry": can_retry,
            "checkpoint_location": checkpoint_location,
            "trigger_interval": trigger_interval,
            "output_mode": output_mode,
            "command_line_options": command_line_options
        },
            "type": "pipeline/properties"
        }
        }
        log.info("Data {}".format(data))
        url = Quest.rest_entity_path + "/" + pipeline_id + "/properties/"
        response = conn.put(url, data)
        return response

    @staticmethod
    def get_health(pipeline_id):
        """
        Get Pipeline Health
        :param pipeline_id:
        :return:
        """
        conn = Qubole.agent()
        url = Quest.rest_entity_path + "/" + pipeline_id
        response = conn.get(url)
        log.info(response)
        return response.get("data").get("attributes").get("health")

    @staticmethod
    def clone(pipeline_id):
        """
        Method to clone pipeline
        :param pipeline_id:
        :return:
        """
        url = Quest.rest_entity_path + "/" + pipeline_id + "/duplicate"
        log.info("Cloning pipeline with id {}".format(pipeline_id))
        conn = Qubole.agent()
        return conn.put(url)

    @staticmethod
    def pause(pipeline_id):
        """
        Method to pause pipeline
        :param pipeline_id:
        :return:
        """
        url = Quest.rest_entity_path + "/" + pipeline_id + "/pause"
        log.info("Pausing pipeline with id {}".format(pipeline_id))
        conn = Qubole.agent()
        return conn.put(url)

    @staticmethod
    def archive(pipeline_id):
        """
        Method to Archive pipeline
        :param pipeline_id:
        :return:
        """
        url = Quest.rest_entity_path + "/" + pipeline_id + "/archive"
        log.info("Archiving pipeline with id {}".format(pipeline_id))
        conn = Qubole.agent()
        return conn.put(url)

    @staticmethod
    def get_status(pipeline_id):
        """
        Get pipeline status
        :param pipeline_id:
        :return:
        """
        conn = Qubole.agent()
        url = Quest.rest_entity_path + "/" + pipeline_id
        response = conn.get(url)
        log.info(response)
        return response.get("data").get("attributes").get("pipeline_instance_status")

    @staticmethod
    def delete(pipeline_id):
        """
        Method to delete pipeline
        :param pipeline_id:
        :return:
        """
        conn = Qubole.agent()
        url = Quest.rest_entity_path + "/" + pipeline_id + "/delete"
        log.info("Deleting Pipeline with id: {}".format(pipeline_id))
        response = conn.put(url)
        log.info(response)
        return response

    @staticmethod
    def edit_pipeline_name(pipeline_id, pipeline_name):
        """
        Method to edit pipeline name (Required in case of cloning)
        :param pipeline_id:
        :param pipeline_name:
        :return:
        """
        conn = Qubole.agent()
        url = Quest.rest_entity_path + "/" + pipeline_id
        data = {"data": {"attributes": {"name": pipeline_name}, "type": "pipelines"}}
        return conn.put(url, data)

    @staticmethod
    def set_alert(pipeline_id, channel_id):
        """

        :param pipeline_id:
        :param channel_id: List of channel's id
        :return:
        """
        data = {
            "data": {"attributes": {"event_type": "error", "notification_channels": [channel_id], "can_notify": True},
                     "type": "pipeline/alerts"}}
        conn = Qubole.agent()
        url = Quest.rest_entity_path + "/" + pipeline_id + "/alerts"
        return conn.put(url, data)

    @staticmethod
    def get_code(pipeline_id):
        """
        Get pipeline code
        :param pipeline_id:
        :return:
        """
        url = Quest.rest_entity_path + "/" + pipeline_id
        conn = Qubole.agent()
        reponse = conn.get(url)
        code = reponse.get("meta")["command_details"]["code"]
        return code


class QuestCode(Quest):
    create_type = 3

    @staticmethod
    def create_pipeline(pipeline_name, code_or_fileLoc, cluster_label, checkpoint_location,
                        language=None,
                        trigger_interval=None,
                        output_mode=None,
                        can_retry=True,
                        channel_id=None):
        """
        Method to create pipeline in BYOC mode in one go.
        :param pipeline_name:
        :param code_or_fileLoc:
        :param cluster_label:
        :param checkpoint_location:
        :param language:
        :param trigger_interval:
        :param output_mode:
        :param can_retry:
        :param channel_id:
        :return:
        """
        response = Quest.create(pipeline_name, QuestCode.create_type)
        log.info(response)
        pipeline_id = Quest.get_pipline_id(response)
        response = Quest.add_property(pipeline_id, cluster_label, checkpoint_location,
                                      trigger_interval=trigger_interval,
                                      output_mode=output_mode,
                                      can_retry=can_retry)
        log.info(response)
        response = QuestCode.save_code(pipeline_id, code_or_fileLoc=code_or_fileLoc, language=language)
        if channel_id:
            response = Quest.set_alert(pipeline_id, channel_id)
            log.info(response)
        QuestCode.pipeline_id = QuestCode.get_pipline_id(response)
        return pipeline_id

    @staticmethod
    def save_code(pipeline_id, code_or_fileLoc, language=None):
        """
        Method to save code
        :param pipeline_id:
        :param code_or_fileLoc:
        :param language:scala or python
        :return:
        """
        try:
            code = None
            if code_or_fileLoc:
                if os.path.isdir(code_or_fileLoc):
                    q = open(code_or_fileLoc).read()
                    code = q
                else:
                    code = code_or_fileLoc

        except IOError as e:
            raise ParseError("Unable to open script location or script location and code both are empty")
        QuestCode.pipeline_code = code
        data = {"data": {
            "attributes": {"create_type": QuestCode.create_type, "code": str(code), "language": str(language)}}}
        conn = Qubole.agent()
        url = Quest.rest_entity_path + "/" + str(pipeline_id) + "/save_code"
        response = conn.put(url, data)
        return response


class QuestJar(Quest):
    create_type = 2

    @staticmethod
    def create_pipeline(pipeline_name, jar_path, cluster_label, checkpoint_location, main_class_name,
                        channel_id=None,
                        output_mode=None,
                        trigger_interval=None,
                        can_retry=True,
                        command_line_options=None,
                        user_arguments=None):
        """
        Method to create pipeline in BYOJ mode
        :param pipeline_name:
        :param jar_path:
        :param cluster_label:
        :param checkpoint_location:
        :param main_class_name:
        :param channel_id:
        :param output_mode:
        :param trigger_interval:
        :param can_retry:
        :param command_line_options:
        :param user_arguments:
        :return:
        """
        response = Quest.create(pipeline_name, QuestJar.create_type)
        log.info(response)
        pipeline_id = Quest.get_pipline_id(response)
        response = Quest.add_property(pipeline_id, cluster_label, checkpoint_location,
                                      output_mode=output_mode,
                                      trigger_interval=trigger_interval,
                                      can_retry=can_retry,
                                      command_line_options=command_line_options)
        log.info(response)
        response = QuestJar.save_code(pipeline_id, jar_path, main_class_name, user_arguments=user_arguments)
        QuestCode.pipeline_id = QuestJar.get_pipline_id(response)
        QuestJar.jar_path = jar_path
        if channel_id:
            response = Quest.set_alert(pipeline_id, channel_id)
            log.info(response)
        return response

    @staticmethod
    def save_code(pipeline_id, jar_path, main_class_name, user_arguments=None):
        """
        :param user_arguments:
        :param pipeline_id:
        :param jar_path:
        :param main_class_name:
        :return:
        """
        if jar_path is None:
            raise ParseError("Unable to open script location or script location and code both are empty")

        data = {"data": {
            "attributes": {"create_type": 2, "user_arguments": user_arguments, "jar_path": jar_path,
                           "language": main_class_name}}}
        conn = Qubole.agent()
        url = Quest.rest_entity_path + "/" + str(pipeline_id) + "/save_code"
        QuestJar.jar_path = jar_path
        response = conn.put(url, data)
        return response


class QuestAssisted(Quest):
    create_type = 1

    @staticmethod
    def add_source(pipeline_id, schema, format, data_store, topic_type, topics=None,
                   endpoint_url=None,
                   stream_name=None,
                   starting_position=None,
                   other_kinesis_settings=None,
                   broker=None,
                   use_registry=None,
                   registry_subject=None,
                   registry_id=None,
                   starting_offsets=None,
                   other_kafka_consumer_settings=None,
                   source_path=None,
                   s3_other_settings=None,
                   gs_other_settings=None):
        """

        :param topics:
        :param pipeline_id:
        :param schema:
        :param format:
        :param data_store:
        :param endpoint_url:
        :param stream_name:
        :param starting_position:
        :param other_kinesis_settings:
        :param broker:
        :param topic_type:
        :param use_registry:
        :param registry_subject:
        :param registry_id:
        :param starting_offsets:
        :param other_kafka_consumer_settings:
        :param source_path:
        :param s3_other_settings:
        :param gs_other_settings:
        :return:
        """
        url = Quest.rest_entity_path + "/" + pipeline_id + "/node"
        if data_store == "kinesis":
            return QuestAssisted._source_kinesis(url, schema, format, endpoint_url, stream_name,
                                                 starting_position=starting_position,
                                                 other_kinesis_settings=other_kinesis_settings)
        if data_store == "kafka":
            return QuestAssisted._source_kafka(url, schema, format, broker,
                                               topics=topics,
                                               topic_type=topic_type,
                                               use_registry=use_registry,
                                               registry_subject=registry_subject,
                                               registry_id=registry_id,
                                               starting_offsets=starting_offsets,
                                               other_kafka_consumer_settings=other_kafka_consumer_settings)
        if data_store == "s3":
            return QuestAssisted._source_s3(url, schema, format, source_path,
                                            other_settings=s3_other_settings)
        if data_store == "google_storage":
            return QuestAssisted._source_google_storage(url, schema, format, source_path,
                                                        other_settings=gs_other_settings)
        raise ParseError("Please add only one valid source out of [kafka, s3, kinesis]")

    @staticmethod
    def add_sink(pipeline_id, data_store,
                 data_format=None,
                 kafka_bootstrap_server=None,
                 topic=None,
                 other_kafka_settings=None,
                 sink_path=None,
                 partition_by=None,
                 other_s3_configurations=None,
                 other_gs_configurations=None,
                 table_name=None,
                 hive_database=None,
                 other_hive_configurations=None):
        """
        Method to add sink for given pipeline.
        :param pipeline_id:
        :param format:
        :param data_store:
        :param kafka_bootstrap_server:
        :param topic:
        :param other_kafka_settings:
        :param sink_path:
        :param partition_by:
        :param other_s3_configurations:
        :param other_gs_configurations:
        :param table_name:
        :param hive_database:
        :param other_hive_configurations:
        :return:
        """
        url = Quest.rest_entity_path + "/" + pipeline_id + "/node"
        if data_store == "kafka":
            return QuestAssisted._sink_kafka(url, data_format, kafka_bootstrap_server, topic,
                                             other_kafka_settings=other_kafka_settings)
        if data_store == "s3":
            return QuestAssisted._sink_s3(url, data_format, sink_path, partition_by,
                                          other_configurations=other_s3_configurations)
        if data_store == "snowflake":
            return QuestAssisted._sink_snowflake(url, data_format)
        if data_store == "google_storage":
            return QuestAssisted._sink_google_storage(url, data_format, sink_path, partition_by,
                                                      other_configurations=other_gs_configurations)
        if data_store == "hive":
            return QuestAssisted._sink_hive(url, table_name, databases=hive_database,
                                            default_other_configurations=other_hive_configurations)
        raise ParseError("Please add only one valid sink out of [kafka, s3, snowflake, hive, google_storage]")

    @staticmethod
    def create_pipeline(pipeline_name, schema, source_data_format, source_data_store, sink_data_store, topic_type,
                        checkpoint_location,
                        cluster_label,
                        output_mode,
                        sink_data_format=None,
                        source_topics=None,
                        sink_topics=None,
                        trigger_interval=None,
                        can_retry=True,
                        command_line_options=None,
                        operators=None,
                        channel_id=None,
                        endpoint_url=None,
                        stream_name=None,
                        starting_position=None,
                        other_kinesis_settings=None,
                        broker=None,
                        use_registry=None,
                        registry_subject=None,
                        registry_id=None,
                        starting_offsets=None,
                        other_kafka_consumer_settings=None,
                        source_path=None,
                        s3_other_settings=None,
                        gs_other_settings=None,
                        kafka_bootstrap_server=None,
                        other_kafka_settings=None,
                        sink_path=None,
                        partition_by=None,
                        other_s3_configurations=None,
                        other_gs_configurations=None,
                        table_name=None,
                        hive_database=None,
                        other_hive_configurations=None):
        """

        :param watermark_frequency:
        :param sink_topics:
        :param source_topics:
        :param sink_data_format:
        :param source_data_format:
        :param filter_column_name:
        :param pipeline_name:
        :param schema:
        :param format:
        :param source_data_store:
        :param sink_data_store:
        :param checkpoint_location:
        :param cluster_label:
        :param output_mode:
        :param trigger_interval:
        :param can_retry:
        :param command_line_options:
        :param operators: dict of operators
        :param channel_id:
        :param endpoint_url:
        :param stream_name:
        :param starting_position:
        :param other_kinesis_settings:
        :param broker:
        :param topic_type:
        :param use_registry:
        :param registry_subject:
        :param registry_id:
        :param starting_offsets:
        :param other_kafka_consumer_settings:
        :param source_path:
        :param s3_other_settings:
        :param gs_other_settings:
        :param kafka_bootstrap_server:
        :param topic:
        :param other_kafka_settings:
        :param sink_path:
        :param partition_by:
        :param other_s3_configurations:
        :param other_gs_configurations:
        :param table_name:
        :param hive_database:
        :param other_hive_configurations:
        :param condition:
        :param value:
        :param select_column_names: List of column names
        :param frequency:
        :param sliding_window_value_frequency:
        :param window_interval_frequency:
        :param other_columns:
        :return:
        """
        response = Quest.create(pipeline_name, QuestAssisted.create_type)
        final_response = None
        pipeline_id = response.pipeline_id
        pipeline_id = str(pipeline_id)
        src_response = QuestAssisted.add_source(pipeline_id, schema, source_data_format, source_data_store,
                                                topics=source_topics,
                                                endpoint_url=endpoint_url,
                                                stream_name=stream_name,
                                                starting_position=starting_position,
                                                other_kinesis_settings=other_kinesis_settings,
                                                broker=broker,
                                                topic_type=topic_type,
                                                use_registry=use_registry,
                                                registry_subject=registry_subject,
                                                registry_id=registry_id,
                                                starting_offsets=starting_offsets,
                                                other_kafka_consumer_settings=other_kafka_consumer_settings,
                                                source_path=source_path,
                                                s3_other_settings=s3_other_settings,
                                                gs_other_settings=gs_other_settings)
        log.info(src_response)
        sink_reponse = QuestAssisted.add_sink(pipeline_id, sink_data_store,
                                              data_format=sink_data_format,
                                              kafka_bootstrap_server=kafka_bootstrap_server,
                                              topic=sink_topics,
                                              other_kafka_settings=other_kafka_settings,
                                              sink_path=sink_path,
                                              partition_by=partition_by,
                                              other_s3_configurations=other_s3_configurations,
                                              other_gs_configurations=other_gs_configurations,
                                              table_name=table_name,
                                              hive_database=hive_database,
                                              other_hive_configurations=other_hive_configurations)
        log.info(sink_reponse)
        property_response = QuestAssisted.add_property(pipeline_id, cluster_label, checkpoint_location, output_mode,
                                                       trigger_interval=trigger_interval,
                                                       can_retry=can_retry,
                                                       command_line_options=command_line_options)
        log.info(property_response)
        final_response = property_response
        if operators:
            for operator in operators:
                if operator.items()[0][0] is "filter":
                    operator_response = QuestAssisted.add_operator(pipeline_id, operator="filter",
                                                                   condition=operator["filter"]["condition"],
                                                                   value=operator["filter"]["value"],
                                                                   filter_column_name=operator["filter"]["column_name"])
                elif operator.items()[0][0] is "select":
                    operator_response = QuestAssisted.add_operator(pipeline_id, operator="select",
                                                                   select_column_names=operator["select"]["column_names"])
                elif operator.items()[0][0] is "watermark":
                    operator_response = QuestAssisted.add_operator(pipeline_id, operator="watermark",
                                                                   watermark_column_name=operator["watermark"]["column_names"],
                                                                   watermark_frequency=operator["watermark"]["frequency"])

                elif operator.items()[0][0] is "windowed_group":
                    operator_response = QuestAssisted.add_operator(pipeline_id, operator="windowed_group",
                                                                   groupby_column_name=operator["windowed_group"]["column_name"],
                                                                   sliding_window_value=operator["windowed_group"]["sliding_window_value"],
                                                                   window_interval_frequency=operator["windowed_group"].get("window_interval_frequency"),
                                                                   other_columns=operator["windowed_group"]["other_columns"])
                else:
                    raise ParseError("Please enter valid operator value. Valid values are [filter, select, watermark, windowed_group]")

            log.info(operator_response)

            final_response = operator_response
        if channel_id:
            response = QuestAssisted.set_alert(pipeline_id, channel_id)
            log.info(response)
            final_response = response
        QuestAssisted.pipeline_code = QuestAssisted.get_code(pipeline_id)
        return QuestAssisted

    @staticmethod
    def add_operator(pipeline_id, operator,
                     condition=None,
                     value=None,
                     filter_column_name=None,
                     select_column_names=None,
                     sliding_window_value=None,
                     window_interval_frequency=None,
                     other_columns=None,
                     watermark_frequency=None,
                     groupby_column_name=None,
                     watermark_column_name=None):
        """

        :param watermark_column_name:
        :param groupby_column_name:
        :param watermark_frequency:
        :param select_column_names:
        :param filter_column_name:
        :param pipeline_id:
        :param operator:
        :param condition:
        :param value:
        :param sliding_window_value:
        :param window_interval_frequency:
        :param other_columns:
        :return:
        """
        url = Quest.rest_entity_path + "/" + pipeline_id + "/operator"
        if operator is None:
            return
        if operator == "filter":
            return QuestAssisted._filter_operator(url, filter_column_name, condition, value)
        if operator == "select":
            return QuestAssisted._select_operator(url, select_column_names)
        if operator == "watermark":
            return QuestAssisted._watermark_operator(url, watermark_column_name, watermark_frequency)
        if operator == "windowed_group":
            return QuestAssisted._window_group_operator(url, groupby_column_name, sliding_window_value,
                                                        window_interval_frequency, other_columns)
        raise ParseError("Please add only one valid sink out of [filter, select, watermark, windowed_group]. Given value is {}".format(operator))

    @staticmethod
    def _select_operator(url, column_names):
        """
        :param url: API url with pipeline id
        :param column_names:
        :return:
        """
        conn = Qubole.agent()
        data = {"data": {"attributes": {"operator": "select", "column_names": column_names}}}
        return conn.put(url, data)

    @staticmethod
    def _filter_operator(url, column_name, condition, value):
        """
        :param url: API url with pipeline id
        :param column_name:
        :param condition:
        :param value:
        :return:
        """
        conn = Qubole.agent()
        data = {"data": {"attributes": {"operator": "filter", "column_name": column_name,
                                        "condition": condition, "value": value}}}
        return conn.put(url, data)

    @staticmethod
    def _watermark_operator(url, column_name, frequency):
        """
        :param url: API url with pipeline id
        :param column_name:
        :param frequency:
        :return:
        """
        conn = Qubole.agent()
        data = {"data": {"attributes": {"operator": "watermark", "column_name": column_name,
                                        "frequency": frequency, "unit": "minute"}}}
        return conn.put(url, data)

    @staticmethod
    def _window_group_operator(url, column_name, sliding_window_value, window_interval_frequency,
                               other_columns):
        """
        :param url: API url with pipeline id
        :param column_name:
        :param sliding_window_value_frequency:
        :param window_interval_frequency:
        :param other_columns:
        :return:
        """
        conn = Qubole.agent()
        data = {"data": {"attributes": {"operator": "windowed_group",
                                        "window_expression":
                                            {"column_name": column_name,
                                             "sliding_window_value": {
                                                 "frequency": sliding_window_value,
                                                 "unit": "minute"},
                                             "window_interval": {
                                                 "frequency": window_interval_frequency,
                                                 "unit": "minute"}},
                                        "other_columns": other_columns,
                                        "action": "count"}}}
        return conn.put(url, data)

    @staticmethod
    def _source_kafka(url, schema, data_format, broker, topics, topic_type, starting_offsets, use_registry,
                      registry_subject=None,
                      registry_id=None,
                      other_kafka_consumer_settings=None):
        """

        :param url:
        :param schema:
        :param data_format:
        :param broker:
        :param topics:
        :param topic_type:
        :param use_registry:
        :param registry_subject:
        :param registry_id:
        :param starting_offsets:
        :param other_kafka_consumer_settings:
        :return:
        """
        conn = Qubole.agent()
        if other_kafka_consumer_settings is None:
            other_kafka_consumer_settings = {"kafkaConsumer.pollTimeoutMs": 512, "fetchOffset.numRetries": 3,
                                             "fetchOffset.retryIntervalMs": 10}

        if (use_registry is "schemaRegistry") and (registry_subject is None or registry_id is None):
            raise ParseError("registry_id and registry_subject cannot be empty if use_registry is auto fetch")

        data = {
            "data": {
                "attributes": {
                    "fields": {
                        "brokers": broker,
                        "topics": topics,
                        "topic_type": topic_type,
                        "schema": schema,
                        "use_registry": use_registry,
                        "registry_subject": registry_subject,
                        "registry_id": registry_id,
                        "starting_offsets": starting_offsets,
                        "format": data_format,
                        "other_kafka_consumer_settings": other_kafka_consumer_settings
                    },
                    "data_store": "kafka"
                },
                "type": "source"
            }
        }
        log.info("data = {}".format(data))
        response = conn.put(url, data)
        log.info(response)
        return response

    @staticmethod
    def _source_kinesis(url, schema, data_format, endpoint_url, stream_name, starting_position=None,
                        other_kinesis_settings=None):
        """
        :param url:
        :param schema:
        :param data_format:
        :param endpoint_url:
        :param stream_name:
        :param starting_position:
        :param other_kinesis_settings:
        :return:
        """
        conn = Qubole.agent()
        if other_kinesis_settings is None:
            other_kinesis_settings = {"kinesis.executor.maxFetchTimeInMs": 1000,
                                      "kinesis.executor.maxFetchRecordsPerShard": 100000,
                                      "kinesis.executor.maxRecordPerRead": 10000}

        data = {
            "data": {
                "attributes": {
                    "fields": {
                        "endpoint_url": endpoint_url,
                        "stream_name": stream_name,
                        "schema": schema,
                        "starting_position": starting_position,
                        "format": data_format,
                        "other_kinesis_settings": other_kinesis_settings
                    },
                    "data_store": "kinesis"
                },
                "type": "source"
            }
        }
        return conn.put(url, data)

    @staticmethod
    def _source_s3(url, schema, data_format, path, other_settings=None):
        """
        :param url: API url with pipeline id
        :param schema:
        :param data_format:
        :param other_settings: {"fileNameOnly": "false", "latestFirst": "false"}
        :return:
        """
        conn = Qubole.agent()
        if other_settings is None:
            other_settings = {"fileNameOnly": "false", "latestFirst": "false"}
        data = {
            "data": {
                "attributes": {
                    "fields": {
                        "path": path,
                        "schema": schema,
                        "format": data_format,
                        "other_settings": other_settings
                    },
                    "data_store": "s3"
                },
                "type": "source"
            }
        }
        return conn.put(url, data)

    @staticmethod
    def _source_google_storage(url, schema, data_format, source_path, other_settings=None):
        """
        :param url: API url with pipeline id
        :param schema:
        :param data_format:
        :param other_settings:
        :return:
        """
        if other_settings is None:
            other_settings = {"fileNameOnly": "false", "latestFirst": "false"}
        conn = Qubole.agent()
        data = {"data":
                    {"attributes":
                         {"fields":
                              {"path": source_path,
                               "format": data_format,
                               "schema": schema,
                               "other_settings": other_settings
                               },
                          "data_store": "googleStorage"},
                     "type": "source"
                     }
                }
        return conn.put(url, data)

    @staticmethod
    def _sink_kafka(url, data_format, kafka_bootstrap_server, topic, other_kafka_settings=None):
        """

        :param url:
        :param data_format:
        :param kafka_bootstrap_server:
        :param topic:
        :param other_kafka_settings:
        :return:
        """
        conn = Qubole.agent()
        if other_kafka_settings is None:
            other_kafka_settings = {"kafka.max.block.ms": 60000}
        data = {"data": {"attributes": {
            "fields": {"kafka_bootstrap_server": kafka_bootstrap_server, "topic": topic,
                       "format": data_format, "other_kafka_settings": other_kafka_settings},
            "data_store": "kafka"},
            "type": "sink"}}
        return conn.put(url, data)

    @staticmethod
    def _sink_s3(url, data_format, path, partition_by, other_configurations=None):
        """

        :param url:
        :param data_format:
        :param path:
        :param partition:
        :param other_configurations:
        :return:
        """
        conn = Qubole.agent()
        if other_configurations is None:
            other_configurations = {}
        if partition_by is None:
            partition_by = ""
        data = {"data": {"attributes": {
            "fields": {"path": path, "partition_by": partition_by,
                       "other_configurations": other_configurations, "format": data_format}, "data_store": "s3"},
            "type": "sink"}}
        return conn.put(url, data)

    @staticmethod
    def _sink_hive(url, table_name, databases="default", default_other_configurations=None):
        """

        :param url:
        :param table_name:
        :param databases:
        :param default_other_configurations:
        :return:
        """
        conn = Qubole.agent()
        if default_other_configurations is None:
            default_other_configurations = {"table.metastore.stopOnFailure": "false",
                                            "table.metastore.updateIntervalSeconds": 10}

        data = {"data": {"attributes": {
            "fields": {"database": databases, "table_name": table_name,
                       "other_configurations": default_other_configurations},
            "data_store": "hive"},
            "type": "sink"}}
        return conn.put(url, data)

    @staticmethod
    def _sink_snowflake(url, format, **kwargs):
        """

        :param url: API url with pipeline id
        :param format:
        :param kwargs:
        :return:
        """
        pass

    @staticmethod
    def _sink_google_storage(url, format, sink_path, partition_by, other_configurations):
        """

        :param url:
        :param format:
        :param sink_path:
        :param partition_by:
        :param other_configurations:
        :return:
        """
        conn = Qubole.agent()
        data = {"data": {"attributes": {
            "fields": {"path": sink_path, "partition_by": partition_by,
                       "other_configurations": other_configurations, "format": format},
            "data_store": "googleStorage"}, "type": "sink"}}
        return conn.put(url, data)

    @staticmethod
    def add_registry(registry_name, host,
                     port=8081,
                     registry_type='schema',
                     use_gateway=False,
                     gateway_ip=None,
                     gateway_port=None,
                     gateway_username=None,
                     gateway_private_key=None,
                     **kwargs):
        """
        :param registry_name: Name of Registry
        :param registry_type:
        :param host:
        :param port:
        :param use_gateway:
        :param gateway_ip:
        :param gateway_port:
        :param gateway_username:
        :param gateway_private_key:
        :return:
        """
        conn = Qubole.agent()
        url = QuestAssisted.rest_entity_path + '/' + 'quest_data_registries'
        data = {"data": {
            "attributes": {"name": registry_name, "description": kwargs.get("description"),
                           "registry_type": registry_type, "host": host,
                           "port": port, "use_gateway": use_gateway, "gateway_ip": gateway_ip,
                           "gateway_port": gateway_port, "gateway_username": gateway_username,
                           "gateway_private_key": gateway_private_key}, "type": "schemas"}}
        conn.post(url, data)

    @staticmethod
    def switch_from_assisted(pipeline_id):
        """
        Switch pipeline from assisted BYOC or BYOJ
        :param pipeline_id:
        :return:
        """
        conn = Qubole.agent()
        url = QuestAssisted.rest_entity_path + '/' + pipeline_id
        response = conn.get(url)
        create_type = response.get("data").get("attributes").get("create_type")
        if create_type == 1:
            url = QuestAssisted.rest_entity_path + '/' + pipeline_id + '/switch'
            data = {"data": {"attributes": {"create_type": QuestAssisted.create_type}}}
            response = conn.get(url, data)
            return response
        else:
            log.info("Pipeline is already in non-Assisted mode, create_type = {}".format(create_type))
