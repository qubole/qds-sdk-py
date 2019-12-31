"""
The quest module contains the base definition for
a generic quest commands.
"""

import json
from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser
from qds_sdk.commands import *
from qds_sdk.actions import *

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
            "attributes": {"name": pipeline_name, "status": "DRAFT", "create_type": create_type},
            "type": "pipelines"}}
        url = Quest.rest_entity_path + "?mode=wizard"
        response = conn.post(url, data)
        return response

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
        pipeline_status = response.get('data').get('pipeline_instance_status')
        while pipeline_status == 'waiting':
            log.info("Pipeline is in waiting state....")
            time.sleep(10)
            response = conn.put(url)
            pipeline_status = response.get('data').get('pipeline_instance_status')
        log.info("State of pipeline is %s " % pipeline_status)
        return response

    @staticmethod
    def add_property(pipeline_id, cluster_label, checkpoint_location, output_mode, trigger_interval=None,
                     can_retry=True, **kwargs):
        """
        Method to add properties in pipeline
        :param can_retry:
        :param pipeline_id:
        :param cluster_label:
        :param checkpoint_location:
        :param trigger_interval:
        :param output_mode:
        :param **kwargs: command_line_options
        :return:
        """
        conn = Qubole.agent()
        default_cmdline = "--conf spark.driver.extraLibraryPath=/usr/lib/hadoop2/lib/native\n--conf spark.eventLog.compress=true\n--conf spark.eventLog.enabled=true\n--conf spark.sql.streaming.qubole.enableStreamingEvents=true\n--conf spark.qubole.event.enabled=true"
        data = {"data": {"attributes": {
            "cluster_label": cluster_label,
            "can_retry": can_retry,
            "checkpoint_location": checkpoint_location,
            "trigger_interval": trigger_interval,
            "output_mode": output_mode,
            "command_line_options": kwargs.get("command_line_options", default_cmdline)
        },
            "type": "pipeline/properties"
        }
        }
        log.info("Data {}".format(data))
        url = Quest.rest_entity_path + "/" + pipeline_id + "/properties/"
        response = conn.put(url, data)
        return response

    def health(self):
        pass

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
    def edit(pipeline_id, **kwargs):
        pass

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

    # @staticmethod
    # def status(pipeline_id):
    #     conn = Qubole.agent()
    #     url = Quest.rest_entity_path + "/" + pipeline_id + "/status"
    #     response = conn.put(url)
    #     log.info(response)
    #     return response

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


class QuestCode(Quest):
    create_type = 3

    @staticmethod
    def create_pipeline(pipeline_name, code_or_fileLoc, cluster_label, checkpoint_location,
                        language='scala', **kwargs):
        """
        Method to create pipeline in BYOC mode in one go.
        :param checkpoint_location:
        :param cluster_label:
        :param pipeline_name: Name by which pipeline will be created.
        :param code_or_fileLoc: code/location of file
        :param language: scala/python
        :param kwargs: other params are checkpoint_location, trigger_interval, outputmode, can_retry(true by default)
        :return: pipeline id
        """
        response = Quest.create(pipeline_name, QuestCode.create_type)
        log.info(response)
        pipeline_id = Quest.get_pipline_id(response)
        property = Quest.add_property(pipeline_id, cluster_label, checkpoint_location,
                                      trigger_interval=kwargs.get("trigger_interval"),
                                      output_mode=kwargs.get("output_mode"), can_retry=kwargs.get("can_retry"))
        log.info(property)
        save_response = QuestCode.save_code(pipeline_id, code_or_fileLoc=code_or_fileLoc, language=language)
        if kwargs.get("channel_id"):
            response = Quest.set_alert(pipeline_id, kwargs.get("channel_id"))
            log.info(response)
        pipeline_id = Quest.get_pipline_id(save_response)
        return pipeline_id

    @staticmethod
    def edit(pipeline_id, **kwargs):
        """
        Method to Edit pipeline
        :param pipeline_id: pipeline id
        :return:
        """
        checkpoint_location = kwargs.get("checkpoint_location")
        cluster_label = kwargs.get("cluster_label")
        code = kwargs.get("code_or_fileLoc")
        language = kwargs.get("language")
        trigger_interval = kwargs.get("trigger_interval")
        output_mode = kwargs.get("output_mode")
        can_retry = kwargs.get("can_retry")
        property = Quest.add_property(pipeline_id, cluster_label, checkpoint_location,
                                      trigger_interval=trigger_interval, output_mode=output_mode, can_retry=can_retry)
        log.info(property)
        save_response = QuestCode.save_code(pipeline_id, code, language=language)
        return save_response

    @staticmethod
    def save_code(pipeline_id, code_or_fileLoc, language='scala'):
        """
        Method to save code
        :param pipeline_id:
        :param code_or_fileLoc:
        :param language:
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
                        **kwargs):
        """
        Method to create pipeline in BYOC mode in one go.
        :param main_class_name:
        :param checkpoint_location:
        :param cluster_label:
        :param jar_path:
        :param pipeline_name: Name by which pipeline will be created.
        :param create_type:
        :param kwargs:
        :return: pipeline id
        """
        response = Quest.create(pipeline_name, QuestJar.create_type)
        log.info(response)
        pipeline_id = Quest.get_pipline_id(response)
        property = Quest.add_property(pipeline_id, cluster_label, checkpoint_location, **kwargs)
        log.info(property)
        save_response = QuestJar.save_code(pipeline_id, jar_path, main_class_name, **kwargs)
        pipeline_id = Quest.get_pipline_id(save_response)
        if kwargs.get("channel_id"):
            response = Quest.set_alert(pipeline_id, kwargs.get("channel_id"))
            log.info(response)
        return pipeline_id

    @staticmethod
    def edit(pipeline_id, **kwargs):
        """
        Method to Edit pipeline
        :param pipeline_id: pipeline id
        :param kwargs: checkpoint_location, cluster_label, code_or_fileLoc
        :return:
        """
        checkpoint_location = kwargs.get("checkpoint_location")
        cluster_label = kwargs.get("cluster_label")
        code = kwargs.get("code_or_fileLoc")
        language = kwargs.get("language")
        trigger_interval = kwargs.get("trigger_interval")
        output_mode = kwargs.get("output_mode")
        can_retry = kwargs.get("can_retry")
        property = Quest.add_property(pipeline_id, cluster_label, checkpoint_location,
                                      trigger_interval=trigger_interval, output_mode=output_mode, can_retry=can_retry)
        log.info(property)
        save_response = QuestJar.save_code(pipeline_id, create_type=3, code_or_fileLoc=code, language=language)
        return save_response

    @staticmethod
    def save_code(pipeline_id, jar_path, main_class_name, **kwargs):
        """
        :param pipeline_id:
        :param jar_path:
        :param main_class_name:
        :param kwargs: user_arguments
        :return:
        """
        if jar_path is None:
            raise ParseError("Unable to open script location or script location and code both are empty")

        data = {"data": {
            "attributes": {"create_type": 2, "user_arguments": kwargs.get("user_arguments"), "jar_path": jar_path,
                           "language": main_class_name}}}
        conn = Qubole.agent()
        url = Quest.rest_entity_path + "/" + str(pipeline_id) + "/save_code"
        response = conn.put(url, data)
        return response


class QuestAssisted(Quest):
    create_type = 1

    @staticmethod
    def add_source(pipeline_id, schema, format, data_store, **kwargs):
        """
        Method to add source in assisted mode pipeline.
        :param pipeline_id: id of pipeline for which source need to be added/updated.
        :param source_path: location of data
        :param schema: key value pair eg: {"id":"Integer"}
        :param format: JSON/Avro/Parquet/ORC.
        :param other_settings: key value pairs eg: {"fileNameOnly": "false", "latestFirst": "false"}
        :return: response in dict format
        """
        url = Quest.rest_entity_path + "/" + pipeline_id + "/node"
        if data_store == "kinesis":
            return QuestAssisted._source_kinesis(url, schema, format, kwargs.get("endpoint_url"),
                                                 kwargs.get("stream_name"),
                                                 starting_position=kwargs.get("starting_position"),
                                                 other_kinesis_settings=kwargs.get("other_kinesis_settings"))
        if data_store == "kafka":
            return QuestAssisted._source_kafka(url, schema, format, kwargs.get("broker"), kwargs.get("topics"),
                                               topic_type=kwargs.get("topic_type"),
                                               use_registry=kwargs.get("use_registry"),
                                               registry_subject=kwargs.get("registry_subject"),
                                               registry_id=kwargs.get("registry_id"),
                                               starting_offsets=kwargs.get("starting_offsets"),
                                               other_kafka_consumer_settings=kwargs.get(
                                                   "other_kafka_consumer_settings"))
        if data_store == "s3":
            return QuestAssisted._source_s3(url, schema, format, kwargs.get("source_path"),
                                            other_settings=kwargs.get("other_settings"))
        if data_store == "google_storage":
            return QuestAssisted._source_google_storage(url, schema, format, kwargs.get("source_path"))
        raise ParseError("Please add only one valid source out of [kafka, s3, kinesis]")

    @staticmethod
    def add_sink(pipeline_id, format, data_store, **kwargs):
        """
        Method to add sink for given pipeline.
        :param pipeline_id:
        :param sink_path:
        :param schema:
        :param format:
        :param data_store:
        :param kwargs:
        :return:
        """
        url = Quest.rest_entity_path + "/" + pipeline_id + "/node"
        if data_store == "kafka":
            return QuestAssisted._sink_kafka(url, format, kwargs.get("kafka_bootstrap_server"), kwargs.get("topic"),
                                             other_kafka_settings=kwargs.get("other_kafka_settings"))
        if data_store == "s3":
            return QuestAssisted._sink_s3(url, format, kwargs.get("sink_path"), kwargs.get("partition_by"),
                                          other_configurations=kwargs.get("other_configurations"))
        if data_store == "snowflake":
            return QuestAssisted._sink_snowflake(url, format, **kwargs)
        if data_store == "google_storage":
            QuestAssisted._sink_google_storage(url, format, kwargs.get("sink_path"), kwargs.get("partition_by"),
                                               other_configurations=kwargs.get("other_configurations"))
        if data_store == "hive":
            QuestAssisted._sink_hive(url, kwargs.get("table_name"), **kwargs)
        raise ParseError("Please add only one valid sink out of [kafka, s3, snowflake, hive, google_storage]")

    @staticmethod
    def create_pipeline(pipeline_name, schema, format, source_data_store, sink_data_store, checkpoint_location,
                        cluster_label, output_mode, **kwargs):
        """

        :param pipeline_name:
        :param schema:
        :param format:
        :param source_data_store:
        :param sink_data_store:
        :param checkpoint_location:
        :param cluster_label:
        :param kwargs: source_path, sink_path, output_mode, can_retry, operator, column_name
        :return:
        """
        response = Quest.create(pipeline_name, QuestAssisted.create_type)
        log.info(response)
        pipeline_id = Quest.get_pipline_id(response)
        pipeline_id = str(pipeline_id)
        src_response = QuestAssisted.add_source(pipeline_id, schema, format, source_data_store, **kwargs)
        log.info(src_response)
        sink_reponse = QuestAssisted.add_sink(pipeline_id, format, sink_data_store, **kwargs)
        log.info(sink_reponse)
        property_response = QuestAssisted.add_property(pipeline_id, cluster_label, checkpoint_location,
                                                       output_mode,
                                                       trigger_interval=kwargs.get("trigger_interval", None),
                                                       can_retry=kwargs.get("can_retry", True),
                                                       command_line_options=kwargs.get("command_line_options"))
        log.info(property_response)
        if kwargs.get("operator"):
            operator_response = QuestAssisted.add_operator(kwargs.get("operator"), pipeline_id,
                                                           kwargs.get("column_name"),
                                                           **kwargs)
            log.info(operator_response)
        if kwargs.get("channel_id"):
            response = QuestAssisted.set_alert(pipeline_id, kwargs.get("channel_id"))
            log.info(response)
        return response

    @staticmethod
    def add_operator(operator, pipeline_id, column_name, **kwargs):
        """

        :param operator:
        :param pipeline_id:
        :param column_name:
        :param kwargs: condition, value, frequency, sliding_window_value_frequency, window_interval_frequency, other_columns
        :return:
        """
        url = Quest.rest_entity_path + "/" + pipeline_id + "/operator"
        if operator is None:
            return
        if operator == "filter":
            return QuestAssisted._filter_operator(url, column_name, kwargs.get("condition"), kwargs.get("value"))
        if operator == "select":
            return QuestAssisted._select_operator(url, column_name)
        if operator == "watermark":
            return QuestAssisted._watermark_operator(url, column_name, kwargs.get("frequency"))
        if operator == "window_group":
            return QuestAssisted._window_group_operator(url, column_name, kwargs.get("sliding_window_value_frequency"),
                                                        kwargs.get("window_interval_frequency"),
                                                        kwargs.get("other_columns"))
        raise ParseError("Please add only one valid sink out of [kafka, s3, snowflake, hive, google_storage]")

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
    def _window_group_operator(url, column_name, sliding_window_value_frequency, window_interval_frequency,
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
                                        "window_expression": {"column_name": column_name, "sliding_window_value": {
                                            "frequency": sliding_window_value_frequency,
                                            "unit": "minute"},
                                                              "window_interval": {
                                                                  "frequency": window_interval_frequency,
                                                                  "unit": "minute"}}, "other_columns": other_columns,
                                        "action": "count"}}}
        return conn.put(url, data)

    @staticmethod
    def _source_kafka(url, schema, format, broker, topics, **kwargs):
        """
        :param url: API url with pipeline id
        :param schema:
        :param format: JSON/AVRO/ORC/Parquet
        :param kwargs: topic_type, use_registry, registry_subject, registry_id, starting_offsets, other_kafka_consumer_settings
        :return:
        """
        conn = Qubole.agent()
        default_kafka = {"kafkaConsumer.pollTimeoutMs": 512, "fetchOffset.numRetries": 3,
                         "fetchOffset.retryIntervalMs": 10}
        data = {
            "data": {
                "attributes": {
                    "fields": {
                        "brokers": broker,
                        "topics": topics,
                        "topic_type": kwargs.get("topic_type", "multiple"),
                        "schema": schema,
                        "use_registry": kwargs.get("use_registry", "write"),
                        "registry_subject": kwargs.get("registry_subject", None),
                        "registry_id": kwargs.get("registry_id", None),
                        "starting_offsets": kwargs.get("starting_offsets", "latest"),
                        "format": format,
                        "other_kafka_consumer_settings": kwargs.get("other_kafka_consumer_settings", default_kafka)
                    },
                    "data_store": "kafka"
                },
                "type": "source"
            }
        }
        return conn.put(url, data)

    @staticmethod
    def _source_kinesis(url, schema, format, endpoint_url, stream_name, **kwargs):
        """
        :param url: API url with pipeline id
        :param schema:
        :param format:
        :param kwargs: other_kinesis_settings, starting_position,
        :return:
        """
        conn = Qubole.agent()
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
                        "starting_position": kwargs.get("starting_position", "latest"),
                        "format": format,
                        "other_kinesis_settings": kwargs.get("other_kinesis_settings", other_kinesis_settings)
                    },
                    "data_store": "kinesis"
                },
                "type": "source"
            }
        }
        return conn.put(url, data)

    @staticmethod
    def _source_s3(url, schema, format, path, **kwargs):
        """
        :param url: API url with pipeline id
        :param schema:
        :param format:
        :param kwargs: other_settings
        :return:
        """
        conn = Qubole.agent()
        other_settings = {"fileNameOnly": "false", "latestFirst": "false"}
        data = {
            "data": {
                "attributes": {
                    "fields": {
                        "path": path,
                        "schema": schema,
                        "format": format,
                        "other_settings": kwargs.get("other_settings", other_settings)
                    },
                    "data_store": "s3"
                },
                "type": "source"
            }
        }
        return conn.put(url, data)

    @staticmethod
    def _source_google_storage(url, schema, format, source_path, **kwargs):
        """
        :param url: API url with pipeline id
        :param schema:
        :param format:
        :param kwargs:
        :return:
        """
        conn = Qubole.agent()
        other_settings = {"fileNameOnly": "false", "latestFirst": "false"}
        data = {"data":
                    {"attributes":
                         {"fields":
                              {"path": source_path,
                               "format": format,
                               "schema": schema,
                               "other_settings": kwargs.get("other_settings", other_settings)
                               },
                          "data_store": "googleStorage"},
                     "type": "source"
                     }
                }
        return conn.put(url, data)

    @staticmethod
    def _sink_kafka(url, format, kafka_bootstrap_server, topic, **kwargs):
        """
        :param url: API url with pipeline id
        :param format:
        :param kwargs: other_kafka_settings
        :return:
        """
        conn = Qubole.agent()
        default = {"kafka.max.block.ms": 60000}
        data = {"data": {"attributes": {
            "fields": {"kafka_bootstrap_server": kafka_bootstrap_server, "topic": topic,
                       "format": format, "other_kafka_settings": kwargs.get("other_kafka_settings", default)},
            "data_store": "kafka"},
            "type": "sink"}}
        return conn.put(url, data)

    @staticmethod
    def _sink_s3(url, format, path, partition, **kwargs):
        """

        :param url: API url with pipeline id
        :param format:
        :param kwargs: other_configurations
        :return:
        """
        conn = Qubole.agent()
        data = {"data": {"attributes": {
            "fields": {"path": path, "partition_by": partition,
                       "other_configurations": kwargs.get("other_configurations"), "format": format},
            "data_store": "s3"}, "type": "sink"}}
        return conn.put(url, data)

    @staticmethod
    def _sink_hive(url, table_name, **kwargs):
        """

        :param url: API url with pipeline id
        :param format:
        :param kwargs: other_configuration
        :return:
        """
        conn = Qubole.agent()
        default_other_configurations = {"table.metastore.stopOnFailure": "false",
                                        "table.metastore.updateIntervalSeconds": 10}
        data = {"data": {"attributes": {
            "fields": {"database": kwargs.get("databases", "default"), "table_name": table_name,
                       "other_configurations": kwargs.get("other_configuration", default_other_configurations)},
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
    def _sink_google_storage(url, format, sink_path, partition_by, **kwargs):
        """
        :param url: API url with pipeline id
        :param format:
        :param kwargs: other_configurations
        :return:
        """
        conn = Qubole.agent()
        data = {"data": {"attributes": {
            "fields": {"path": sink_path, "partition_by": partition_by,
                       "other_configurations": kwargs.get("other_configurations"), "format": format},
            "data_store": "googleStorage"}, "type": "sink"}}
        return conn.put(url, data)

    @staticmethod
    def add_registry(registry_name, host, port=8081, registry_type='schema', use_gateway=False, gateway_ip=None, gateway_port=None,
                     gateway_username=None, gateway_private_key=None, **kwargs):
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
            "attributes": {"name": registry_name, "description": kwargs.get("description"), "registry_type": registry_type, "host": host,
                           "port": port, "use_gateway": use_gateway, "gateway_ip": gateway_ip,
                           "gateway_port": gateway_port, "gateway_username": gateway_username,
                           "gateway_private_key": gateway_private_key}, "type": "schemas"}}
        conn.post(url, data)
