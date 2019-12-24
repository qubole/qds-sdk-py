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
        return response.get('data').get('id')

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
            **kwargs: keyword arguments specific to create type

        Returns:
            Command object
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
    def add_property(pipeline_id, cluster_label, checkpoint_location, trigger_interval=None, output_mode=None,
                     can_retry=True):
        conn = Qubole.agent()
        data = {"data": {"attributes": {
            "cluster_label": cluster_label,
            "can_retry": can_retry,
            "checkpoint_location": checkpoint_location,
            "trigger_interval": trigger_interval,
            "output_mode": output_mode,
            "command_line_options": "--conf spark.driver.extraLibraryPath=/usr/lib/hadoop2/lib/native\n--conf spark.eventLog.compress=true\n--conf spark.eventLog.enabled=true\n--conf spark.sql.streaming.qubole.enableStreamingEvents=true\n--conf spark.qubole.event.enabled=true"
        },
            "type": "pipeline/properties"
        }
        }
        url = Quest.rest_entity_path + "/" + str(pipeline_id) + "/properties/"
        response = conn.put(url, data)
        return response

    @staticmethod
    def save_code(pipeline_id, create_type, code_or_fileLoc=None, jar_file_loc=None, language='scala'):
        try:
            code = None
            if code_or_fileLoc:
                if os.path.isdir(code_or_fileLoc):
                    q = open(code_or_fileLoc).read()
                    code = q
                else:
                    code = code_or_fileLoc
            elif jar_file_loc:
                code = jar_file_loc

        except IOError as e:
            raise ParseError("Unable to open script location or script location and code both are empty")

        data = {"data": {"attributes": {"create_type": create_type, "code": str(code), "language": str(language)}}}
        conn = Qubole.agent()
        url = Quest.rest_entity_path + "/" + str(pipeline_id) + "/save_code"
        response = conn.put(url, data)
        return response

    def health(self):
        pass

    @staticmethod
    def clone(pipeline_id):
        url = Quest.rest_entity_path + "/" + pipeline_id + "/duplicate"
        log.info("Cloning pipeline with id {}".format(pipeline_id))
        conn = Qubole.agent()
        return conn.put(url)

    @staticmethod
    def pause(pipeline_id):
        url = Quest.rest_entity_path + "/" + pipeline_id + "/pause"
        log.info("Pausing pipeline with id {}".format(pipeline_id))
        conn = Qubole.agent()
        return conn.put(url)

    @staticmethod
    def edit(pipeline_id, **kwargs):
        pass

    @staticmethod
    def archive(pipeline_id):
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
        conn = Qubole.agent()
        url = Quest.rest_entity_path + "/" + pipeline_id + "/delete"
        log.info("Deleting Pipeline with id: {}".format(pipeline_id))
        response = conn.put(url)
        log.info(response)
        return response

    @staticmethod
    def edit_pipeline_name(pipeline_id, pipeline_name):
        conn = Qubole.agent()
        url = Quest.rest_entity_path + "/" + pipeline_id
        data = {"data": {"attributes": {"name": pipeline_name}, "type": "pipelines"}}
        return conn.put(url, data)


class QuestCode(Quest):
    @staticmethod
    def create_pipeline(pipeline_name, create_type, code_or_fileLoc, cluster_label, checkpoint_location,
                        language='scala', **kwargs):
        """
        Method to create pipeline in BYOC mode in one go.
        :param pipeline_name: Name by which pipeline will be created.
        :param create_type: 1->Assisted Mode, 2->BYOJ mode, 3->BYOC mode
        :param code_or_fileLoc: code/location of file
        :param language: scala/python
        :param kwargs: other params are checkpoint_location, trigger_interval, outputmode, can_retry(true by default)
        :return: pipeline id
        """
        response = Quest.create(pipeline_name, create_type)
        log.info(response)
        pipeline_id = Quest.get_pipline_id(response)
        property = Quest.add_property(pipeline_id, cluster_label, checkpoint_location,
                                      trigger_interval=kwargs.get("trigger_interval"),
                                      output_mode=kwargs.get("output_mode"), can_retry=kwargs.get("can_retry"))
        log.info(property)
        save_response = Quest.save_code(pipeline_id, create_type=3, code_or_fileLoc=code_or_fileLoc, language=language)
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
        save_response = Quest.save_code(pipeline_id, create_type=3, code_or_fileLoc=code, language=language)
        return save_response


class QuestJar(Quest):
    @staticmethod
    def create_pipeline(pipeline_name, create_type, jar_file, cluster_label, checkpoint_location, language='scala',
                        **kwargs):
        """
        Method to create pipeline in BYOC mode in one go.
        :param pipeline_name: Name by which pipeline will be created.
        :param create_type: 1->Assisted Mode, 2->BYOJ mode, 3->BYOC mode
        :param code_or_fileLoc: code/location of file
        :param language: scala/python
        :param kwargs: other params are checkpoint_location, trigger_interval, outputmode, can_retry(true by default)
        :return: pipeline id
        """
        response = Quest.create(pipeline_name, create_type)
        log.info(response)
        pipeline_id = Quest.get_pipline_id(response)
        property = Quest.add_property(pipeline_id, cluster_label, checkpoint_location, **kwargs)
        log.info(property)
        save_response = Quest.save_code(pipeline_id, create_type, jar_file_loc=jar_file, language=language)
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
        save_response = Quest.save_code(pipeline_id, create_type=3, code_or_fileLoc=code, language=language)
        return save_response


class QuestAssisted(Quest):
    @staticmethod
    def add_source(pipeline_id, source_path, schema, format, data_store, **kwargs):
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
        sources = {"kafka": QuestAssisted._source_kafka, "kinesis": QuestAssisted._source_kinesis,
                   "s3": QuestAssisted._source_s3, "google_storage": QuestAssisted._source_google_storage}
        if data_store in sources.keys():
            return sources[data_store](url, pipeline_id, source_path, schema, format, data_store, **kwargs)
        raise ParseError("Please add only one valid source out of [kafka, s3, kinesis]")

    @staticmethod
    def add_sink(pipeline_id, source_path, schema, format, data_store, **kwargs):
        """
        Method to add sink for given pipeline.
        :param pipeline_id:
        :param source_path:
        :param schema:
        :param format:
        :param data_store:
        :param kwargs:
        :return:
        """
        url = Quest.rest_entity_path + "/" + pipeline_id + "/node"
        sources = {"kafka": QuestAssisted._sink_kafka, "snowflake": QuestAssisted._sink_snowflake,
                   "hive": QuestAssisted._sink_hive,
                   "s3": QuestAssisted._sink_s3, "google_storage": QuestAssisted._sink_google_storage}
        if data_store in sources.keys():
            return sources[data_store](url, pipeline_id, source_path, schema, format, **kwargs)
        raise ParseError("Please add only one valid sink out of [kafka, s3, snowflake, hive, google_storage]")

    @staticmethod
    def add_operator(operator, pipeline_id, column_name, **kwargs):
        url = Quest.rest_entity_path + "/" + pipeline_id + "/operator"
        if operator == 'filter':
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
        conn = Qubole.agent()
        data = {"data": {"attributes": {"operator": "select", "column_names": column_names}}}
        return conn.put(url, data)

    @staticmethod
    def _filter_operator(url, column_name, condition, value):
        conn = Qubole.agent()
        data = {"data": {"attributes": {"operator": "filter", "column_name": column_name,
                                        "condition": condition, "value": value}}}
        return conn.put(url, data)

    @staticmethod
    def _watermark_operator(url, column_name, frequency):
        conn = Qubole.agent()
        data = {"data": {"attributes": {"operator": "watermark", "column_name": column_name,
                                        "frequency": frequency, "unit": "minute"}}}
        return conn.put(url, data)

    @staticmethod
    def _window_group_operator(url, column_name, sliding_window_value_frequency, window_interval_frequency,
                               other_columns):
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
    def _source_kafka(url, schema, format, **kwargs):
        conn = Qubole.agent()
        default_kafka = {"kafkaConsumer.pollTimeoutMs": 512, "fetchOffset.numRetries": 3,
                         "fetchOffset.retryIntervalMs": 10}
        data = {
            "data": {
                "attributes": {
                    "fields": {
                        "name": kwargs.get("name", "source_kafka"),
                        "brokers": kwargs.get("broker"),
                        "topics": kwargs.get("topics"),
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
    def _source_kinesis(url, schema, format, **kwargs):
        conn = Qubole.agent()
        other_kinesis_settings = {"kinesis.executor.maxFetchTimeInMs": 1000,
                                  "kinesis.executor.maxFetchRecordsPerShard": 100000,
                                  "kinesis.executor.maxRecordPerRead": 10000}
        data = {
            "data": {
                "attributes": {
                    "fields": {
                        "name": kwargs.get("name", "source_kinesis"),
                        "endpoint_url": kwargs.get("endpoint_url"),
                        "stream_name": kwargs.get("stream_name"),
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
    def _source_s3(url, schema, format, **kwargs):
        conn = Qubole.agent()
        other_settings = {"fileNameOnly": "false", "latestFirst": "false"}
        data = {
            "data": {
                "attributes": {
                    "fields": {
                        "name": kwargs.get("name", "source_s3"),
                        "path": kwargs.get("path"),
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
    def _source_google_storage(url, schema, format, **kwargs):
        conn = Qubole.agent()
        other_settings = {"fileNameOnly": "false", "latestFirst": "false"}
        data = {"data":
                    {"attributes":
                         {"fields":
                              {"path": kwargs.get("path"),
                               "format": format,
                               "schema": schema,
                               "other_settings": other_settings
                               },
                          "data_store": "googleStorage"},
                     "type": "source"
                     }
                }
        return conn.put(url, data)

    @staticmethod
    def _sink_kafka(url, format, **kwargs):
        conn = Qubole.agent()
        default = {"kafka.max.block.ms": 60000}
        data = {"data": {"attributes": {
            "fields": {"kafka_bootstrap_server": kwargs.get("kafka_bootstrap_server"), "topic": kwargs.get("topic"),
                       "format": format, "other_kafka_settings": kwargs.get("other_kafka_settings", default)},
            "data_store": "kafka"},
            "type": "sink"}}
        return conn.put(url, data)

    @staticmethod
    def _sink_s3(url, format, **kwargs):
        conn = Qubole.agent()
        data = {"data": {"attributes": {
            "fields": {"path": kwargs.get("path"), "partition_by": kwargs.get("partition_by"),
                       "other_configurations": kwargs.get("other_configurations"), "format": format},
            "data_store": "s3"}, "type": "sink"}}
        return conn.put(url, data)

    @staticmethod
    def _sink_hive(url, format, **kwargs):
        conn = Qubole.agent()
        default_other_configurations = {"table.metastore.stopOnFailure": "false",
                                        "table.metastore.updateIntervalSeconds": 10}
        data = {"data": {"attributes": {
            "fields": {"database": kwargs.get("databases", "default"), "table_name": kwargs.get("table_name"),
                       "other_configurations": kwargs.get("other_configuration", default_other_configurations)},
            "data_store": "hive"},
            "type": "sink"}}
        return conn.put(url, data)

    @staticmethod
    def _sink_snowflake(url, format, **kwargs):
        pass

    @staticmethod
    def _sink_google_storage(url, format, **kwargs):
        conn = Qubole.agent()
        data = {"data": {"attributes": {
            "fields": {"path": kwargs.get("path"), "partition_by": kwargs.get("partition_by"),
                       "other_configurations": kwargs.get("other_configurations"), "format": format},
            "data_store": "googleStorage"}, "type": "sink"}}
        return conn.put(url, data)
