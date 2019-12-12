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
            raise ParseError("Please select only one param out of --start, --pause, --delete, --archive, --clone and --edit.")
        return json.dumps(response, default=lambda o: o.attributes, sort_keys=True, indent=4)

    @staticmethod
    def index(args):
        pipelinelist = Quest.index(args.status)
        return json.dumps(pipelinelist, default=lambda o: o.attributes, sort_keys=True, indent=4)

    @staticmethod
    def create(args):
        pipeline = None
        print "create type = ", type(args.create_type)
        if args.create_type == "1":
            pipeline = Assisted.create(args.name, args.create_type, args)
        elif args.create_type == "2":
            pipeline = Jar.create(args.name, args.create_type, args)
        elif args.create_type == "3":
            c = Code()
            c.create(args.name, args)
        return json.dumps(pipeline, default=lambda o: o.attributes, sort_keys=True, indent=4)


class Quest(Resource):
    """
    qds_sdk.Quest is the base Qubole Quest class.
    """

    """ all commands use the /pipelines endpoint"""

    rest_entity_path = "pipelines"

    @staticmethod
    def index(status=None):
        if status is None or status == 'all':
            params = {"filter": "draft,archive,active"}
        else:
            params = {"filter": status}
        conn = Qubole.agent()
        url_path = Quest.rest_entity_path
        questjson = conn.get(url_path, params)
        return questjson

    @staticmethod
    def create(name, args):
        pass

    def add_source(self, pipeline_id, args):
        data_store = args.data_store
        if data_store == 's3':
            return self.s3_source(pipeline_id, args)
        elif data_store == 'kafka':
            return self.kafka_source(pipeline_id, args)
        elif data_store == 'kinesis':
            return self.kinesis_source(pipeline_id, args)
        else:
            log.error("Provide a valid data source {s3, kafka, kinesis}")

    def add_sink(self, pipeline_id, args):
        data_store = args.get("data_store")
        if data_store == 's3':
            return self.s3_sink(pipeline_id, args)
        elif data_store == 'kafka':
            return self.kafka_sink(pipeline_id, args)
        elif data_store == 'kinesis':
            return self.kinesis_sink(pipeline_id, args)
        elif data_store == 'snowflake':
            return self.kinesis_sink(pipeline_id, args)
        elif data_store == 'hive':
            return self.kinesis_sink(pipeline_id, args)
        else:
            log.error("Provide a valid data source {s3, kafka, kinesis}")

    def s3_source(self, pipeline_id, args):
        data_store = "s3"
        type = "source"
        schema = args.schema
        name = args.source_name
        path = args.source_path
        format = args.format
        other_settings = {"fileNameOnly": "false", "latestFirst": "false"}
        conn = Qubole.agent()
        url = self.rest_entity_path + "/" + str(pipeline_id) + '/node'
        data = {"data": {"attributes": {"fields": {"name": name, "path": path, "format": format, "schema": schema,
                                                   "other_settings": other_settings}, "data_store": data_store},
                         "type": type}}
        return conn.post(url, data)

    def add_property(self):
        pass

    @staticmethod
    def pause(pipeline_id):
        url = Quest.rest_entity_path + "/" + pipeline_id + "/pause"
        conn = Qubole.agent()
        return conn.put(url)

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
    def status(pipeline_id):
        conn = Qubole.agent()
        url = Quest.rest_entity_path + "/" + pipeline_id + "/status"
        response = conn.put(url)
        log.info(response)
        return response

    @staticmethod
    def delete(pipeline_id):
        conn = Qubole.agent()
        url = Quest.rest_entity_path + "/" + pipeline_id + "/delete"
        response = conn.put(url)
        log.info(response)
        return response

    def get_pipline_id(self, response):
        return response.get('data').get('id')

    def kafka_source(self, **kwargs):
        pass

    def kinesis_source(self, **kwargs):
        pass

    def health(self):
        pass

    def clone(self):
        pass

    def save_code(self):
        pass

    def edit(self):
        pass


class Code(Quest):
    def __init__(self):
        self.create_type = 3

    def create(self, name, args):
        conn = Qubole.agent()
        data = {"data": {"attributes": {"name": name, "status": "DRAFT", "description": args.description,
                                        "create_type": self.create_type}, "type": "pipelines"}}
        url = Quest.rest_entity_path + "?mode=wizard"
        response = conn.post(url, data)
        pipeline_id = response.get("data").get("id")
        property = self.add_property(pipeline_id, cluster_label=args.cluster_label)
        save_code = self.save_code(pipeline_id, script_location=args.script_location, code=args.code,
                                   language=args.language)
        return save_code

    def add_property(self, pipeline_id, **kwargs):
        conn = Qubole.agent()
        data = {"data": {"attributes": {
            "cluster_label": kwargs.get("cluster_label"),
            "can_retry": "true",
            "checkpoint_location": kwargs.get("checkpoint_location"),
            "trigger_interval": kwargs.get("trigger_interval"),
            "output_mode": kwargs.get("output_mode"),
            "command_line_options": "--conf spark.driver.extraLibraryPath=/usr/lib/hadoop2/lib/native\n--conf spark.eventLog.compress=true\n--conf spark.eventLog.enabled=true\n--conf spark.sql.streaming.qubole.enableStreamingEvents=true\n--conf spark.qubole.event.enabled=true"
        },
            "type": "pipeline/properties"
        }
        }
        url = self.rest_entity_path + "/" + str(pipeline_id) + "/properties/"
        response = conn.put(url, data)
        return response

    def save_code(self, pipeline_id, **kwargs):
        try:
            code = None
            if kwargs.get("code"):
                code = kwargs.get('code')
            elif kwargs.get("script_location"):
                q = open(kwargs.get("script_location")).read()
                code = q
        except IOError as e:
            raise ParseError("Unable to open script location or script location and code both are empty")

        language = kwargs.get("language")
        data = {"data": {"attributes": {"create_type": self.create_type, "code": str(code), "language": str(language)}}}
        conn = Qubole.agent()
        url = self.rest_entity_path + "/" + str(pipeline_id) + "/save_code"
        response = conn.put(url, data)
        return response

    def start(self, pipeline_id):
        return super(Quest, self).start(pipeline_id)


class Jar(Quest):
    pass


class Assisted(Quest):
    pass
