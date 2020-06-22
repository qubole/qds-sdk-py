"""
The Pipelines module contains the base definition for
a generic Pipelines commands.
"""
from qds_sdk.actions import *
import json
from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser

log = logging.getLogger("qds_quest")

# Pattern matcher for s3 path
_URI_RE = re.compile(r's3://([^/]+)/?(.*)')


class PipelinesCmdLine:
    """qds_sdk.PipelinesCmdLine is the interface used by qds.py."""

    @staticmethod
    def parsers():
        argparser = ArgumentParser(prog="qds.py pipelines",
                                   description="Pipelines client for Qubole Data Service.")
        subparsers = argparser.add_subparsers()

        # Create
        create = subparsers.add_parser("create", help="Create a new pipeline")
        create.add_argument("--create-type", dest="create_type", required=True,
                            help="create_type=1 for assisted, "
                                 "create_type=2 for jar, create_type=3 for code")
        create.add_argument("--pipeline-name", dest="name", required=True,
                            help="Name of pipeline")
        create.add_argument("--description", dest="description", default=None,
                            help="Pipeline description"),
        create.add_argument("--cluster-label", dest="cluster_label",
                            default="default", help="Cluster label")
        create.add_argument("-c", "--code", dest="code", help="query string")
        create.add_argument("-f", "--script-location", dest="script_location",
                            help="Path where code to run is stored. "
                                 "local file path")
        create.add_argument("-l", "--language", dest="language",
                            help="Language for bring your own code, "
                                 "valid values are python and scala")
        create.add_argument("--jar-path", dest="jar_path",
                            help="Location of Jar")
        create.add_argument("--user-arguments", dest="user_arguments",
                            help="Additional user arguments")
        create.add_argument("--main-class-name", dest="main_class_name",
                            help="class name of your jar file. "
                                 "Required for create_type=2(BYOJ)")
        create.add_argument("--command-line-options",
                            dest="command_line_options",
                            help="command line options on property page.")
        create.set_defaults(func=PipelinesCmdLine.create)

        # Update/Edit
        update_properties = subparsers.add_parser("update-property",
                                                  help="Update properties of "
                                                       "a existing pipeline")
        update_properties.add_argument("--pipeline-id",
                                       dest="pipeline_id",
                                       required=True,
                                       help='Id of pipeline which need to be updated')
        update_properties.add_argument("--cluster-label", dest="cluster_label",
                                       help="Update cluster label.")
        update_properties.add_argument("--command-line-options", dest="command_line_options",
                                       help="command line options on property page.")
        update_properties.add_argument("--can-retry", dest="can_retry",
                                       help="can retry true or false")
        update_properties.set_defaults(func=PipelinesCmdLine.update_properties)
        update_code = subparsers.add_parser("update-code",
                                            help="Update code of a existing pipeline")
        update_code.add_argument(
            "-c", "--code", dest="code", help="query string")
        update_code.add_argument("-f", "--script-location", dest="script_location",
                                 help="Path where code to run is stored. local file path")
        update_code.set_defaults(func=PipelinesCmdLine.update_code)
        update_code.add_argument(
            "--jar-path",
            dest="jar_path",
            help="Location of Jar")
        update_code.add_argument("--user-arguments", dest="user_arguments",
                                 help="Additional user arguments")
        update_code.add_argument("--main-class-name", dest="main_class_name",
                                 help="class name of your jar file. "
                                      "Required for create_type=2(BYOJ)")
        update_code.add_argument("--language", dest="language",
                                 help="language of code scala or python")
        update_code.add_argument("--pipeline-id", dest="pipeline_id", required=True,
                                 help='Id of pipeline which need to be updated')

        # Pipeline Util (Utility for start, pause, clone, edit, delete,
        # archive)
        delete = subparsers.add_parser("delete", help="Delete Pipeline")
        delete.add_argument("--pipeline-id", dest="pipeline_id", required=True,
                            help='Id of pipeline which need to be started')
        delete.set_defaults(func=PipelinesCmdLine.delete)
        status = subparsers.add_parser("status", help="Status of Pipeline")
        status.add_argument("--pipeline-id", dest="pipeline_id", required=True,
                            help='Id of pipeline which need to be started')
        status.set_defaults(func=PipelinesCmdLine.status)
        start = subparsers.add_parser("start", help="Start Pipeline")
        start.add_argument("--pipeline-id", dest="pipeline_id", required=True,
                           help='Id of pipeline which need to be started')
        start.set_defaults(func=PipelinesCmdLine.start)
        pause = subparsers.add_parser("pause", help="pause Pipeline")
        pause.add_argument("--pipeline-id", dest="pipeline_id", required=True,
                           help='Id of pipeline which need to be started')
        pause.set_defaults(func=PipelinesCmdLine.pause)
        clone = subparsers.add_parser("clone", help="clone Pipeline")
        clone.add_argument("--pipeline-id", dest="pipeline_id", required=True,
                           help='Id of pipeline which need to be started')
        clone.set_defaults(func=PipelinesCmdLine.clone)
        archive = subparsers.add_parser("archive", help="archive Pipeline")
        archive.add_argument("--pipeline-id", dest="pipeline_id", required=True,
                             help='Id of pipeline which need to be started')
        archive.set_defaults(func=PipelinesCmdLine.archive)
        health = subparsers.add_parser("health", help="health of Pipeline")
        health.add_argument("--pipeline-id", dest="pipeline_id", required=True,
                            help='Id of pipeline which need to be started')
        health.set_defaults(func=PipelinesCmdLine.health)
        # list
        index = subparsers.add_parser("list", help="list of Pipeline.")
        index.add_argument("--pipeline-status", dest="status", required=True,
                           help='Id of pipeline which need to be started. '
                                'Valid values = [active, archive, all, draft] ')
        index.set_defaults(func=PipelinesCmdLine.index)
        return argparser

    @staticmethod
    def run(args):
        """
        Commandline method to run pipeline.
        :param args:
        :return:
        """
        parser = PipelinesCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)

    @staticmethod
    def delete(args):
        """
        Commandline method to delete pipeline.
        :param args:
        :return:
        """
        response = Pipelines.delete(args.pipeline_id)
        return json.dumps(
            response, default=lambda o: o.attributes, sort_keys=True, indent=4)

    @staticmethod
    def pause(args):
        """
        Commandline method to pause pipeline.
        :param args:
        :return:
        """
        response = Pipelines.pause(args.pipeline_id)
        return json.dumps(
            response, default=lambda o: o.attributes, sort_keys=True, indent=4)

    @staticmethod
    def archive(args):
        """
        commandline method to archive active pipeline.
        :param args:
        :return:
        """
        response = Pipelines.archive(args.pipeline_id)
        return json.dumps(
            response, default=lambda o: o.attributes, sort_keys=True, indent=4)

    @staticmethod
    def clone(args):
        """
        Commandline method to clone pipeline
        :param args:
        :return:
        """
        response = Pipelines.clone(args.pipeline_id)
        return json.dumps(response, default=lambda o: o.attributes, sort_keys=True, indent=4)

    @staticmethod
    def status(args):
        """
        CommandLine method to get pipeline status
        :param args:
        :return:
        """
        response = Pipelines.get_status(args.pipeline_id)
        return json.dumps(
            response, default=lambda o: o.attributes, sort_keys=True, indent=4)

    @staticmethod
    def health(args):
        """
        Commandline method to get health of pipeline.
        :param args:
        :return:
        """
        response = Pipelines.get_health(args.pipeline_id)
        return json.dumps(
            response, default=lambda o: o.attributes, sort_keys=True, indent=4)

    @staticmethod
    def start(args):
        """
        Commandline method to start pipeline.
        :param args:
        :return:
        """
        response = Pipelines.start(args.pipeline_id)
        return json.dumps(response, sort_keys=True, indent=4)

    @staticmethod
    def index(args):
        """
        Commandline method to list pipeline.
        :param args:
        :return:
        """
        pipelinelist = Pipelines.list(args.status)
        return json.dumps(
            pipelinelist, default=lambda o: o.attributes, sort_keys=True, indent=4)

    @staticmethod
    def create(args):
        """
        Commandline method to create pipeline.
        :param args:
        :return:
        """
        pipeline = None
        if int(args.create_type) == 2:
            pipeline = PipelinesJar.create_pipeline(pipeline_name=args.name,
                                                jar_path=args.jar_path,
                                                main_class_name=args.main_class_name,
                                                cluster_label=args.cluster_label,
                                                user_arguments=args.user_arguments,
                                                command_line_options=args.command_line_options)
        elif int(args.create_type) == 3:
            if args.code:
                pipeline = PipelinesCode.create_pipeline(pipeline_name=args.name,
                                                     cluster_label=args.cluster_label,
                                                     code=args.code,
                                                     file_path=args.script_location,
                                                     language=args.language,
                                                     user_arguments=args.user_arguments,
                                                     command_line_options=args.command_line_options)
            elif args.script_location:
                pipeline = PipelinesCode.create_pipeline(pipeline_name=args.name,
                                                     cluster_label=args.cluster_label,
                                                     code=args.code,
                                                     file_path=args.script_location,
                                                     language=args.language,
                                                     user_arguments=args.user_arguments,
                                                     command_line_options=args.command_line_options)

        return json.dumps(pipeline)

    @staticmethod
    def update_properties(args):
        """
        Commandline method to update pipeline properties.
        :param args:
        :return:
        """
        params = args.__dict__
        log.debug(params)
        Pipelines.add_property(pipeline_id=args.pipeline_id,
                           cluster_label=args.cluster_label,
                           can_retry=args.can_retry,
                           command_line_options=args.command_line_options)

    @staticmethod
    def update_code(args):
        """
        Commandline method to update code/Jar_Path
        :param args:
        :return:
        """
        if args.jar_path or args.main_class_name:
            response = PipelinesJar.save_code(pipeline_id=args.pipeline_id,
                                          code=args.code,
                                          file_path=args.script_location,
                                          language=args.language,
                                          jar_path=args.jar_path,
                                          user_arguments=args.user_arguments,
                                          main_class_name=args.main_class_name)
        elif args.code or args.script_location:
            response = PipelinesCode.save_code(pipeline_id=args.pipeline_id,
                                           code=args.code,
                                           file_path=args.script_location,
                                           language=args.language,
                                           jar_path=args.jar_path,
                                           user_arguments=args.user_arguments,
                                           main_class_name=args.main_class_name)
        return json.dumps(response, sort_keys=True, indent=4)


class Pipelines(Resource):
    """qds_sdk.Pipelines is the base Qubole Pipelines class."""

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
        """
        Method to list pipeline on the basis of status.
        :param status: Valid values - all, draft, archive, active.
        :return: List of pipeline in json format.
        """
        if status is None or status.lower() == 'all':
            params = {"filter": "draft,archive,active"}
        else:
            params = {"filter": status.lower()}
        conn = Qubole.agent()
        url_path = Pipelines.rest_entity_path
        pipeline_list = conn.get(url_path, params)
        return pipeline_list

    @classmethod
    def create(cls, pipeline_name, create_type, **kwargs):
        """
        Create a pipeline object by issuing a POST
        request to the /pipeline?mode=wizard endpoint
        Note - this creates pipeline in draft mode

        Args:
            pipeline_name: Name to be given.
            create_type: 1->Assisted, 2->Jar, 3->Code
            **kwargs: keyword arguments specific to create type

        Returns:
            response
        """
        conn = Qubole.agent()
        url = Pipelines.rest_entity_path
        if create_type is None:
            raise ParseError("Provide create_type for Pipeline.", None)
        if not kwargs or create_type == 1:
            data = {
                "data": {
                        "attributes": {
                            "name": pipeline_name,
                            "status": "DRAFT",
                            "create_type": create_type
                        },
                        "type": "pipeline"
                    }
            }
            url = url + "?mode=wizard"
        else:
            data = {
                "data": {
                    "type": "pipeline",
                    "attributes": {
                        "name": pipeline_name,
                        "create_type": create_type,
                        "properties": {
                            "cluster_label": kwargs.get('cluster_label'),
                            "can_retry": kwargs.get('can_retry'),
                            "command_line_options": kwargs.get('command_line_options'),
                            "user_arguments": kwargs.get('user_arguments')
                        }
                    },
                    "relationships": {
                        "alerts": {
                            "data": {
                                "type": "pipeline/alerts",
                                "attributes": {
                                    "can_notify": kwargs.get('can_notify'),
                                    "notification_channels": kwargs.get('channel_ids')
                                }
                            }
                        }
                    }
                }
            }
            if create_type == 2:
                data['data']['attributes']['properties']['jar_path'] = \
                    kwargs.get('jar_path')
                data['data']['attributes']['properties']['main_class_name'] = \
                    kwargs.get('main_class_name')
            elif create_type == 3:
                data['data']['attributes']['properties']['code'] = \
                    kwargs.get('code')
                data['data']['attributes']['properties']['language'] = \
                    kwargs.get('language')

        response = conn.post(url, data)
        cls.pipeline_id = Pipelines.get_pipline_id(response)
        cls.pipeline_name = pipeline_name
        return response

    @staticmethod
    def start(pipeline_id):
        """
        Method to start Pipeline
        :param pipeline_id: id of pipeline to be deleted
        :return: response
        """
        conn = Qubole.agent()
        url = Pipelines.rest_entity_path + "/" + pipeline_id + "/start"
        response = conn.put(url)
        pipeline_status = Pipelines.get_status(pipeline_id)
        while pipeline_status == 'waiting':
            log.info("Pipeline is in waiting state....")
            time.sleep(10)
            pipeline_status = response.get(
                'data').get('pipeline_instance_status')
        log.debug("State of pipeline is %s", pipeline_status)
        return response

    @staticmethod
    def add_property(pipeline_id,
                     cluster_label,
                     checkpoint_location=None,
                     output_mode=None,
                     trigger_interval=None,
                     can_retry=True,
                     command_line_options=None):
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
            command_line_options = """--conf spark.driver.extraLibraryPath=/usr/lib/hadoop2/lib/native\n--conf spark.eventLog.compress=true\n--conf spark.eventLog.enabled=true\n--conf spark.sql.streaming.qubole.enableStreamingEvents=true\n--conf spark.qubole.event.enabled=true"""
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
        url = Pipelines.rest_entity_path + "/" + pipeline_id + "/properties"
        response = conn.put(url, data)
        log.debug(response)
        return response

    @classmethod
    def save_code(cls, pipeline_id,
                  code=None,
                  file_path=None,
                  language=None,
                  jar_path=None,
                  main_class_name=None,
                  user_arguments=None):
        """
        :param file_path:
        :param code:
        :param language:
        :param user_arguments:
        :param pipeline_id:
        :param jar_path:
        :param main_class_name:
        :return:
        """
        data = None
        if cls.create_type == 2:
            if jar_path is None or main_class_name is None:
                raise ParseError(
                    "Provide Jar path for BYOJ mode.")
            else:
                cls.jar_path = jar_path
                data = {"data": {
                    "attributes": {"create_type": cls.create_type,
                                   "user_arguments": str(user_arguments),
                                   "jar_path": str(jar_path),
                                   "main_class_name": str(main_class_name)}}}

        elif cls.create_type == 3:
            if code or file_path:
                try:
                    if file_path:
                        with open(file_path, 'r') as f:
                            code = f.read()
                    else:
                        code = code
                except IOError as e:
                    raise ParseError("Unable to open script location or script "
                                     "location and code both are empty. ",
                                     e.message)
                cls.pipeline_code = code
                data = {"data": {
                    "attributes": {"create_type": cls.create_type, "user_arguments": str(user_arguments),
                                   "code": str(code), "language": str(language)}}}

            else:
                raise ParseError(
                    "Provide code or file location for BYOC mode.")

        conn = Qubole.agent()
        url = cls.rest_entity_path + "/" + str(pipeline_id) + "/save_code"
        response = conn.put(url, data)
        log.debug(response)
        return response

    @staticmethod
    def get_health(pipeline_id):
        """
        Get Pipeline Health
        :param pipeline_id:
        :return:
        """
        conn = Qubole.agent()
        url = Pipelines.rest_entity_path + "/" + pipeline_id
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
        url = Pipelines.rest_entity_path + "/" + pipeline_id + "/duplicate"
        log.info("Cloning pipeline with id {}".format(pipeline_id))
        conn = Qubole.agent()
        return conn.post(url)

    @staticmethod
    def pause(pipeline_id):
        """
        Method to pause pipeline
        :param pipeline_id:
        :return:
        """
        url = Pipelines.rest_entity_path + "/" + pipeline_id + "/pause"
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
        url = Pipelines.rest_entity_path + "/" + pipeline_id + "/archive"
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
        url = Pipelines.rest_entity_path + "/" + pipeline_id
        response = conn.get(url)
        log.debug(response)
        return response.get("data").get(
            "attributes").get("pipeline_instance_status")

    @staticmethod
    def delete(pipeline_id):
        """
        Method to delete pipeline
        :param pipeline_id:
        :return:
        """
        conn = Qubole.agent()
        url = Pipelines.rest_entity_path + "/" + pipeline_id + "/delete"
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
        url = Pipelines.rest_entity_path + "/" + pipeline_id
        data = {
            "data": {
                "attributes": {
                    "name": pipeline_name},
                "type": "pipelines"}}
        return conn.put(url, data)

    @staticmethod
    def set_alert(pipeline_id, channel_id):
        """

        :param pipeline_id:
        :param channel_id: List of channel's id
        :return:
        """
        data = {
            "data": {"attributes": {
                "event_type": "error",
                "notification_channels": [channel_id],
                "can_notify": True},
                "type": "pipeline/alerts"
            }
        }
        conn = Qubole.agent()
        url = Pipelines.rest_entity_path + "/" + pipeline_id + "/alerts"
        return conn.put(url, data)

    @staticmethod
    def get_code(pipeline_id):
        """
        Get pipeline code
        :param pipeline_id:
        :return:
        """
        url = Pipelines.rest_entity_path + "/" + pipeline_id
        conn = Qubole.agent()
        reponse = conn.get(url)
        code = reponse.get("meta")["command_details"]["code"]
        return code


class PipelinesCode(Pipelines):
    create_type = 3

    @staticmethod
    def create_pipeline(pipeline_name,
                        cluster_label,
                        code=None,
                        file_path=None,
                        language=None,
                        can_retry=True,
                        channel_id=None,
                        command_line_options=None,
                        user_arguments=None):
        """
        Method to create pipeline in BYOC mode in one go.
        :param file_path:
        :param code:
        :param command_line_options:
        :param user_arguments:
        :param pipeline_name:
        :param cluster_label:
        :param language:
        :param can_retry:
        :param channel_id:
        :return:
        """
        PipelinesCode.create(pipeline_name, PipelinesCode.create_type)
        pipeline_id = PipelinesCode.pipeline_id
        response = PipelinesCode.add_property(pipeline_id, cluster_label,
                                          can_retry=can_retry,
                                          command_line_options=command_line_options)
        log.debug(response)
        response = PipelinesCode.save_code(pipeline_id,
                                       code=code,
                                       file_path=file_path,
                                       language=language,
                                       user_arguments=user_arguments)
        if channel_id:
            response = Pipelines.set_alert(pipeline_id, channel_id)
            log.info(response)
        return response


class PipelinesJar(Pipelines):
    create_type = 2

    @staticmethod
    def create_pipeline(pipeline_name,
                        jar_path,
                        cluster_label,
                        main_class_name,
                        channel_id=None,
                        can_retry=True,
                        command_line_options=None,
                        user_arguments=None):
        """
        Method to create pipeline in BYOJ mode
        :param pipeline_name:
        :param jar_path:
        :param cluster_label:
        :param main_class_name:
        :param channel_id:
        :param can_retry:
        :param command_line_options:
        :param user_arguments:
        :return:
        """
        PipelinesJar.create(pipeline_name, PipelinesJar.create_type)
        pipeline_id = PipelinesJar.pipeline_id
        PipelinesJar.add_property(pipeline_id,
                              cluster_label,
                              can_retry=can_retry,
                              command_line_options=command_line_options)
        PipelinesJar.save_code(pipeline_id,
                           jar_path=jar_path,
                           main_class_name=main_class_name,
                           user_arguments=user_arguments)
        PipelinesJar.jar_path = jar_path
        if channel_id:
            response = Pipelines.set_alert(pipeline_id, channel_id)
            log.info(response)
        return PipelinesJar


class PipelinesAssisted(Pipelines):
    create_type = 1

    @staticmethod
    def add_source():
        """Method to add source."""
        pass

    @staticmethod
    def add_sink():
        """Method to add sink."""
        pass

    @staticmethod
    def create_pipeline():
        """Parent Method to create end to end pipeline."""
        pass

    @staticmethod
    def add_operator():
        """Parent method to add operator"""
        pass

    @staticmethod
    def _select_operator():
        """Method to add select operator."""
        pass

    @staticmethod
    def _filter_operator():
        """Method to add filter operator."""
        pass

    @staticmethod
    def _watermark_operator():
        """Method to add watermark operator"""
        pass

    @staticmethod
    def _window_group_operator():
        """Method to add window group operator"""
        pass

    @staticmethod
    def _source_kafka():
        """Method to as kafka as source."""
        pass

    @staticmethod
    def _source_kinesis():
        """Method to add kinesis as source."""
        pass

    @staticmethod
    def _source_s3():
        """Method to add s3 as source."""
        pass

    @staticmethod
    def _source_google_storage():
        """Method to add google storage as source."""
        pass

    @staticmethod
    def _sink_kafka():
        """Method to add kafka as sink."""
        pass

    @staticmethod
    def _sink_s3():
        """Method to add s3 as sink."""
        pass

    @staticmethod
    def _sink_hive():
        """method to add hive as sink."""
        pass

    @staticmethod
    def _sink_snowflake():
        """Method to add Snowflake as sink"""
        pass

    @staticmethod
    def _sink_google_storage():
        """Method to add google storage as sink"""
        pass

    @staticmethod
    def _sink_BigQuery():
        """Method to add BigQuery as sink."""
        pass

    @staticmethod
    def add_registry():
        """Method to add registry."""
        pass

    @staticmethod
    def switch_from_assisted():
        """Method to switch to Assisted from BYOC or BYOJ mode."""
        pass
