"""
The commands module contains the base definition for
a generic Qubole command and the implementation of all
the specific commands
"""

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from qds_sdk.exception import ParseError
from qds_sdk.account import Account
from qds_sdk.util import GentleOptionParser
from qds_sdk.util import OptionParsingError
from qds_sdk.util import OptionParsingExit
from optparse import SUPPRESS_HELP
import boto

import time
import logging
import sys
import re
import pipes
import os
import json

log = logging.getLogger("qds_commands")

# Pattern matcher for s3 path
_URI_RE = re.compile(r's3://([^/]+)/?(.*)')


class Command(Resource):

    """
    qds_sdk.Command is the base Qubole command class. Different types of Qubole
    commands can subclass this.
    """

    """all commands use the /commands endpoint"""
    rest_entity_path = "commands"

    @staticmethod
    def is_done(status):
        """
        Does the status represent a completed command
        Args:
            `status`: a status string

        Returns:
            True/False
        """
        return status == "cancelled" or status == "done" or status == "error"

    @staticmethod
    def is_success(status):
        return status == "done"

    @classmethod
    def create(cls, **kwargs):
        """
        Create a command object by issuing a POST request to the /command endpoint
        Note - this does not wait for the command to complete

        Args:
            `**kwargs`: keyword arguments specific to command type

        Returns:
            Command object
        """

        conn = Qubole.agent()
        if kwargs.get('command_type') is None:
            kwargs['command_type'] = cls.__name__
        if kwargs.get('tags') is not None:
            kwargs['tags'] = kwargs['tags'].split(',')

        return cls(conn.post(cls.rest_entity_path, data=kwargs))

    @classmethod
    def run(cls, **kwargs):
        """
        Create a command object by issuing a POST request to the /command endpoint
        Waits until the command is complete. Repeatedly polls to check status

        Args:
            `**kwargs`: keyword arguments specific to command type

        Returns:
            Command object
        """
        cmd = cls.create(**kwargs)
        while not Command.is_done(cmd.status):
            time.sleep(Qubole.poll_interval)
            cmd = cls.find(cmd.id)

        return cmd

    @classmethod
    def cancel_id(cls, id):
        """
        Cancels command denoted by this id

        Args:
            `id`: command id
        """
        conn = Qubole.agent()
        data = {"status": "kill"}
        return conn.put(cls.element_path(id), data)

    def cancel(self):
        """
        Cancels command represented by this object
        """
        self.__class__.cancel_id(self.id)

    @classmethod
    def get_log_id(cls, id):
        """
        Fetches log for the command represented by this id

        Args:
            `id`: command id
        """
        conn = Qubole.agent()
        r = conn.get_raw(cls.element_path(id) + "/logs")
        return r.text

    def get_log(self):
        """
        Fetches log for the command represented by this object

        Returns:
            The log as a string
        """
        log_path = self.meta_data['logs_resource']
        conn = Qubole.agent()
        r = conn.get_raw(log_path)
        return r.text


    @classmethod
    def get_jobs_id(cls, id):
        """
        Fetches information about the hadoop jobs which were started by this
        command id. This information is only available for commands which have
        completed (i.e. Status = 'done', 'cancelled' or 'error'.) Also, the
        cluster which ran this command should be running for this information
        to be available. Otherwise only the URL and job_id is shown.

        Args:
            `id`: command id
        """
        conn = Qubole.agent()
        r = conn.get_raw(cls.element_path(id) + "/jobs")
        return r.text


    def get_results(self, fp=sys.stdout, inline=True, delim=None, fetch=True):
        """
        Fetches the result for the command represented by this object

        get_results will retrieve results of the command and write to stdout by default.
        Optionally one can write to a filestream specified in `fp`. The `inline` argument
        decides whether the result can be returned as a CRLF separated string. In cases where
        the results are greater than 20MB, get_results will attempt to read from s3 and write
        to fp. The retrieval of results from s3 can be turned off by the `fetch` argument

        Args:
            `fp`: a file object to write the results to directly
            `inline`: whether or not results are returned inline as CRLF separated string
            `fetch`: True to fetch the result even if it is greater than 20MB, False to
                     only get the result location on s3
        """
        result_path = self.meta_data['results_resource']

        conn = Qubole.agent()

        r = conn.get(result_path, {'inline': inline})
        if r.get('inline'):
            if sys.version_info < (3, 0, 0):
                fp.write(r['results'].encode('utf8'))
            else:
                import io
                if isinstance(fp, io.TextIOBase):
                    fp.buffer.write(r['results'].encode('utf8'))
                elif isinstance(fp, io.BufferedIOBase) or isinstance(fp, io.RawIOBase):
                    fp.write(r['results'].encode('utf8'))
                else:
                    # Can this happen? Don't know what's the right thing to do in this case.
                    pass
        else:
            if fetch:
                acc = Account.find()
                boto_conn = boto.connect_s3(aws_access_key_id=acc.storage_access_key,
                                            aws_secret_access_key=acc.storage_secret_key)

                log.info("Starting download from result locations: [%s]" % ",".join(r['result_location']))
                #fetch latest value of num_result_dir
                num_result_dir = Command.find(self.id).num_result_dir
                for s3_path in r['result_location']:
                    # In Python 3, in this case, `fp` should always be binary mode.
                    _download_to_local(boto_conn, s3_path, fp, num_result_dir, delim=delim)
            else:
                fp.write(",".join(r['result_location']))



class HiveCommand(Command):

    usage = ("hivecmd <submit|run> [options]")

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("-q", "--query", dest="query", help="query string")

    optparser.add_option("-f", "--script_location", dest="script_location",
                         help="Path where hive query to run is stored. Can be S3 URI or local file path")

    optparser.add_option("--macros", dest="macros",
                         help="expressions to expand macros used in query")

    optparser.add_option("--tags", dest="tags",
                         help="comma-separated list of tags to be associated with the query ( e.g., tag1 tag1,tag2 )")

    optparser.add_option("--sample_size", dest="sample_size",
                         help="size of sample in bytes on which to run query")

    optparser.add_option("--cluster-label", dest="label",
                         help="the label of the cluster to run the command on")

    optparser.add_option("--notify", action="store_true", dest="can_notify",
                         default=False, help="sends an email on command completion")

    optparser.add_option("--name", dest="name",
                         help="Assign a name to this query")

    optparser.add_option("--print-logs", action="store_true", dest="print_logs",
                         default=False, help="Fetch logs and print them to stderr.")

    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a command

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """

        try:
            (options, args) = cls.optparser.parse_args(args)
            if options.query is None and options.script_location is None:
                raise ParseError("One of query or script location"
                                 " must be specified",
                                 cls.optparser.format_help())
        except OptionParsingError as e:
            raise ParseError(e.msg, cls.optparser.format_help())
        except OptionParsingExit as e:
            return None

        if options.script_location is not None:
            if options.query is not None:
                raise ParseError(
                    "Both query and script_location cannot be specified",
                    cls.optparser.format_help())

            if ((options.script_location.find("s3://") != 0) and
                (options.script_location.find("s3n://") != 0)):

                # script location is local file

                try:
                    q = open(options.script_location).read()
                except IOError as e:
                    raise ParseError("Unable to open script location: %s" %
                                     str(e),
                                     cls.optparser.format_help())
                options.script_location = None
                options.query = q

        if options.macros is not None:
            options.macros = json.loads(options.macros)
        v = vars(options)
        v["command_type"] = "HiveCommand"
        return v

class SparkCommand(Command):

    usage = ("sparkcmd <submit|run> [options]")
    allowedlanglist = ["python", "scala"]

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("--program", dest="program",help=SUPPRESS_HELP)

    optparser.add_option("--cmdline", dest="cmdline", help="command line for Spark")

    optparser.add_option("-f", "--script_location", dest="script_location",
                         help="Path where spark program to run is stored. Has to be a local file path")

    optparser.add_option("--macros", dest="macros",
                         help="expressions to expand macros used in query")

    optparser.add_option("--tags", dest="tags",
                         help="comma-separated list of tags to be associated with the query ( e.g., tag1 tag1,tag2 )")

    optparser.add_option("--cluster-label", dest="label", help="the label of the cluster to run the command on")

    optparser.add_option("--language", dest="language", choices = allowedlanglist, help=SUPPRESS_HELP)

    optparser.add_option("--notify", action="store_true", dest="can_notify", default=False, help="sends an email on command completion")

    optparser.add_option("--name", dest="name", help="Assign a name to this query")

    optparser.add_option("--arguments", dest = "arguments", help = "Spark Submit Command Line Options")

    optparser.add_option("--user_program_arguments", dest = "user_program_arguments", help = "Arguments for User Program")

    optparser.add_option("--print-logs", action="store_true", dest="print_logs",
                         default=False, help="Fetch logs and print them to stderr.")
    @classmethod
    def validate_program(cls, options):
        bool_program = options.program is not None
        bool_other_options = options.script_location is not None or options.cmdline is not None

        # if both are false then no option is specified ==> raise ParseError
        # if both are true then atleast two option specified ==> raise ParseError
        if bool_program == bool_other_options:
            raise ParseError("Exactly One of script location or program or cmdline should be specified", cls.optparser.format_help())
        if bool_program:
            if options.language is None:
                raise ParseError("Unspecified language for Program", cls.optparser.format_help())

    @classmethod
    def validate_cmdline(cls, options):
        bool_cmdline = options.cmdline is not None
        bool_other_options = options.script_location is not None or options.program is not None

        # if both are false then no option is specified ==> raise ParseError
        # if both are true then atleast two option specified ==> raise ParseError
        if bool_cmdline == bool_other_options:
            raise ParseError("Exactly One of script location or program or cmdline should be specified", cls.optparser.format_help())
        if bool_cmdline:
            if options.language is not None:
                raise ParseError("Language cannot be specified with the commandline option", cls.optparser.format_help())

    @classmethod
    def validate_script_location(cls, options):
        bool_script_location = options.script_location is not None
        bool_other_options = options.program is not None or options.cmdline is not None

        # if both are false then no option is specified ==> raise ParseError
        # if both are true then atleast two option specified ==> raise ParseError
        if bool_script_location == bool_other_options:
            raise ParseError("Exactly One of script location or program or cmdline should be specified", cls.optparser.format_help())

        if bool_script_location:
            if options.language is not None:
                raise ParseError("Both script location and language cannot be specified together", cls.optparser.format_help())
            # for now, aws script_location is not supported and throws an error
            if ((options.script_location.find("s3://") != 0) and
                (options.script_location.find("s3n://") != 0)):

                # script location is local file so set the program as the text from the file

                try:
                    q = open(options.script_location).read()
                except IOError as e:
                    raise ParseError("Unable to open script location: %s" %
                                     str(e),
                                     cls.optparser.format_help())


                fileName, fileExtension = os.path.splitext(options.script_location)
                # getting the language of the program from the file extension
                if fileExtension == ".py":
                    options.language = "python"
                elif fileExtension == ".scala":
                    options.language = "scala"
                else:
                    raise ParseError("Invalid program type, Please choose one from python or scala %s" %str(fileExtension),
                                     cls.optparser.format_help())
            else:
                raise ParseError("Invalid location, Please choose a local file location",
                                 cls.optparser.format_help())

            options.script_location = None
            options.program = q

    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a command

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """
        try:
            (options, args) = cls.optparser.parse_args(args)
        except OptionParsingError as e:
            raise ParseError(e.msg, cls.optparser.format_help())
        except OptionParsingExit as e:
            return None

        SparkCommand.validate_program(options)
        SparkCommand.validate_script_location(options)
        SparkCommand.validate_cmdline(options)

        if options.macros is not None:
            options.macros = json.loads(options.macros)

        v = vars(options)
        v["command_type"] = "SparkCommand"
        return v



class PrestoCommand(Command):

    usage = ("prestocmd <submit|run> [options]")

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("-q", "--query", dest="query", help="query string")

    optparser.add_option("-f", "--script_location", dest="script_location",
                         help="Path where presto query to run is stored. Can be S3 URI or local file path")

    optparser.add_option("--macros", dest="macros",
                         help="expressions to expand macros used in query")

    optparser.add_option("--tags", dest="tags",
                         help="comma-separated list of tags to be associated with the query ( e.g., tag1 tag1,tag2 )")

    optparser.add_option("--cluster-label", dest="label",
                         help="the label of the cluster to run the command on")

    optparser.add_option("--notify", action="store_true", dest="can_notify",
                         default=False, help="sends an email on command completion")

    optparser.add_option("--name", dest="name",
                         help="Assign a name to this query")

    optparser.add_option("--print-logs", action="store_true", dest="print_logs",
                         default=False, help="Fetch logs and print them to stderr.")

    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a command

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """

        try:
            (options, args) = cls.optparser.parse_args(args)
            if options.query is None and options.script_location is None:
                raise ParseError("One of query or script location"
                                 " must be specified",
                                 cls.optparser.format_help())
        except OptionParsingError as e:
            raise ParseError(e.msg, cls.optparser.format_help())
        except OptionParsingExit as e:
            return None

        if options.script_location is not None:
            if options.query is not None:
                raise ParseError(
                    "Both query and script_location cannot be specified",
                    cls.optparser.format_help())

            if ((options.script_location.find("s3://") != 0) and
                (options.script_location.find("s3n://") != 0)):

                # script location is local file
                try:
                    q = open(options.script_location).read()
                except IOError as e:
                    raise ParseError("Unable to open script location: %s" %
                                     str(e),
                                     cls.optparser.format_help())
                options.script_location = None
                options.query = q

        if options.macros is not None:
            options.macros = json.loads(options.macros)
        v = vars(options)
        v["command_type"] = "PrestoCommand"
        return v


class HadoopCommand(Command):
    subcmdlist = ["jar", "s3distcp", "streaming"]
    usage = "hadoopcmd <submit|run> [options] <%s> <arg1> [arg2] ..." % "|".join(subcmdlist)

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("--cluster-label", dest="label",
                         help="the label of the cluster to run the command on")

    optparser.add_option("--notify", action="store_true", dest="can_notify",
                         default=False, help="sends an email on command completion")

    optparser.add_option("--name", dest="name",
                         help="Assign a name to this command")

    optparser.add_option("--tags", dest="tags",
                         help="comma-separated list of tags to be associated with the query ( e.g., tag1 tag1,tag2 )")

    optparser.add_option("--print-logs", action="store_true", dest="print_logs",
                         default=False, help="Fetch logs and print them to stderr.")

    optparser.disable_interspersed_args()

    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a command

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """
        parsed = {}

        try:
            (options, args) = cls.optparser.parse_args(args)
        except OptionParsingError as e:
            raise ParseError(e.msg, cls.optparser.format_help())
        except OptionParsingExit as e:
            return None

        parsed['label'] = options.label
        parsed['can_notify'] = options.can_notify
        parsed['name'] = options.name
        parsed['tags'] = options.tags
        parsed["command_type"] = "HadoopCommand"
        parsed['print_logs'] = options.print_logs

        if len(args) < 2:
            raise ParseError("Need at least two arguments", cls.usage)

        subcmd = args.pop(0)
        if subcmd not in cls.subcmdlist:
            raise ParseError("First argument must be one of <%s>" %
                             "|".join(cls.subcmdlist))

        parsed["sub_command"] = subcmd
        parsed["sub_command_args"] = " ".join("'" + a + "'" for a in args)

        return parsed


class ShellCommand(Command):
    usage = ("shellcmd <submit|run> [options] [arg1] [arg2] ...")

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("-s", "--script", dest="inline", help="inline script that can be executed by bash")

    optparser.add_option("-f", "--script_location", dest="script_location",
                         help="Path where bash script to run is stored. Can be S3 URI or local file path")

    optparser.add_option("-i", "--files", dest="files",
                         help="List of files [optional] Format : file1,file2 (files in s3 bucket) These files will be copied to the working directory where the command is executed")

    optparser.add_option("-a", "--archives", dest="archives",
                         help="List of archives [optional] Format : archive1,archive2 (archives in s3 bucket) These are unarchived in the working directory where the command is executed")

    optparser.add_option("--cluster-label", dest="label",
                         help="the label of the cluster to run the command on")

    optparser.add_option("--notify", action="store_true", dest="can_notify",
                         default=False, help="sends an email on command completion")

    optparser.add_option("--tags", dest="tags",
                         help="comma-separated list of tags to be associated with the query ( e.g., tag1 tag1,tag2 )")

    optparser.add_option("--name", dest="name",
                         help="Assign a name to this command")

    optparser.add_option("--print-logs", action="store_true", dest="print_logs",
                         default=False, help="Fetch logs and print them to stderr.")

    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a command

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """

        try:
            (options, args) = cls.optparser.parse_args(args)
            if options.inline is None and options.script_location is None:
                raise ParseError("One of script or it's location"
                                 " must be specified",
                                 cls.optparser.format_help())
        except OptionParsingError as e:
            raise ParseError(e.msg, cls.optparser.format_help())
        except OptionParsingExit as e:
            return None

        if options.script_location is not None:
            if options.inline is not None:
                raise ParseError(
                    "Both script and script_location cannot be specified",
                    cls.optparser.format_help())

            if ((options.script_location.find("s3://") != 0) and
                (options.script_location.find("s3n://") != 0)):

                # script location is local file

                try:
                    s = open(options.script_location).read()
                except IOError as e:
                    raise ParseError("Unable to open script location: %s" %
                                     str(e),
                                     cls.optparser.format_help())
                options.script_location = None
                options.inline = s

            if (args is not None) and (len(args) > 0):
                if options.inline is not None:
                    raise ParseError(
                        "Extra arguments can only be "
                        "supplied with a script_location in S3 right now",
                        cls.optparser.format_help())

                setattr(options, 'parameters',
                        " ".join([pipes.quote(a) for a in args]))

        else:
            if (args is not None) and (len(args) > 0):
                raise ParseError(
                    "Extra arguments can only be supplied with a script_location",
                    cls.optparser.format_help())

        v = vars(options)
        v["command_type"] = "ShellCommand"
        return v


class PigCommand(Command):
    usage = ("pigcmd <submit|run> [options] [key1=value1] [key2=value2] ...")

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("-s", "--script", dest="latin_statements",
                         help="latin statements that has to be executed")

    optparser.add_option("-f", "--script_location", dest="script_location",
                         help="Path where bash script to run is stored. Can be S3 URI or local file path")

    optparser.add_option("--cluster-label", dest="label",
                         help="the label of the cluster to run the command on")

    optparser.add_option("--notify", action="store_true", dest="can_notify",
                         default=False, help="sends an email on command completion")

    optparser.add_option("--tags", dest="tags",
                         help="comma-separated list of tags to be associated with the query ( e.g., tag1 tag1,tag2 )")

    optparser.add_option("--name", dest="name",
                         help="Assign a name to this command")

    optparser.add_option("--print-logs", action="store_true", dest="print_logs",
                         default=False, help="Fetch logs and print them to stderr.")

    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a command

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """

        try:
            (options, args) = cls.optparser.parse_args(args)
            if options.latin_statements is None and options.script_location is None:
                raise ParseError("One of script or it's location"
                                 " must be specified",
                                 cls.optparser.format_help())
        except OptionParsingError as e:
            raise ParseError(e.msg, cls.optparser.format_help())
        except OptionParsingExit as e:
            return None

        if options.script_location is not None:
            if options.latin_statements is not None:
                raise ParseError(
                    "Both script and script_location cannot be specified",
                    cls.optparser.format_help())

            if ((options.script_location.find("s3://") != 0) and
                (options.script_location.find("s3n://") != 0)):

                # script location is local file

                try:
                    s = open(options.script_location).read()
                except IOError as e:
                    raise ParseError("Unable to open script location: %s" %
                                     str(e),
                                     cls.optparser.format_help())
                options.script_location = None
                options.latin_statements = s

            if (args is not None) and (len(args) > 0):
                if options.latin_statements is not None:
                    raise ParseError(
                        "Extra arguments can only be "
                        "supplied with a script_location in S3 right now",
                        cls.optparser.format_help())

                p = {}
                for a in args:
                    kv = a.split('=')
                    if len(kv) != 2:
                        raise ParseError("Arguments to pig script must be of this format k1=v1 k2=v2 k3=v3...")
                    p[kv[0]] = kv[1]
                setattr(options, 'parameters', p)

        else:
            if (args is not None) and (len(args) > 0):
                raise ParseError(
                    "Extra arguments can only be supplied with a script_location",
                    cls.optparser.format_help())

        v = vars(options)
        v["command_type"] = "PigCommand"
        return v


class DbExportCommand(Command):
    usage = ("dbexportcmd <submit|run> [options]")

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("-m", "--mode", dest="mode",
                         help="Can be 1 for Hive export or 2 for HDFS/S3 export")
    optparser.add_option("--hive_table", dest="hive_table",
                         help="Mode 1: Name of the Hive Table from which data will be exported")
    optparser.add_option("--partition_spec", dest="partition_spec",
                         help="Mode 1: (optional) Partition specification for Hive table")
    optparser.add_option("--dbtap_id", dest="dbtap_id",
                         help="Modes 1 and 2: DbTap Id of the target database in Qubole")
    optparser.add_option("--db_table", dest="db_table",
                         help="Modes 1 and 2: Table to export to in the target database")
    optparser.add_option("--db_update_mode", dest="db_update_mode",
                         help="Modes 1 and 2: (optional) can be 'allowinsert' or "
                              "'updateonly'. If updateonly is "
                              "specified - only existing rows are updated. If allowinsert "
                              "is specified - then existing rows are updated and non existing "
                              "rows are inserted. If this option is not specified - then the "
                              "given the data will be appended to the table")
    optparser.add_option("--db_update_keys", dest="db_update_keys",
                         help="Modes 1 and 2: Columns used to determine the uniqueness of rows for "
                              "'updateonly' mode")
    optparser.add_option("--export_dir", dest="export_dir",
                         help="Mode 2: HDFS/S3 location from which data will be exported")
    optparser.add_option("--fields_terminated_by", dest="fields_terminated_by",
                         help="Mode 2: Hex of the char used as column separator "
                              "in the dataset, for eg. \0x20 for space")

    optparser.add_option("--notify", action="store_true", dest="can_notify",
                         default=False, help="sends an email on command completion")

    optparser.add_option("--tags", dest="tags",
                         help="comma-separated list of tags to be associated with the query ( e.g., tag1 tag1,tag2 )")

    optparser.add_option("--name", dest="name",
                         help="Assign a name to this command")

    optparser.add_option("--print-logs", action="store_true", dest="print_logs",
                         default=False, help="Fetch logs and print them to stderr.")

    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a command

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """

        try:
            (options, args) = cls.optparser.parse_args(args)
            if options.mode not in ["1", "2"]:
                raise ParseError("mode must be either '1' or '2'",
                                 cls.optparser.format_help())

            if (options.dbtap_id is None) or (options.db_table is None):
                raise ParseError("dbtap_id and db_table are required",
                                 cls.optparser.format_help())

            if options.mode is "1":
                if options.hive_table is None:
                    raise ParseError("hive_table is required for mode 1",
                                     cls.optparser.format_help())
            elif options.export_dir is None:    # mode 2
                raise ParseError("export_dir is required for mode 2",
                                 cls.optparser.format_help())

            if options.db_update_mode is not None:
                if options.db_update_mode not in ["allowinsert", "updateonly"]:
                    raise ParseError("db_update_mode should either be left blank for append "
                                     "mode or be 'updateonly' or 'allowinsert'",
                                     cls.optparser.format_help())
                if options.db_update_mode is "updateonly":
                    if options.db_update_keys is None:
                        raise ParseError("db_update_keys is required when db_update_mode "
                                         "is 'updateonly'",
                                         cls.optparser.format_help())
                elif options.db_update_keys is not None:
                    raise ParseError("db_update_keys is used only when db_update_mode "
                                     "is 'updateonly'",
                                     cls.optparser.format_help())

        except OptionParsingError as e:
            raise ParseError(e.msg, cls.optparser.format_help())
        except OptionParsingExit as e:
            return None

        v = vars(options)
        v["command_type"] = "DbExportCommand"
        return v


class DbexportCommand(DbExportCommand):
    pass


class DbImportCommand(Command):
    usage = "dbimportcmd <submit|run> [options]"

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("-m", "--mode", dest="mode",
                         help="Can be 1 for Hive export or 2 for HDFS/S3 export")
    optparser.add_option("--hive_table", dest="hive_table",
                         help="Mode 1: Name of the Hive Table from which data will be exported")
    optparser.add_option("--dbtap_id", dest="dbtap_id",
                         help="Modes 1 and 2: DbTap Id of the target database in Qubole")
    optparser.add_option("--db_table", dest="db_table",
                         help="Modes 1 and 2: Table to export to in the target database")
    optparser.add_option("--where_clause", dest="db_where",
                         help="Mode 1: where clause to be applied to the table before extracting rows to be imported")
    optparser.add_option("--parallelism", dest="db_parallelism",
                         help="Mode 1 and 2: Number of parallel threads to use for extracting data")

    optparser.add_option("--extract_query", dest="db_extract_query",
                         help="Modes 2: SQL query to be applied at the source database for extracting data. "
                              "$CONDITIONS must be part of the where clause")
    optparser.add_option("--boundary_query", dest="db_boundary_query",
                         help="Mode 2: query to be used get range of rowids to be extracted")
    optparser.add_option("--split_column", dest="db_split_column",
                         help="column used as rowid to split data into range")

    optparser.add_option("--notify", action="store_true", dest="can_notify",
                         default=False, help="sends an email on command completion")

    optparser.add_option("--tags", dest="tags",
                         help="comma-separated list of tags to be associated with the query ( e.g., tag1 tag1,tag2 )")

    optparser.add_option("--name", dest="name",
                         help="Assign a name to this command")

    optparser.add_option("--print-logs", action="store_true", dest="print_logs",
                         default=False, help="Fetch logs and print them to stderr.")

    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a command

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """

        try:
            (options, args) = cls.optparser.parse_args(args)
            if options.mode not in ["1", "2"]:
                raise ParseError("mode must be either '1' or '2'",
                                 cls.optparser.format_help())

            if (options.dbtap_id is None) or (options.db_table is None):
                raise ParseError("dbtap_id and db_table are required",
                                 cls.optparser.format_help())

            # TODO: Semantic checks for parameters in mode 1 and 2

        except OptionParsingError as e:
            raise ParseError(e.msg, cls.optparser.format_help())
        except OptionParsingExit as e:
            return None

        v = vars(options)
        v["command_type"] = "DbImportCommand"
        return v


class CompositeCommand(Command):
    @classmethod
    def compose(cls, sub_commands, macros=None, cluster_label=None, notify=False, name=None, tags=None):
        """
        Args:
            `sub_commands`: list of sub-command dicts

        Returns:
            Dictionary that can be used in create method

        Example Usage:
            cmd1 = HiveCommand.parse(['--query', "show tables"])
            cmd2 = PigCommand.parse(['--script_location', "s3://paid-qubole/PigAPIDemo/scripts/script1-hadoop-s3-small.pig"])
            composite = CompositeCommand.compose([cmd1, cmd2])
            cmd = CompositeCommand.run(**composite)
        """
        if macros is not None:
            macros = json.loads(macros)
        return {
                "sub_commands": sub_commands,
                "command_type": "CompositeCommand",
                "macros": macros,
                "label": cluster_label,
                "tags": tags,
                "can_notify": notify,
                "name": name
               }


class DbTapQueryCommand(Command):
    usage = "dbtapquerycmd <submit|run> [options]"

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("--db_tap_id", dest="db_tap_id",
                         help="dbTap Id of the target database in Qubole")
    optparser.add_option("-q", "--query", dest="query", help="query string")
    optparser.add_option("--notify", action="store_true", dest="can_notify",
                         default=False, help="sends an email on command completion")
    optparser.add_option("--macros", dest="macros",
                         help="expressions to expand macros used in query")

    optparser.add_option("--tags", dest="tags",
                         help="comma-separated list of tags to be associated with the query ( e.g., tag1 tag1,tag2 )")
    optparser.add_option("--name", dest="name",
                         help="Assign a name to this command")

    optparser.add_option("--print-logs", action="store_true", dest="print_logs",
                         default=False, help="Fetch logs and print them to stderr.")

    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a command

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """

        try:
            (options, args) = cls.optparser.parse_args(args)
            if (options.db_tap_id is None):
                raise ParseError("db_tap_id is required",
                                 cls.optparser.format_help())
            if (options.query is None):
                raise ParseError("query is required",
                                 cls.optparser.format_help())

        except OptionParsingError as e:
            raise ParseError(e.msg, cls.optparser.format_help())
        except OptionParsingExit as e:
            return None

        if options.macros is not None:
            options.macros = json.loads(options.macros)
        v = vars(options)
        v["command_type"] = "DbTapQueryCommand"
        return v

def _read_iteratively(key_instance, fp, delim):
    key_instance.open_read()
    while True:
        try:
            # Default buffer size is 8192 bytes
            data = next(key_instance)
            fp.write(str(data).replace(chr(1), delim))
        except StopIteration:
            # Stream closes itself when the exception is raised
            return


def _download_to_local(boto_conn, s3_path, fp, num_result_dir, delim=None):
    '''
    Downloads the contents of all objects in s3_path into fp

    Args:
        `boto_conn`: S3 connection object

        `s3_path`: S3 path to be downloaded

        `fp`: The file object where data is to be downloaded
    '''
    #Progress bar to display download progress
    def _callback(downloaded, total):
        '''
        Call function for upload.

        `downloaded`: File size already downloaded (int)

        `total`: Total file size to be downloaded (int)
        '''
        if (total is 0) or (downloaded == total):
            return
        progress = downloaded*100/total
        sys.stderr.write('\r[{0}] {1}%'.format('#'*progress, progress))
        sys.stderr.flush()

    def _is_complete_data_available(bucket_paths, num_result_dir):
        if num_result_dir == -1:
            return True
        unique_paths = set()
        files = {}
        for one_path in bucket_paths:
            name = one_path.name.replace(key_prefix, "", 1)
            if name.startswith('_tmp.'):
                continue
            path = name.split("/")
            dir = path[0].replace("_$folder$", "", 1)
            unique_paths.add(dir)
            if len(path) > 1:
                file = int(path[1])
                if dir not in files:
                    files[dir] = []
                files[dir].append(file)
        if len(unique_paths) < num_result_dir:
            return False
        for k in files:
            v = files.get(k)
            if len(v) > 0 and max(v) + 1 > len(v):
                return False
        return True

    m = _URI_RE.match(s3_path)
    bucket_name = m.group(1)
    bucket = boto_conn.get_bucket(bucket_name)
    retries = 6
    if s3_path.endswith('/') is False:
        #It is a file
        key_name = m.group(2)
        key_instance = bucket.get_key(key_name)
        while key_instance is None and retries > 0:
            retries = retries - 1
            log.info("Results file is not available on s3. Retry: " + str(6-retries))
            time.sleep(10)
            key_instance = bucket.get_key(key_name)
        if key_instance is None:
            raise Exception("Results file not available on s3 yet. This can be because of s3 eventual consistency issues.")
        log.info("Downloading file from %s" % s3_path)
        if delim is None:
            key_instance.get_contents_to_file(fp)  # cb=_callback
        else:
            # Get contents as string. Replace parameters and write to file.
            _read_iteratively(key_instance, fp, delim=delim)

    else:
        #It is a folder
        key_prefix = m.group(2)
        bucket_paths = bucket.list(key_prefix)
        complete_data_available = _is_complete_data_available(bucket_paths, num_result_dir)
        while complete_data_available is False and retries > 0:
            retries = retries - 1
            log.info("Results dir is not available on s3. Retry: " + str(6-retries))
            time.sleep(10)
            complete_data_available = _is_complete_data_available(bucket_paths, num_result_dir)
        if complete_data_available is False:
            raise Exception("Results file not available on s3 yet. This can be because of s3 eventual consistency issues.")

        for one_path in bucket_paths:
            name = one_path.name

            # Eliminate _tmp_ files which ends with $folder$
            if name.endswith('$folder$'):
                continue

            log.info("Downloading file from %s" % name)
            if delim is None:
                one_path.get_contents_to_file(fp)  # cb=_callback
            else:
                _read_iteratively(one_path, fp, delim=delim)
