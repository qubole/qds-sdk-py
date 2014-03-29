"""
The template module allows template and macro processing on QDS
"""

from qubole import Qubole
from resource import Resource
from exception import ParseError
from commands import Command
from qds_sdk.util import GentleOptionParser
from qds_sdk.util import OptionParsingError
from qds_sdk.util import OptionParsingExit

import boto

import time
import logging
import sys
import re
import pipes

import json

log = logging.getLogger("qds_templates")

# Pattern matcher for s3 path
_URI_RE = re.compile(r's3://([^/]+)/?(.*)')


class Template(Resource):

    """
    qds_sdk.Template is the Qubole template class.
    """

    rest_entity_path = "command_templates"

    @classmethod
    def create(cls, **kwargs):
        """
        Create a template object by issuing a POST request to the /command_templates endpoint
        Note - this does not wait for the command to complete

        Args:
            `\**kwargs` - keyword arguments specific to command type

        Returns:
            Template object
        """

        conn = Qubole.agent()
        if kwargs.get('command_type') is None:
            kwargs['command_type'] = cls.__name__

        return Template(conn.post(cls.rest_entity_path, data=kwargs))

    @classmethod
    def run(cls, **kwargs):
        """
        Run a saved command_template as a command

        Args:
            `\**kwargs` - keyword arguments specific to command type

        Returns:
            Command object
        """
        tmplt = cls.create(**kwargs)
        conn = Qubole.agent()
        template_run_path = "/".join(["command_templates", str(tmplt.id), "run"])
        cmd = conn.post(template_run_path, data=kwargs['macros'])
        while not cmd.is_done(cmd.status):
            time.sleep(Qubole.poll_interval)
            cmd = cls.find(cmd.id)
        return cmd


class TemplateCommand(Template):

    usage = ("templatecmd -n <name> -t <command_type> <submit|run> [options]")

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("-n", "--name", dest="name", help="name of this template")
    optparser.add_option("-t", "--type", dest="command_type", help="command_type <hive|hadoop|shell|pig etc>")
    optparser.add_option("-c", "--command", dest="command", help="templateized command query string")

    optparser.add_option("-f", "--script_location", dest="script_location",
                         help="Path where templatized query to run is stored. Can be S3 URI or local file path")

    optparser.add_option("--macros", dest="macros",
                         help="expressions to expand macros used in query")

    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a template

        Args:
            `args` - sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """

        (options, args) = cls.optparser.parse_args(args)
        if options.name is None:
          raise ParseError("Name is mandatory for the query",
              cls.optparser.format_help())

        if options.command_type is None:
          raise ParseError("Command Type must be specified",
              cls.optparser.format_help())
        else:
          options.command_type = options.command_type[0].upper() + options.command_type[1:] + "Command"

        try:
            if options.command is None and options.script_location is None:
                raise ParseError("One of query or script location"
                                 " must be specified",
                                 cls.optparser.format_help())
        except OptionParsingError as e:
            raise ParseError(e.msg, cls.optparser.format_help())
        except OptionParsingExit as e:
            return None

        if options.script_location is not None:
            if options.command is not None:
                raise ParseError(
                    "Both query and script_location cannot be specified",
                    cls.optparser.format_help())

            if ((options.script_location.find("s3://") != 0) and
                (options.script_location.find("s3n://") != 0)):

                # script location is local file

                try:
                    q = open(options.script_location).read()
                except:
                    raise ParseError("Unable to open script location: %s" %
                                     options.script_location,
                                     cls.optparser.format_help())
                options.script_location = None
                options.command = json.dumps({ "query": q })


        if options.macros is not None:
            options.macros = json.loads(options.macros)
        return vars(options)
