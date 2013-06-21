""" 
The commands module contains the base definition for
a generic Qubole command and the implementation of all 
the specific commands
"""

from qubole import Qubole
from resource import Resource
from exception import ParseError
from qds_sdk.util import GentleOptionParser
from qds_sdk.util import OptionParsingError
from qds_sdk.util import OptionParsingExit

import time
import logging
import os
import sys

log = logging.getLogger("qds_commands")

class Command(Resource):

    """
    qds_sdk.Command is the base Qubole command class. Different types of Qubole
    commands can subclass this.
    """

    """ all commands use the /commands endpoint"""
    rest_entity_path="commands"

    @staticmethod
    def is_done(status):
        """
        Does the status represent a completed command
        Args:
            ``status``: a status string

        Returns:
            True/False
        """
        return (status == "cancelled" or status == "done" or status == "error")
    

    @classmethod
    def create(cls, **kwargs):
        """
        Create a command object by issuing a POST request to the /command endpoint
        Note - this does not wait for the command to complete

        Args:
            `\**kwargs` - keyword arguments specific to command type

        Returns:
            Command object
        """

        conn=Qubole.agent()
        if kwargs.get('command_type') is None:
            kwargs['command_type'] = cls.__name__
        return cls(conn.post(cls.rest_entity_path, data=kwargs))


    @classmethod
    def run(cls, **kwargs):
        """
        Create a command object by issuing a POST request to the /command endpoint
        Waits until the command is complete. Repeatedly polls to check status

        Args:
            `\**kwargs` - keyword arguments specific to command type

        Returns:
            Command object
        """
        cmd = cls.create(**kwargs)
        self.run_until_completion(cmd)

    @classmethod
    def cancel_id(cls, id):
        """
        Cancels command denoted by this id

        Args:
            `id` - command id
        """
        conn=Qubole.agent()
        data={"status":"kill"}
        conn.put(cls.element_path(id), data)
        

    def cancel(self):
        """
        Cancels command represented by this object
        """
        self.__class__.cancel_id(self.id)


    def get_log(self):
        """
        Fetches log for the command represented by this object

        Returns:
            The log as a string
        """
        log_path = self.meta_data['logs_resource']
        conn=Qubole.agent()
        r=conn.get_raw(log_path)
        return r.text

    def get_results(self):
        """
        Fetches the result for the command represented by this object

        Returns:
            The result as a string
        """
        result_path = self.meta_data['results_resource']
        conn=Qubole.agent()
        r = conn.get(result_path)
        if r.get('inline'):
            return r['results'] 
        else:
            # TODO - this will be implemented in future
            print r.get('result_location')
            log.error("Unable to download results, please fetch from S3")

    def run_until_completion(cls, cmd):
        """
        Given a command, waits until the command is complete. Repeatedly polls to check status.

        Args:
            cmd - the command being run

        Returns:
            Command object
        """
        while not Command.is_done(cmd.status):
            time.sleep(Qubole.poll_interval)
            cmd = cls.find(cmd.id)

        return cmd

    def save_results_locally(self, local_file_location, boto_client=None):
        """
        Saves the results to the passed in file_location

        Args:
            local_file_location: the place locally where you want to store the file
            boto_client: the boto client to use if we're fetching results from s3.

        Returns:
            dictionary of:
             'file_location' : <file path + file name>
             'size' : <file size>
             'success' : True or False
             'error' : error message if there was any
        """
        result_path = self.meta_data['results_resource']
        conn=Qubole.agent()
        r = conn.get(result_path)
        if r.get('inline'):
            error = ''
            success = True
            try:
                f = open(local_file_location, 'w')
                f.write(r['results'])
                f.close()
            except Exception as e:
                error = e
                success = False
            result = {
                'file_location': local_file_location,
                'size': os.path.getsize(local_file_location),
                'success': success,
                'error': error
            }
            return result
        else:
            # TODO:finish
            s3_locations = r.get('result_location')
            if not boto_client:
                log.error("Unable to download results, no boto client provided.\
                           Please fetch from S3: %s" % s3_locations)    
                return {'success':False, 'error': 'Not boto client'}
            print s3_locations
            result = {
                'file_location': local_file_location,
                'size': 0, #os.path.getsize(local_file_location),
                'success': False,
                'error': 'Not implemented'
            }
            return result



class HiveCommand(Command):

    usage = ("hivecmd <--query query-string | --script_location location-string>"
             " [--macros <expressions-to-expand-macros>]"
             " [--sample_size <sample-bytes-to-run-query-on]")
               

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("--query", dest="query", help="query string")

    optparser.add_option("--script_location", dest="script_location", 
                         help="Path where hive query to run is stored")

    optparser.add_option("--macros", dest="macros", 
                         help="expressions to expand macros used in query")

    optparser.add_option("--sample_size", dest="sample_size", 
                         help="size of sample in bytes on which to run query")


    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a command

        Args:
            `args` - sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """

        try:
            (options, args) = cls.optparser.parse_args(args)
            if options.query is None and options.script_location is None:
                raise ParseError("One of query or script location"
                                 " must be specified", cls.usage)
        except OptionParsingError as e:
            raise ParseError(e.msg, cls.usage)
        except OptionParsingExit as e:
            return None
        
        return vars(options)

class HadoopCommand(Command):
    subcmdlist = ["jar", "s3distcp", "streaming"]
    usage = "hadoopcmd <%s> <arg1> [arg2] ..." % " | ".join(subcmdlist)
    
    @classmethod
    def parse(cls, args):
        """
        Parse command line arguments to construct a dictionary of command
        parameters that can be used to create a command

        Args:
            `args` - sequence of arguments

        Returns:
            Dictionary that can be used in create method

        Raises:
            ParseError: when the arguments are not correct
        """
        parsed = {}
        
        if len(args) >= 1 and args[0] == "-h":
            sys.stderr.write(cls.usage + "\n")
            return None

        if len(args) < 2:
            raise ParseError("Need at least two arguments", cls.usage)
        
        subcmd = args.pop(0)
        if subcmd not in cls.subcmdlist:
            raise ParseError("First argument must be one of <%s>" % 
                             "|".join(cls.subcmdlist))

        parsed["sub_command"] = subcmd
        parsed["sub_command_args"] = " ".join("'" + a + "'" for a in args)
        
        return parsed

    pass

class PigCommand(Command):
    @classmethod
    def parse(cls, args):
        raise ParseError("pigcmd not implemented yet", "")
    pass

class DbImportCommand(Command):
    @classmethod
    def parse(cls, args):
        raise ParseError("dbimport command not implemented yet", "")
    pass
