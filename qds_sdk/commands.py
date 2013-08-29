""" 
The commands module contains the base definition for
a generic Qubole command and the implementation of all 
the specific commands
"""

from qubole import Qubole
from resource import Resource
from exception import ParseError
from account import Account
from qds_sdk.util import GentleOptionParser
from qds_sdk.util import OptionParsingError
from qds_sdk.util import OptionParsingExit

import boto

import time
import logging
import sys
import re
import os
import pipes

log = logging.getLogger("qds_commands")

# Pattern matcher for s3 path
_URI_RE = re.compile(r's3://([^/]+)/?(.*)')

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

    @staticmethod
    def is_success(status):
        return (status == "done")
    
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
        while not Command.is_done(cmd.status):
            time.sleep(Qubole.poll_interval)
            cmd = cls.find(cmd.id)

        return cmd

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

    def get_results(self, fp=sys.stdout, inline=True):
        """
        Fetches the result for the command represented by this object

        @param fp: a file object to write the results to directly
        """
        result_path = self.meta_data['results_resource']
        
        conn=Qubole.agent()
        
        r = conn.get(result_path , {'inline': inline})
        if r.get('inline'):
            fp.write(r['results'].encode('utf8'))
        else:    
            acc = Account.find()
            boto_conn = boto.connect_s3(aws_access_key_id=acc.storage_access_key,
                                        aws_secret_access_key=acc.storage_secret_key)

            log.info("Starting download from result locations: [%s]" % ",".join(r['result_location']))

            for s3_path in r['result_location']:
                _download_to_local(boto_conn, s3_path, fp)


class HiveCommand(Command):

    usage = ("hivecmd run [options]")
               

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("-q", "--query", dest="query", help="query string")

    optparser.add_option("-f", "--script_location", dest="script_location", 
                         help="Path where hive query to run is stored. Can be S3 URI or local file path")

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
                except:
                    raise ParseError("Unable to open script location: %s" % 
                                     options.script_location,
                                     cls.optparser.format_help())
                options.script_location = None
                options.query = q


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

class ShellCommand(Command):
    usage = ("shellcmd run [options] [arg1] [arg2] ...")
               

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("-s", "--script", dest="inline", help="inline script that can be executed by bash")

    optparser.add_option("-f", "--script_location", dest="script_location", 
                         help="Path where bash script to run is stored. Can be S3 URI or local file path")

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
                except:
                    raise ParseError("Unable to open script location: %s" % 
                                     options.script_location,
                                     cls.optparser.format_help())
                options.script_location = None
                options.inline = s

            if ((args is not None) and (len(args) > 0)):
                if options.inline is not None:
                    raise ParseError(
                        "This sucks - but extra arguments can only be "
                        "supplied with a script_location in S3 right now",
                        cls.optparser.format_help())

                setattr(options, 'parameters',
                        " ".join([pipes.quote(a) for a in args]))


        else:
            if ((args is not None) and (len(args) > 0)):
                raise ParseError(
                    "Extra arguments can only be supplied with a script_location",
                    cls.optparser.format_help())                

        return vars(options)

class PigCommand(Command):
    usage = ("pigcmd run [options] [key1=value1] [key2=value2] ...")
               

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("-s", "--script", dest="latin_statements",
                         help="latin statements that has to be executed")

    optparser.add_option("-f", "--script_location", dest="script_location", 
                         help="Path where bash script to run is stored. Can be S3 URI or local file path")

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
                except:
                    raise ParseError("Unable to open script location: %s" % 
                                     options.script_location,
                                     cls.optparser.format_help())
                options.script_location = None
                options.latin_statements = s

            if ((args is not None) and (len(args) > 0)):
                if options.latin_statements is not None:
                    raise ParseError(
                        "This sucks - but extra arguments can only be "
                        "supplied with a script_location in S3 right now",
                        cls.optparser.format_help())

                p = {}
                for a in args:
                  kv = a.split('=')
                  if len(kv)!=2:
                    raise ParseError("Arguments to pig script must be of this format k1=v1 k2=v2 k3=v3...")
                  p[kv[0]] = kv[1]
                setattr(options, 'parameters',p)

        else:
            if ((args is not None) and (len(args) > 0)):
                raise ParseError(
                    "Extra arguments can only be supplied with a script_location",
                    cls.optparser.format_help())                
        
        return vars(options)

class DbImportCommand(Command):
    @classmethod
    def parse(cls, args):
        raise ParseError("dbimport command not implemented yet", "")
    pass


def _download_to_local(boto_conn, s3_path, fp):
    '''
    Downloads the contents of all objects in s3_path into fp
    
    @param boto_conn: S3 connection object
    @param s3_path: S3 path to be downloaded
    @param fp: The file object where data is to be downloaded
    
    '''
    #Progress bar to display download progress
    def _callback(downloaded,  total):
        '''
        Call function for upload.
        @param key_name: File size already downloaded
        @type key_name: int
        @param key_prefix: Total file size to be downloaded
        @type key_prefix: int
        '''
        if ((total is 0) or (downloaded == total)):
            return
        progress = downloaded*100/total
        sys.stderr.write('\r[{0}] {1}%'.format('#'*progress, progress))
        sys.stderr.flush()
        
    
    m = _URI_RE.match(s3_path)     
    bucket_name = m.group(1)
    bucket = boto_conn.get_bucket(bucket_name)
        
    if s3_path.endswith('/') is False:
        #It is a file
        key_name = m.group(2)  
        key_instance = bucket.get_key(key_name)
        
        log.info("Downloading file from %s" % s3_path)
        key_instance.get_contents_to_file(fp) #cb=_callback
        
    else:
        #It is a folder
        key_prefix = m.group(2)
        bucket_paths = bucket.list(key_prefix)
        
        for one_path in bucket_paths:
            name = one_path.name
            
            # Eliminate _tmp_ files which ends with $folder$
            if name.endswith('$folder$'):
                continue
                
            log.info("Downloading file from %s" % name)
            one_path.get_contents_to_file(fp) #cb=_callback
