from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from qds_sdk.exception import ParseError

from qds_sdk.account import Account

from qds_sdk.util import GentleOptionParser
from qds_sdk.util import OptionParsingError
from qds_sdk.util import OptionParsingExit

import time
import logging
import sys
		    
class Schedule(Resource):

    """
    qds_sdk.Schedule is the base Qubole Schedule class.
    """

    """ all commands use the /scheduler endpoint"""
    
    rest_entity_path="scheduler"
    
    def __init__(self,action):
        self.action = action

    usage = ("schedule --scheduler-id <scheduler-id> "
             " [--instance-id <instance-id>]")

    optparser = GentleOptionParser(usage=usage)
    optparser.add_option("--scheduler-id", dest="schid", help="Scheduler id")

    optparser.add_option("--instance-id", dest="instid", 
                         help="Sequence id")

    def parse(self, args):
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
        
        if self.action == "list":
            return None
        try:
            (options, args) = self.optparser.parse_args(args)
            if options.schid is None:
                raise ParseError("Scheduler id must be present", self.usage)
            instid_reqd = ["viewcommand", "getresult", "getlog"]
            if self.action in instid_reqd:
                if options.instid is None:
                    raise ParseError("Instance id must be present", self.usage)
        except OptionParsingError as e:
            raise ParseError(e.msg, cls.usage)
        except OptionParsingExit as e:
            return None
        
        return vars(options)
        
    def list_schedulers(self):
        
        conn =  Qubole.agent()
        return conn.get(Schedule.rest_entity_path)
    
    def view_schedulers(self, schid):
        
        conn =  Qubole.agent()
        return conn.get(self.element_path(schid))
        
    def suspend_scheduler(self, schid):
        
        conn=Qubole.agent()
        data={"status":"suspend"}
        conn.put(self.element_path(schid), data)
        return conn.get(self.element_path(schid))['status']
        
    def resume_scheduler(self, schid):
        
        conn=Qubole.agent()
        data={"status":"resume"}
        conn.put(self.element_path(schid), data)
        return conn.get(self.element_path(schid))['status']
        
    def kill_scheduler(self, schid):
        
        conn=Qubole.agent()
        data={"status":"kill"}
        conn.put(self.element_path(schid), data)
        return conn.get(self.element_path(schid))['status']
                
    def list_instance(self, schid):
        
        conn=Qubole.agent()
        
        url_path = self.element_path(schid) + "/" + "instances"
        return conn.get(url_path)
        
        
    def view_command(self, schid, instid):
        
        conn=Qubole.agent()
        
        #Any better way to do it?
        url_path = self.element_path(schid) + "/" + "instances" + "/" + instid
        return conn.get(url_path)
        
        
    def get_results(self, schid, instid):
        
        conn=Qubole.agent()
        
        #Any better way to do it?
        url_path = self.element_path(schid) + "/" + "instances" + "/" + instid + "/" + "results"
        return conn.get(url_path)
        
    def get_logs(self, schid, instid):
        
        conn=Qubole.agent()
        
        #Any better way to do it? -- All words are hardcoded! :(
        url_path = self.element_path(schid) + "/" + "instances" + "/" + instid + "/" + "logs"
        return (conn.get_raw(url_path)).text
        
