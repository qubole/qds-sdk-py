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
		    
class Scheduler(Resource):

    """
    qds_sdk.Schedule is the base Qubole Schedule class.
    """

    """ all commands use the /scheduler endpoint"""
    
    rest_entity_path="scheduler"
    
    def __init__(self,subparsers):
      #List
      list = subparsers.add_parser("list",
          help="List all schedulers")
      list.add_argument("--running", dest="running",
          action="store_true", help="Show running schedules")
      list.add_argument("--fields", nargs="*", dest="fields",
          help="List of fields to show")
      list.set_defaults(func=self.list)

      #View
      view = subparsers.add_parser("view",
          help="View a specific schedule")
      view.add_argument("id", help="Numeric id or name of the schedule")
      view.set_defaults(func=self.view)

      #Suspend
      suspend = subparsers.add_parser("suspend",
          help="Suspend a specific schedule")
      suspend.add_argument("id", help="Numeric id or name of the schedule")
      suspend.set_defaults(func=self.suspend)

      #Resume
      resume = subparsers.add_parser("resume",
          help="Resume a specific schedule")
      resume.add_argument("id", help="Numeric id or name of the schedule")
      resume.set_defaults(func=self.resume)

      #List Instances
      list_instances = subparsers.add_parser("list-instances",
          help="List instances of a specific schedule")
      list_instances.add_argument("id", help="Numeric id or name of the schedule")
      list_instances.set_defaults(func=self.list_instances)
      
    def list(self, args):
        conn =  Qubole.agent()
        return conn.get(Schedule.rest_entity_path)
    
    def view(self, args):
        conn =  Qubole.agent()
        return conn.get(self.element_path(args.id))
        
    def suspend(self, args):
        conn=Qubole.agent()
        data={"status":"suspend"}
        conn.put(self.element_path(args.id), data)
        return conn.get(self.element_path(args.id))['status']
        
    def resume(self, args):
        conn=Qubole.agent()
        data={"status":"resume"}
        conn.put(self.element_path(args.id), data)
        return conn.get(self.element_path(args.id))['status']
        
    def kill(self, args):
        conn=Qubole.agent()
        data={"status":"kill"}
        conn.put(self.element_path(args.id), data)
        return conn.get(self.element_path(args.id))['status']
                
    def list_instances(self, args):
        conn=Qubole.agent()
        url_path = self.element_path(args.id) + "/" + "instances"
        return conn.get(url_path)
