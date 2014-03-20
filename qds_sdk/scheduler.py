from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from qds_sdk.exception import ParseError

from qds_sdk.account import Account

from qds_sdk.util import GentleOptionParser
from qds_sdk.util import OptionParsingError
from qds_sdk.util import OptionParsingExit

from qds_sdk import argparse

import time
import logging
import sys
import json
		    
class Scheduler(Resource):

    """
    qds_sdk.Schedule is the base Qubole Schedule class.
    """

    """ all commands use the /scheduler endpoint"""
    
    rest_entity_path="scheduler"
    
    def __init__(self):
      self.argparser = argparse.ArgumentParser(prog="qds.py scheduler", description="Scheduler client for Qubole Data Service.")
      subparsers = self.argparser.add_subparsers()
      #List
      list = subparsers.add_parser("list",
          help="List all schedulers")
      list.add_argument("--running", dest="running",
          action="store_true", help="Show running schedules")
      list.add_argument("--fields", nargs="*", dest="fields",
          help="List of fields to show")
      list.add_argument("--per-page", dest="per_page",
          help="Number of items per page")
      list.add_argument("--page", dest="page",
          help="Page Number")
      list.set_defaults(func=self.list)

      #View
      view = subparsers.add_parser("view",
          help="View a specific schedule")
      view.add_argument("id", help="Numeric id or name of the schedule")
      view.add_argument("--fields", nargs="*", dest="fields",
          help="List of fields to show")
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
      
      #Kill
      kill = subparsers.add_parser("kill",
          help="Kill a specific schedule")
      kill.add_argument("id", help="Numeric id or name of the schedule")
      kill.set_defaults(func=self.kill)

      #List Instances
      list_instances = subparsers.add_parser("list-instances",
          help="List instances of a specific schedule")
      list_instances.add_argument("id", help="Numeric id or name of the schedule")
      list_instances.set_defaults(func=self.list_instances)
    
    def run(self, args):
      parsed = self.argparser.parse_args(args)
      parsed.func(parsed)

    def filter_fields(self,schedule, fields):
      filtered = {}
      for field in fields:
        filtered[field] = schedule[field]
      return filtered

    def list(self, args):
        conn =  Qubole.agent()
        url = Scheduler.rest_entity_path
        page_attr = []
        if args.page is not None:
          page_attr.append("page=%s"  % args.page)
        if args.per_page is not None:
          page_attr.append("per_page=%s"  % args.per_page)
        if args:
          url = "%s?%s" % (Scheduler.rest_entity_path, "&".join(page_attr))
        ll = conn.get(url)
        if args.fields:
          result = {}
          result["paging_info"] = ll["paging_info"]
          result["schedules"] = []
          schedules = ll["schedules"]
          for schedule in ll["schedules"]:
            result["schedules"].append(self.filter_fields(schedule, args.fields))
          ll = result
        print json.dumps(ll, indent=4, sort_keys=True)
    
    def view(self, args):
        conn =  Qubole.agent()
        schedule = conn.get(self.element_path(args.id))
        if args.fields:
          schedule = self.filter_fields(schedule, args.fields)
        print json.dumps(schedule, indent=4, sort_keys=True)
        
    def suspend(self, args):
        conn=Qubole.agent()
        data={"status":"suspend"}
        conn.put(self.element_path(args.id), data)
        print conn.get(self.element_path(args.id))['status']
        
    def resume(self, args):
        conn=Qubole.agent()
        data={"status":"resume"}
        conn.put(self.element_path(args.id), data)
        return conn.get(self.element_path(args.id))['status']
        
    def kill(self, args):
        conn=Qubole.agent()
        data={"status":"kill"}
        conn.put(self.element_path(args.id), data)
        print conn.get(self.element_path(args.id))['status']
                
    def list_instances(self, args):
        conn=Qubole.agent()
        url_path = self.element_path(args.id) + "/" + "instances"
        return conn.get(url_path)
