"""
The actions module contains the definition for actions started by scheduler.
"""

from __future__ import print_function
import json

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser
from qds_sdk.commands import *

import logging

log = logging.getLogger("qds_actions")

class ActionCmdLine:
    """
    qds_sdk.ActionsCmdLine is the interface used by qds.py.
    """

    @staticmethod
    def parsers():
        argparser = ArgumentParser(prog="qds.py action",
                                        description="Scheduler action client for Qubole Data Service.")
        subparsers = argparser.add_subparsers()

        #List
        list = subparsers.add_parser("list",
                                     help="List actions for all schedulers")
        list.add_argument("--per-page", dest="per_page",
                          help="Number of items per page")
        list.add_argument("--page", dest="page",
                          help="Page Number")
        list.add_argument("--fields", nargs="*", dest="fields",
                          help="List of fields to show")
        list.set_defaults(func=ActionCmdLine.list)

        #View
        view = subparsers.add_parser("view",
                                     help="View a specific action")
        view.add_argument("id", help="Numeric id or name of the action")
        view.add_argument("--fields", nargs="*", dest="fields",
                          help="List of fields to show")
        view.set_defaults(func=ActionCmdLine.view)

        #Kill
        kill = subparsers.add_parser("kill",
                                     help="Kill a specific schedule action")
        kill.add_argument("id", help="Numeric id or name of the action")
        kill.set_defaults(func=ActionCmdLine.kill)

        #rerun
        rerun = subparsers.add_parser("rerun",
                                     help="rerun a specific schedule action")
        rerun.add_argument("id", help="Numeric id or name of the action")
        rerun.set_defaults(func=ActionCmdLine.rerun)

        #logs
        logs = subparsers.add_parser("logs",
                                     help="logs for a specific schedule action")
        logs.add_argument("id", help="Numeric id or name of the action")
        logs.set_defaults(func=ActionCmdLine.logs)

        #resultss
        results = subparsers.add_parser("results",
                                     help="results for a specific schedule action")
        results.add_argument("id", help="Numeric id or name of the action")
        results.set_defaults(func=ActionCmdLine.results)

        return argparser

    @staticmethod
    def run(args):
        parser = ActionCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)

    @staticmethod
    def filter_fields(act, fields):
        filtered = {}
        for field in fields:
            filtered[field] = act[field]
        return filtered

    @staticmethod
    def list(args):
        actionlist = Action.list(args.page, args.per_page)
        if args.fields:
            for a in actionlist:
                a.attributes = ActionCmdLine.filter_fields(a.attributes, args.fields)
        return json.dumps(actionlist, default=lambda o: o.attributes, sort_keys=True, indent=4)

    @staticmethod
    def view(args):
        act = Action.find(args.id)
        if args.fields:
            act.attributes = ActionCmdLine.filter_fields(act.attributes, args.fields)
        return json.dumps(act.attributes, sort_keys=True, indent=4)

    @staticmethod
    def kill(args):
        conn = Qubole.agent()
        ret_val = conn.put(Action.element_path(args.id) + "/kill", data=None)
        return json.dumps(ret_val, sort_keys=True, indent=4)

    @staticmethod
    def rerun(args):
        conn = Qubole.agent()
        ret_val = conn.post(Action.element_path(args.id) + "/rerun", data=None)
        return json.dumps(ret_val, sort_keys=True, indent=4)

    @staticmethod
    def logs(args):
        action = Action.find(args.id)
        print(action.logs())

    @staticmethod
    def results(args):
        action = Action.find(args.id)
        action.results()

class Action(Resource):

    """
    qds_sdk.Action is the Qubole action class.
    """

    """all actions use the /actions endpoint"""
    rest_entity_path = "actions"

    def getcommand(self):
        cmd = self.attributes["command"]
        if cmd is None:
            return None
        cmdclass = globals()[cmd["command_type"]]
        obj = cmdclass(cmd)
        return obj

    @staticmethod
    def list(page = None, per_page = None):
        conn = Qubole.agent()
        url_path = Action.rest_entity_path
        params = {}
        if page is not None:
            params['page'] = page
        if per_page is not None:
            params['per_page'] = per_page

        #Todo Page numbers are thrown away right now
        actjson = conn.get(url_path, params)
        actlist = []
        for a in actjson["actions"]:
            actlist.append(Action(a))
        return actlist

    def kill(self):
        conn = Qubole.agent()
        return conn.put(self.element_path(self.id) + "/kill", data=None)

    def rerun(self):
        conn = Qubole.agent()
        return conn.post(self.element_path(self.id) + "/rerun", data=None)

    def status(self):
        return self.attributes["status"]

    def logs(self):
        if self.status().lower() == "submitted":
            return "Logs for action are not yet available. Action is waiting for underlying command to be created."
        if self.status().lower() == "waiting":
            return "Logs for action are not yet available. Waiting for dependencies to be met."
        if self.status().lower() == "not_found":
            return "Logs for action are not available. Dependencies not found."
        if self.status().lower() == "cancelled":
            return "Logs for action are not available. Action was cancelled before underlying command gets created."
        cmd = self.getcommand()
        if cmd is None:
            return "Logs for action are not yet available."
        else:
            return cmd.get_log()

    def results(self):
        cmd = self.getcommand()
        if cmd is None:
            print("Results for action are not yet available.")
        else:
            cmd.get_results()


