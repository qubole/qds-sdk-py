import json

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser
from qds_sdk.commands import *
from qds_sdk.actions import *

class SchedulerCmdLine:
    """
    qds_sdk.ScheduleCmdLine is the interface used by qds.py.
    """

    @staticmethod
    def parsers():
        argparser = ArgumentParser(prog="qds.py scheduler",
                                        description="Scheduler client for Qubole Data Service.")
        subparsers = argparser.add_subparsers()

        #Create
        create = subparsers.add_parser("create",
                                       help="Create a new schedule")
        create.add_argument("--data", dest="data", required=True,
                            help="Path to a file that contains scheduler attributes as a json object")
        create.set_defaults(func=SchedulerCmdLine.create)

        #List
        list = subparsers.add_parser("list",
                                     help="List all schedulers")
        list.add_argument("--fields", nargs="*", dest="fields",
                          help="List of fields to show")
        list.add_argument("--per-page", dest="per_page",
                          help="Number of items per page")
        list.add_argument("--page", dest="page",
                          help="Page Number")
        list.set_defaults(func=SchedulerCmdLine.list)

        #View
        view = subparsers.add_parser("view",
                                     help="View a specific schedule")
        view.add_argument("id", help="Numeric id of the schedule")
        view.add_argument("--fields", nargs="*", dest="fields",
                          help="List of fields to show")
        view.set_defaults(func=SchedulerCmdLine.view)

        #View by name
        view_by_name = subparsers.add_parser("view_by_name",
                                     help="View a specific schedule")
        view_by_name.add_argument("name", help="Name of the schedule")
        view_by_name.add_argument("--fields", nargs="*", dest="fields",
                          help="List of fields to show")
        view_by_name.set_defaults(func=SchedulerCmdLine.view_by_name)

        #Suspend
        suspend = subparsers.add_parser("suspend",
                                        help="Suspend a specific schedule")
        suspend.add_argument("id", help="Numeric id or name of the schedule")
        suspend.set_defaults(func=SchedulerCmdLine.suspend)

        #Resume
        resume = subparsers.add_parser("resume",
                                       help="Resume a specific schedule")
        resume.add_argument("id", help="Numeric id or name of the schedule")
        resume.set_defaults(func=SchedulerCmdLine.resume)

        #Kill
        kill = subparsers.add_parser("kill",
                                     help="Kill a specific schedule")
        kill.add_argument("id", help="Numeric id or name of the schedule")
        kill.set_defaults(func=SchedulerCmdLine.kill)

        #List Actions
        list_actions = subparsers.add_parser("list-actions",
                                               help="List actions of a specific schedule")
        list_actions.add_argument("id", help="Numeric id or name of the schedule")
        list_actions.add_argument("--sequence_id", dest="sequence_id", help="Sequence id of the actions to list")
        list_actions.add_argument("--fields", nargs="*", dest="fields",
                          help="List of fields to show")
        list_actions.add_argument("--per-page", dest="per_page",
                                    help="Number of items per page")
        list_actions.add_argument("--page", dest="page",
                                    help="Page Number")
        list_actions.set_defaults(func=SchedulerCmdLine.list_actions)

        #List Instances
        list_instances = subparsers.add_parser("list-instances",
                                               help="List instances of a specific schedule")
        list_instances.add_argument("id", help="Numeric id or name of the schedule")
        list_instances.add_argument("--fields", nargs="*", dest="fields",
                          help="List of fields to show")
        list_instances.add_argument("--per-page", dest="per_page",
                                    help="Number of items per page")
        list_instances.add_argument("--page", dest="page",
                                    help="Page Number")
        list_instances.set_defaults(func=SchedulerCmdLine.list_instances)

        rerun = subparsers.add_parser("rerun",
                                      help="Rerun an instance of a schedule")
        rerun.add_argument("id", help="Numeric id or name of the schedule")
        rerun.add_argument("instance_id", help="Numeric id of the instance")
        rerun.set_defaults(func=SchedulerCmdLine.rerun)
        return argparser

    @staticmethod
    def run(args):
        parser = SchedulerCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)

    @staticmethod
    def filter_fields(schedule, fields):
        filtered = {}
        for field in fields:
            filtered[field] = schedule[field]
        return filtered

    @staticmethod
    def create(args):
        with open(args.data) as f:
            spec = json.load(f)
        schedule = Scheduler(spec)
        return json.dumps(schedule.attributes, sort_keys=True, indent=4)

    @staticmethod
    def list(args):
        schedlist = Scheduler.list(args.page, args.per_page)
        if args.fields:
            for s in schedlist:
                s.attributes = SchedulerCmdLine.filter_fields(s.attributes, args.fields)
        return json.dumps(schedlist, default=lambda o: o.attributes, sort_keys=True, indent=4)

    @staticmethod
    def view(args):
        schedule = Scheduler.find(args.id)
        if args.fields:
            schedule.attributes = SchedulerCmdLine.filter_fields(schedule.attributes, args.fields)
        return json.dumps(schedule.attributes, sort_keys=True, indent=4)

    @staticmethod
    def view_by_name(args):
        schedule = Scheduler.find_by_name(args.name)
        if schedule is None:
            return "Schedule '%s' not found" % args.name
        if args.fields:
            schedule.attributes = SchedulerCmdLine.filter_fields(schedule.attributes, args.fields)
        return json.dumps(schedule.attributes, sort_keys=True, indent=4)

    @staticmethod
    def suspend(args):
        schedule = Scheduler.find(args.id)
        return json.dumps(schedule.suspend(), sort_keys=True, indent=4)

    @staticmethod
    def resume(args):
        schedule = Scheduler.find(args.id)
        return json.dumps(schedule.resume(), sort_keys=True, indent=4)

    @staticmethod
    def kill(args):
        schedule = Scheduler.find(args.id)
        return json.dumps(schedule.kill(), sort_keys=True, indent=4)

    @staticmethod
    def list_actions(args):
        schedule = Scheduler.find(args.id)
        actlist = schedule.list_actions(args.sequence_id, args.page, args.per_page)
        if args.fields:
            for a in actlist:
                a.attributes = ActionCmdLine.filter_fields(a.attributes, args.fields)
        return json.dumps(actlist, default=lambda o: o.attributes,
                          sort_keys=True, indent=4)

    @staticmethod
    def list_instances(args):
        schedule = Scheduler.find(args.id)
        cmdlist = schedule.list_instances(args.page, args.per_page)
        if args.fields:
            for cmd in cmdlist:
                cmd.attributes = SchedulerCmdLine.filter_fields(cmd.attributes, args.fields)
        return json.dumps(cmdlist, default=lambda o: o.attributes,
                          sort_keys=True, indent=4)

    @staticmethod
    def rerun(args):
        schedule = Scheduler.find(args.id)
        return schedule.rerun(args.instance_id)


class Scheduler(Resource):
    """
    qds_sdk.Schedule is the base Qubole Schedule class.
    """

    """ all commands use the /scheduler endpoint"""

    rest_entity_path = "scheduler"

    @staticmethod
    def list(page = None, per_page = None):
        conn = Qubole.agent()
        url_path = Scheduler.rest_entity_path
        page_attr = []
        if page is not None:
            page_attr.append("page=%s" % page)
        if per_page is not None:
            page_attr.append("per_page=%s" % per_page)
        if page_attr:
            url_path = "%s?%s" % (Scheduler.rest_entity_path, "&".join(page_attr))

        #Todo Page numbers are thrown away right now
        schedjson = conn.get(url_path)
        schedlist = []
        for s in schedjson["schedules"]:
            schedlist.append(Scheduler(s))
        return schedlist

    @staticmethod
    def find_by_name(name):
        conn = Qubole.agent()
        if name is not None:
            schedjson = conn.get(Scheduler.rest_entity_path, params={"name":name})
            if schedjson["schedules"]:
                return Scheduler(schedjson["schedules"][0])
        return None

    def suspend(self):
        conn = Qubole.agent()
        data = {"status": "suspend"}
        return conn.put(self.element_path(self.id), data)

    def resume(self):
        conn = Qubole.agent()
        data = {"status": "resume"}
        return conn.put(self.element_path(self.id), data)

    def kill(self):
        conn = Qubole.agent()
        data = {"status": "kill"}
        return conn.put(self.element_path(self.id), data)

    def list_actions(self, sequence_id = None, page=None, per_page=None):
        conn = Qubole.agent()
        url_path = self.element_path(self.id) + "/" + "actions"
        if sequence_id is not None:
            url_path = url_path + "/" +  str(sequence_id)
        params = {}
        if page is not None:
            params['page'] = page
        if per_page is not None:
            params['per_page'] = per_page

        #Todo Page numbers are thrown away right now
        actjson = conn.get(url_path, params)
        actlist = []
        for act in actjson["actions"]:
            actlist.append(Action(act))
        return actlist

    def list_instances(self, page=None, per_page=None):
        conn = Qubole.agent()
        url_path = self.element_path(self.id) + "/" + "instances"
        page_attr = []
        if page is not None:
            page_attr.append("page=%s" % page)
        if per_page is not None:
            page_attr.append("per_page=%s" % per_page)
        if page_attr:
            url_path = "%s/instances?%s" % (self.element_path(args.id), "&".join(page_attr))
        #Todo Page numbers are thrown away right now
        cmdjson = conn.get(url_path)
        cmdlist = []
        for cmd in cmdjson["commands"]:
            cmdclass = globals()[cmd["command_type"]]
            onecmd = cmdclass(cmd)
            cmdlist.append(onecmd)
        return cmdlist

    def rerun(self, instance_id):
        conn = Qubole.agent()
        url_path = self.element_path(id) + "/instances/" + instance_id + "/rerun"
        return conn.post(url_path)['status']
