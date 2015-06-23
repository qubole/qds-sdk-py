import json

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser
from qds_sdk.commands import *
from qds_sdk.actions import *
import argparse

class CommandTemplateCmdLine:
    list_allparser = None
    viewparser = None
    removeparser = None
    runparser = None
    run_and_waitparser = None
    @staticmethod
    def parsers():
        argparser = ArgumentParser(prog="qds.py commandtemplates",
                                        description="Command Templates client for Qubole Data Service.")
        subparsers = argparser.add_subparsers()

        #list_all command
        list_all = subparsers.add_parser("list", help="List all available command templates")

        list_all.add_argument("--fields", nargs="*", dest="fields",
                          help="List of fields to show")
        list_all.add_argument("--per-page", dest="per_page", type=int, 
                          help="Number of items per page")
        list_all.add_argument("--page", dest="page", type=int,
                          help="Page Number")

        list_all.set_defaults(func=CommandTemplateCmdLine.list_all)
        CommandTemplateCmdLine.list_allparser = list_all

        #view command
        view = subparsers.add_parser("view",
                                     help="View a specific command template")
        view.add_argument("--id", help="Numeric id of the command template", dest="id", type=int)
        view.add_argument("--name", help="Name of the command template", dest="name")
        view.add_argument("--fields", nargs="*", dest="fields",
                          help="List of fields to show")
        view.set_defaults(func=CommandTemplateCmdLine.view)
        CommandTemplateCmdLine.viewparser = view

        #remove command
        remove = subparsers.add_parser("remove",
                                     help="Remove a specific command template")
        remove.add_argument("id", help="Numeric id of the command template", type=int)
        remove.set_defaults(func=CommandTemplateCmdLine.remove)
        CommandTemplateCmdLine.removeparser = remove
        CommandTemplateCmdLine.removeparser = remove

        #run command
        run = subparsers.add_parser("run", help="Run a command template")
        run.add_argument("id", help="Numeric id of the command template")
        run.add_argument("--input_vars",
            help="Add names and values for input variables", dest="input_vars", nargs="*")
        run.set_defaults(func=CommandTemplateCmdLine.run_template)
        CommandTemplateCmdLine.runparser = run

        #run_and_wait command
        run_and_wait = subparsers.add_parser("run_and_wait", 
            help="Run a command template and wait for it to complete")

        run_and_wait.add_argument("id", help="Numeric id of the command template")
        run_and_wait.add_argument("--input_vars", 
            help="Add name and values of input variables", dest="input_vars", nargs="*")

        run_and_wait.set_defaults(func=CommandTemplateCmdLine.run_and_wait)
        CommandTemplateCmdLine.run_and_waitparser = run_and_wait
        return argparser

    @staticmethod
    def get_list_all_help():
        return CommandTemplateCmdLine.list_allparser.format_help()

    @staticmethod
    def get_view_help():
        return CommandTemplateCmdLine.viewparser.format_help()

    @staticmethod
    def get_remove_help():
        return CommandTemplateCmdLine.removeparser.format_help()

    @staticmethod
    def get_run_help():
        return CommandTemplateCmdLine.runparser.format_help()

    @staticmethod
    def get_run_and_wait_help():
        return CommandTemplateCmdLine.run_and_waitparser.format_help()

    @staticmethod
    def run(args):
        parser = CommandTemplateCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)

    @staticmethod
    def filter_fields(schedule, fields):
        filtered = {}
        try:
            for field in fields:
                filtered[field] = schedule[field]
        except KeyError as e:
            raise ParseError("Incorrect field name ",CommandTemplateCmdLine.get_list_all_help() )
        return filtered

    @staticmethod
    def list_all(args):
        commandtemplateslist = CommandTemplate.list_all(args)
        if args.fields:
            for s in commandtemplateslist:
                s.attributes = CommandTemplateCmdLine.filter_fields(s.attributes, args.fields)
        return json.dumps(commandtemplateslist, default=lambda o: o.attributes, sort_keys=True, indent=4)

    @staticmethod
    def view(args):
        if args.id:
            commandtemplate = CommandTemplate.find(args.id)
        elif args.name:
            commandtemplate = CommandTemplate.find_by_name(args.name)
        else:
            raise ParseError("Either template id or template name must be specified",CommandTemplateCmdLine.get_view_help() ) 
        if args.fields:
            commandtemplate.attributes = CommandTemplateCmdLine.filter_fields(commandtemplate.attributes, args.fields)
        return json.dumps(commandtemplate.attributes, sort_keys=True, indent=4)

    @staticmethod
    def remove(args):
        commandtemplate = CommandTemplate.find(args.id)
        return json.dumps(commandtemplate.remove(), sort_keys=True, indent=4)

    @staticmethod
    def run_template(args):
        commandtemplate = CommandTemplate.find(args.id)
        return json.dumps(commandtemplate.run_template(args), sort_keys=True, indent=4)

    @staticmethod
    def run_and_wait(args):
        commandtemplate = CommandTemplate.find(args.id)
        cmd = Command.find(commandtemplate.run_template(args)['id'])
        while not Command.is_done(cmd.status):
            time.sleep(Qubole.poll_interval)
            cmd = Command.find(cmd.id)

        if Command.is_success(cmd.status):
            return cmd.get_results(sys.stdout, delim='\t')
        else:
            return "Cannot fetch results - command Id: %s failed with status: %s" % (cmd.id, cmd.status)


class CommandTemplate(Resource):

    rest_entity_path = "command_templates"
    @staticmethod
    def list_all(args):
        conn = Qubole.agent()
        url_path = CommandTemplate.rest_entity_path
        page_attr = {}
        if args.page is not None:
            page_attr["page"] = args.page
        if args.per_page is not None:
            page_attr["per_page"] = args.per_page
        if len(page_attr):
            myjson = conn.get(url_path, params = page_attr)
        else:
            myjson = conn.get(url_path)
        commandtemplateslist = []
        for commandtemplate in myjson['command_templates']:
            commandtemplateslist.append(CommandTemplate(commandtemplate))
        return commandtemplateslist

    @staticmethod
    def find_by_name(name):
        conn = Qubole.agent()
        if name is not None:
            result_json = (conn.get(CommandTemplate.rest_entity_path, params={"template_name":name}))
            if result_json["command_templates"]:
                return CommandTemplate(result_json["command_templates"][0])
        return None

    def remove(self):
        conn = Qubole.agent()
        remove_url = "%s/remove" % self.element_path(self.id)
        data = {}
        return conn.put(remove_url, data)

    def run_template(self, args):
        conn = Qubole.agent()
        run_url = "%s/run" % self.element_path(self.id)
        data = {}
        data['input_vars'] = []
        if args.input_vars is not None:
            for input_var in args.input_vars:
                a = input_var.split("=")
                input_var_data = {a[0]:a[1]}
                data['input_vars'].append(input_var_data)
        return conn.post(run_url, data)
