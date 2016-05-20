"""
The Template Module contains the base definition for Executing Templates
"""
import json

from argparse import ArgumentParser
from argparse import ArgumentTypeError
from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from qds_sdk.commands import *

CommandClasses = {
    "hivecmd": HiveCommand,
    "sparkcmd": SparkCommand,
    "dbtapquerycmd": DbTapQueryCommand,
    "pigcmd":  PigCommand,
    "hadoopcmd": HadoopCommand,
    "shellcmd": ShellCommand,
    "dbexportcmd": DbExportCommand,
    "dbimportcmd": DbImportCommand,
    "prestocmd": PrestoCommand
}


class TemplateCmdLine:
    
    @staticmethod
    def parsers():
        
        def check_pair(pair):
            try:
                key, value = pair.split("=")
            except Exception:
                raise ArgumentTypeError(
                    "%s is an invalid key=value pair." % pair)
            return key, value
            
        argparser = ArgumentParser(prog="qds.py template", description="Managing Query Templates for Qubole Data Service")
        subparsers = argparser.add_subparsers()
        
        #create 
        create = subparsers.add_parser("create", help="Create a new Template")
        create.add_argument("--data",dest="data",required=True,help="Path to JSON file with template details")
        create.set_defaults(func=TemplateCmdLine.create)
        
        #edit
        edit = subparsers.add_parser("edit", help="Edit an Existing Template")
        edit.add_argument("--data",dest="data",required=True,help="Path to JSON file with template details")
        edit.add_argument("--id",dest="id",required=True, help="Name for the Template")
        edit.set_defaults(func=TemplateCmdLine.edit)
        
        #view
        view = subparsers.add_parser("view", help="To view an existing Template by Id")
        view.add_argument("--id",dest="id",required=True,help="Name for the Template")
        view.set_defaults(func=TemplateCmdLine.view)
        
        #list 
        list = subparsers.add_parser("list", help="To list existing Templates")
        list.add_argument("--per-page", dest="per_page", help="Number of items per page")
        list.add_argument("--page", dest="page", help="Page Number")
        list.set_defaults(func=TemplateCmdLine.list)
        
        #run
        run = subparsers.add_parser("run", help="To view an existing Template by Id or Name")
        run.add_argument("--id", dest="id",required=True, help="Id of the template to run")
        run.add_argument("--j",dest="data",required=True,help="Path to JSON file or json string with input field details")
        run.set_defaults(func=TemplateCmdLine.execute)
        
        return argparser
        
    @staticmethod
    def run(args):      
        parser = TemplateCmdLine.parsers()
        otherArgs = []
        (parsed, otherArgs) = parser.parse_known_args(args)
        return parsed.func(parsed, otherArgs)
    
    @staticmethod
    def create(args, otherArgs):
        with open(args.data) as f:
            spec = json.load(f)
        return Template.createTemplate(spec)
    
    @staticmethod
    def edit(args, otherArgs):
        with open(args.data) as f:
            spec = json.load(f)
        return Template.editTemplate(args.id, spec)
        
    
    @staticmethod
    def execute(args, otherArgs):
        print(args)
        if os.path.isfile(args.data):
            print('yes,..its a valid path')
            with open(args.data) as f:
                spec = json.load(f)
        else:
            print("not a valid path", args.data)
            inputs = json.loads(args.data)
            inputs = formatData(inputs)
            spec = {
                "input_vars" : inputs
            }
        print("spec===", spec)
        return Template.runTemplate(args.id, spec)
    
    @staticmethod
    def view(args, otherArgs):
        id = args.id
        return Template.viewTemplate(id)
    
    @staticmethod
    def list(args, otherArgs):
        return Template.listTemplates(args)


def formatData(inputs):
    res = []
    if len(inputs) != 0:
        for obj in inputs:
            o = {}
            for key in obj:
                o[key] = "'" + obj[key] + "'"
            res.append(o)
    return res
                    
class Template(Resource):
    
    rest_entity_path = "command_templates"
    
    @staticmethod
    def createTemplate(data):
        conn = Qubole.agent()
        return conn.post(Template.rest_entity_path,data)
    
    @staticmethod
    def editTemplate(id, data):
        conn = Qubole.agent()
        return conn.put(Template.element_path(id), data)
    
    @staticmethod
    def viewTemplate(id):
        conn = Qubole.agent()
        return conn.get(Template.element_path(id))
    
    @staticmethod
    def runTemplate(id, data):
        conn = Qubole.agent()
        return conn.post(Template.element_path(id + "/run"), data)
    
    @staticmethod
    def listTemplates(args):
        conn = Qubole.agent()
        url_path = Template.rest_entity_path
        page_attr = []
        #page = None, per_page = None
        if args.page is not None:
            page_attr.append("page=%s" % args.page)
        if args.per_page is not None:
            page_attr.append("per_page=%s" % args.per_page)
        if page_attr:
            url_path = "%s?%s" % (url_path, "&".join(page_attr))

        templatesjson = conn.get(url_path)
        return templatesjson