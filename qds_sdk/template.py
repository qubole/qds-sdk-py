"""
The Template Module contains the base definition for Executing Templates
"""
import json
import logging
import os

from argparse import ArgumentParser
from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource

log = logging.getLogger("qds_template")

from qds_sdk.commands import *

class TemplateCmdLine:
    """
    qds_sdk.TemplateCmdLine is the interface used for template related operation in qds.py
    """
    
    @staticmethod
    def parsers():
        argparser = ArgumentParser(prog="qds.py template", description="Template Client for Qubole Data Service.")
        subparsers = argparser.add_subparsers()
        
        #create 
        create = subparsers.add_parser("create", help="To Create a new Template")
        create.add_argument("--data", dest="data",required=True, help="Path to JSON file with template details")
        create.set_defaults(func=TemplateCmdLine.create)
        
        #edit
        edit = subparsers.add_parser("edit", help="To Edit an existing Template")
        edit.add_argument("--data", dest="data", required=True, help="Path to JSON file with template details")
        edit.add_argument("--id", dest="id", required=True, help="Id for the Template")
        edit.set_defaults(func=TemplateCmdLine.edit)
        
        #clone
        clone = subparsers.add_parser("clone", help="To Clone an existing Template")
        clone.add_argument("--id", dest="id",required=True, help="Id for the Template to be Cloned")
        clone.add_argument("--data", dest="data", required=True, help="Path to JSON file with template details to override")
        clone.set_defaults(func=TemplateCmdLine.clone)
        
        #view
        view = subparsers.add_parser("view", help="To View an existing Template")
        view.add_argument("--id", dest="id", required=True, help="Id for the Template")
        view.set_defaults(func=TemplateCmdLine.view)
        
        #list 
        list = subparsers.add_parser("list", help="To List existing Templates")
        list.add_argument("--per-page", dest="per_page", help="Number of items per page")
        list.add_argument("--page", dest="page", help="Page Number")
        list.set_defaults(func=TemplateCmdLine.list)
        
        #run
        run = subparsers.add_parser("run", help="To Run Template and wait to print Result")
        run.add_argument("--id", dest="id", required=True, help="Id of the template to run")
        run.add_argument("--j", dest="data", required=True, help="Path to JSON file or json string with input field details")
        run.set_defaults(func=TemplateCmdLine.execute)
        
        #submit
        submit = subparsers.add_parser("submit", help="To Submit Template and get the command Id")
        submit.add_argument("--id", dest="id", required=True, help="Id of the template to Submit")
        submit.add_argument("--j", dest="data", required=True, help="Path to JSON file or json string with input field details")
        submit.set_defaults(func=TemplateCmdLine.submit)

        #delete
        delete = subparsers.add_parser("delete",help="Delete a template using the template id")
        delete.add_argument("--id",dest='id',required=True,help="id of the template to be deleted")
        delete.set_defaults(func=TemplateCmdLine.delete)

        return argparser
        
    @staticmethod
    def run(args):      
        parser = TemplateCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)
    
    @staticmethod
    def create(args):
        with open(args.data) as f:
            spec = json.load(f)
        return Template.createTemplate(spec)
    
    @staticmethod
    def edit(args):
        with open(args.data) as f:
            spec = json.load(f)
        return Template.editTemplate(args.id, spec)
    
    @staticmethod
    def clone(args):
        with open(args.data) as f:
            spec = json.load(f)
        id = args.id
        return Template.cloneTemplate(id, spec)
        
    @staticmethod
    def submit(args):
        spec = getSpec(args)
        res = Template.submitTemplate(args.id, spec)
        log.info("Submitted Template  with Id: %s, Command Id: %s, CommandType: %s" % (args.id, res['id'], res['command_type']))
        return res
        
    @staticmethod    
    def execute(args):
        spec = getSpec(args)
        return Template.runTemplate(args.id, spec)
    
    @staticmethod
    def view(args):
        id = args.id
        return Template.viewTemplate(id)
    
    @staticmethod
    def list(args):
        return Template.listTemplates(args)

    @staticmethod
    def delete(args):
        Template.deleteTemplate(args.id)
    
def getSpec(args):
    if args.data is not None:
        if os.path.isfile(args.data):
            with open(args.data) as f:
                spec = json.load(f)
        else:
            spec = json.loads(args.data)
            if 'input_vars' in spec:
                inputs = formatData(spec['input_vars'])
                spec["input_vars"] = inputs
    else:
        spec = {}
    return spec


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
    """
    qds_sdk.Template is the base Qubole Template class.
    
    it uses /command_templates endpoint
    """
    
    rest_entity_path = "command_templates"
    
    @staticmethod
    def createTemplate(data):
        """
        Create a new template.

        Args:
            `data`: json data required for creating a template
        Returns:
            Dictionary containing the details of the template with its ID.
        """
        conn = Qubole.agent()
        return conn.post(Template.rest_entity_path, data)
    
    @staticmethod
    def editTemplate(id, data):
        """
        Edit an existing template.

        Args:
            `id`:   ID of the template to edit
            `data`: json data to be updated
        Returns:
            Dictionary containing the updated details of the template.
        """
        conn = Qubole.agent()
        return conn.put(Template.element_path(id), data)
    
    @staticmethod
    def cloneTemplate(id, data):
        """
        Clone an existing template.

        Args:
            `id`:   ID of the template to be cloned
            `data`: json data to override
        Returns:
            Dictionary containing the updated details of the template.
        """
        conn = Qubole.agent()
        path = str(id) + "/duplicate"
        return conn.post(Template.element_path(path), data)
        
    @staticmethod
    def viewTemplate(id):
        """
        View an existing Template details.

        Args:
            `id`: ID of the template to fetch
        
        Returns:
            Dictionary containing the details of the template.
        """
        conn = Qubole.agent()
        return conn.get(Template.element_path(id))
    
    @staticmethod
    def submitTemplate(id, data):
        """
        Submit an existing Template.

        Args:
            `id`: ID of the template to submit
            `data`: json data containing the input_vars 
        Returns:
            Dictionary containing Command Object details.  
        """
        conn = Qubole.agent()
        path = str(id) + "/run"
        return conn.post(Template.element_path(path), data)
    
    @staticmethod
    def runTemplate(id, data):
        """
        Run an existing Template and waits for the Result.
        Prints result to stdout. 

        Args:
            `id`: ID of the template to run
            `data`: json data containing the input_vars
        
        Returns:  
            An integer as status (0: success, 1: failure)
        """
        conn = Qubole.agent()
        path = str(id) + "/run"
        res = conn.post(Template.element_path(path), data)
        cmdType = res['command_type']
        cmdId = res['id']
        cmdClass = eval(cmdType)
        cmd = cmdClass.find(cmdId)
        while not Command.is_done(cmd.status):
            time.sleep(Qubole.poll_interval)
            cmd = cmdClass.find(cmd.id)
        return Template.getResult(cmdClass, cmd)
    
    @staticmethod
    def getResult(cmdClass, cmd):
        if Command.is_success(cmd.status):
            log.info("Fetching results for %s, Id: %s" % (cmdClass.__name__, cmd.id))
            cmd.get_results(sys.stdout, delim='\t')
            return 0
        else:
            log.error("Cannot fetch results - command Id: %s failed with status: %s" % (cmd.id, cmd.status))
            return 1
    
    @staticmethod
    def listTemplates(args):
        """
        Fetch existing Templates details.

        Args:
            `args`: dictionary containing the value of page number and per-page value
        Returns:
            Dictionary containing paging_info and command_templates details
        """
        conn = Qubole.agent()
        url_path = Template.rest_entity_path
        page_attr = []
        if args.page is not None:
            page_attr.append("page=%s" % args.page)
        if args.per_page is not None:
            page_attr.append("per_page=%s" % args.per_page)
        if page_attr:
            url_path = "%s?%s" % (url_path, "&".join(page_attr))

        return conn.get(url_path)

    @staticmethod
    def deleteTemplate(id):
        path = Template.element_path(id) + "/remove"
        conn = Qubole.agent()
        conn.put(path)
