import json

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser

class GroupCmdLine:
    """
    qds_sdk.GroupCmdLine is the interface used by qds.py.
    """

    @staticmethod
    def parsers():
        argparser = ArgumentParser(prog="qds.py group",
                                        description="Client to create groups in Qubole Data Service.")
        subparsers = argparser.add_subparsers()

        #Create
        create = subparsers.add_parser("create", help="Create a new Group")
        create.add_argument("--name", dest="name", required=True,
                            help="Name of group")
        create.set_defaults(func=GroupCmdLine.create)

        #List
        list = subparsers.add_parser("list", help="List all groups")
        list.add_argument("--per-page", dest="per_page",
                          help="Number of items per page")
        list.add_argument("--page", dest="page",
                          help="Page Number")
        list.set_defaults(func=GroupCmdLine.list)

        #View
        view = subparsers.add_parser("view",
                                     help="View a specific group")
        view.add_argument("id", help="Numeric id or name of the group")
        view.set_defaults(func=GroupCmdLine.view)

        #duplicate
        duplicate = subparsers.add_parser("duplicate",
                                        help="Duplicates/clone a group")
        duplicate.add_argument("id", help="Numeric id of the group")
        duplicate.add_argument("--name", dest="name", required=False,
                            help="Name of group")
        duplicate.set_defaults(func=GroupCmdLine.duplicate)
        
        #Add user
        add_user = subparsers.add_parser("add_user",
                                        help="add users to a group")
        add_user.add_argument("id", help="Numeric id of the group")
        add_user.add_argument("--user_id", dest="user_id", required=True,
help="user Id")
        add_user.set_defaults(func=GroupCmdLine.add_user)

         #remove user
        remove_user = subparsers.add_parser("remove_user",
                                        help="remove users from a group")
        remove_user.add_argument("id", help="Numeric id of the group")
        remove_user.add_argument("--user_id", dest="user_id", required=True,
 help="user Id")
        remove_user.set_defaults(func=GroupCmdLine.remove_user)

         #List roles for a group
        list_roles = subparsers.add_parser("list_roles",
                                        help="List all roles for a group ")
        list_roles.add_argument("id", help="Numeric id of the group")
        list_roles.set_defaults(func=GroupCmdLine.list_roles)

         #List users for a group
        list_users = subparsers.add_parser("list_users",
                                        help="List all users in a group ")
        list_users.add_argument("id", help="Numeric id of the group")
        list_users.set_defaults(func=GroupCmdLine.list_users)

        return argparser

    @staticmethod
    def run(args):
        parser = GroupCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)

    @staticmethod
    def create(args):
        group = Group.create(name=args.name) 
        return json.dumps(group.attributes, sort_keys=True, indent=4)

    @staticmethod
    def list(args):
        grouplist = Group.list(args.page, args.per_page)
        return json.dumps(grouplist, default=lambda o: o.attributes, sort_keys=True, indent=4)

    @staticmethod
    def view(args):
        group = Group.find(args.id)
        return json.dumps(group.attributes, sort_keys=True, indent=4)

    @staticmethod
    def add_user(args):
        group = Group.find(args.id)
        return group.add_user(args.user_id)

    @staticmethod
    def remove_user(args):
        group = Group.find(args.id)
        return group.remove_user(args.user_id)

    @staticmethod
    def duplicate(args):
        group = Group.find(args.id) 
        options = {}
        if args.name is not None:
          options["name"] = args.name
        return json.dumps(group.duplicate(**options), sort_keys=True, indent=4)

    @staticmethod
    def list_roles(args):
        group = Group.find(args.id)
        return json.dumps(group.list_roles(), sort_keys=True, indent=4)

    @staticmethod
    def list_users(args):
        group = Group.find(args.id)
        return json.dumps(group.list_users(), sort_keys=True, indent=4)



class Group(Resource):
    """
    qds_sdk.Group is the base Qubole Group class.
    """

    """ all commands use the /group endpoint"""

    rest_entity_path = "groups"

    @staticmethod
    def list(page = None, per_page = None):
        conn = Qubole.agent()
        url_path = Group.rest_entity_path
        page_attr = []
        if page is not None:
            page_attr.append("page=%s" % page)
        if per_page is not None:
            page_attr.append("per_page=%s" % per_page)
        if page_attr:
            url_path = "%s?%s" % (Group.rest_entity_path, "&".join(page_attr))

        #Todo Page numbers are thrown away right now
        groupjson = conn.get(url_path)
        grouplist = []
        for s in groupjson["groups"]:
            grouplist.append(Group(s))
        return grouplist

    def add_user(self, user_id):
        conn = Qubole.agent()
        url_path = "groups/%s/qbol_users/%s/add" % (self.groups["id"], user_id)
        return conn.put(url_path)

    def remove_user(self, user_id):
        conn = Qubole.agent()
        url_path = "groups/%s/qbol_users/%s/remove" % (self.groups["id"], user_id)
        return conn.put(url_path)

    def duplicate(self, **kwargs):
        conn = Qubole.agent()
        url_path = "groups/%s/duplicate" % self.groups["id"]
        return conn.post(url_path, data=kwargs)

    def list_roles(self):
        conn = Qubole.agent()
        url_path = "groups/%s/roles" % self.groups["id"]
        return conn.get(url_path)

    def list_users(self):
        conn = Qubole.agent()
        url_path = "groups/%s/qbol_users" % self.groups["id"]
        return conn.get(url_path)

