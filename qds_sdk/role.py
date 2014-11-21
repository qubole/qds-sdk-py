import json

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser

class RoleCmdLine:
    """
    qds_sdk.RoleCmdLine is the interface used by qds.py.
    """

    @staticmethod
    def parsers():
        argparser = ArgumentParser(prog="qds.py role",
                                        description="Client to create roles in Qubole Data Service.")
        subparsers = argparser.add_subparsers()

        #Create
        create = subparsers.add_parser("create",
                                       help="Create a new Role")
        create.add_argument("--name", dest="name", required=True,
                            help="Name of role")
        create.add_argument("--policy", dest="policy", required=True,
             help="Policy Statement example '[{\"access\":\"allow\", \"resource\": \"all\"}]'")
        create.set_defaults(func=RoleCmdLine.create)

        #List
        list = subparsers.add_parser("list",
                                     help="List all roles")
        list.add_argument("--per-page", dest="per_page",
                          help="Number of items per page")
        list.add_argument("--page", dest="page",
                          help="Page Number")
        list.set_defaults(func=RoleCmdLine.list)

        #View
        view = subparsers.add_parser("view",
                                     help="View a specific role")
        view.add_argument("id", help="Numeric id or Name of the role")
        view.set_defaults(func=RoleCmdLine.view)

        #update
        update = subparsers.add_parser("update",
                                        help="update a specific role")
        update.add_argument("id", help="Numeric id of the role")
        update.add_argument("--name", dest="name", help="Name of role")
        update.add_argument("--policy", dest="policy",
                            help="Policy Statement")
        update.set_defaults(func=RoleCmdLine.update)

        #duplicate
        duplicate = subparsers.add_parser("duplicate",
                                        help="Duplicates/clone a role")
        duplicate.add_argument("id", help="Numeric id of the role")
        duplicate.add_argument("--name", dest="name", required=False,
                            help="Name of the new role")
        duplicate.add_argument("--policy", dest="policy", required=False,
             help="Optional policy Statement example '[{\"access\":\"allow\", \"resource\": \"all\"}]'")
        duplicate.set_defaults(func=RoleCmdLine.duplicate)

        #Assign Role
        assign_role = subparsers.add_parser("assign_role",
                                        help="assign a role to a group")
        assign_role.add_argument("id", help="Numeric id of the role")
        assign_role.add_argument("--group_id", dest="group_id", required=True,
help="Numeric Id of the group")
        assign_role.set_defaults(func=RoleCmdLine.assign_role)

         #UnAssign Role
        unassign_role = subparsers.add_parser("unassign_role",
                                        help="unassign a role to a group")
        unassign_role.add_argument("id", help="Numeric id of the role")
        unassign_role.add_argument("--group_id", dest="group_id", required=True, help="Numeric Id of the group")
        unassign_role.set_defaults(func=RoleCmdLine.unassign_role)

        #List groups
        list_groups = subparsers.add_parser("list_groups",
                                        help="List all groups for a role ")
        list_groups.add_argument("id", help="Numeric id of the role")
        list_groups.set_defaults(func=RoleCmdLine.list_groups)

        return argparser

    @staticmethod
    def run(args):
        parser = RoleCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)

    @staticmethod
    def create(args):
        role = Role.create(name=args.name, policy=args.policy) 
        return json.dumps(role.attributes, sort_keys=True, indent=4)

    @staticmethod
    def list(args):
        rolelist = Role.list(args.page, args.per_page)
        return json.dumps(rolelist, default=lambda o: o.attributes, sort_keys=True, indent=4)

    @staticmethod
    def view(args):
        role = Role.find(args.id)
        return json.dumps(role.attributes, sort_keys=True, indent=4)

    @staticmethod
    def update(args):
        options = {}
        if args.name is not None:
          options["name"] = args.name
        if args.policy is not None:
          options["policy"] = args.policy
        return json.dumps(Role.update(args.id, **options), sort_keys=True, indent=4)

    @staticmethod
    def duplicate(args):
        options = {}
        if args.name is not None:
          options["name"] = args.name
        if args.policy is not None:
          options["policy"] = args.policy
        return json.dumps(Role.duplicate(args.id, **options), sort_keys=True, indent=4)

    @staticmethod
    def assign_role(args):
        return Role.assign_role(args.id, args.group_id)

    @staticmethod
    def unassign_role(args):
        return Role.unassign_role(args.id, args.group_id)

    @staticmethod
    def list_groups(args):
        return json.dumps(Role.list_groups(args.id), sort_keys=True, indent=4)

class Role(Resource):
    """
    qds_sdk.Role is the base Qubole Role class.
    """

    """ all commands use the /role endpoint"""

    rest_entity_path = "roles"

    @staticmethod
    def list(page = None, per_page = None):
        conn = Qubole.agent()
        url_path = Role.rest_entity_path
        page_attr = []
        if page is not None:
            page_attr.append("page=%s" % page)
        if per_page is not None:
            page_attr.append("per_page=%s" % per_page)
        if page_attr:
            url_path = "%s?%s" % (Role.rest_entity_path, "&".join(page_attr))

        #Todo Page numbers are thrown away right now
        rolejson = conn.get(url_path)
        rolelist = []
        for s in rolejson["roles"]:
            rolelist.append(Role(s))
        return rolelist

    @staticmethod
    def update(role_id, **kwargs):
        conn = Qubole.agent()
        url_path = "roles/%s" % role_id
        return conn.put(url_path, data=kwargs)

    @staticmethod
    def duplicate(role_id, **kwargs):
        conn = Qubole.agent()
        url_path = "roles/%s/duplicate" % role_id
        return conn.post(url_path, data=kwargs)

    @staticmethod
    def assign_role(role_id, qbol_group_id):
        conn = Qubole.agent()
        url_path = "groups/%s/roles/%s/assign" % (qbol_group_id, role_id)
        return conn.put(url_path)

    @staticmethod
    def unassign_role(role_id, qbol_group_id):
        conn = Qubole.agent()
        url_path = "groups/%s/roles/%s/unassign" % (qbol_group_id, role_id)
        return conn.put(url_path)

    @staticmethod
    def list_groups(role_id):
        conn = Qubole.agent()
        url_path = "roles/%s/groups" % role_id
        return conn.get(url_path)


