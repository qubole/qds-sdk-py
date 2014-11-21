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
                                   description="Client to Manage Roles in Qubole Data Service.")
        subparsers = argparser.add_subparsers()

        # Create
        create = subparsers.add_parser("create", help="Create a new Role")
        create.add_argument("--name", dest="name", required=True, help="Name of the new Role")
        create.add_argument("--policy", dest="policy", required=True,
                            help="Policy Statement example '[{\"access\":\"deny\", \"resource\": \"all\", \"action\": \"[\"create\",\"update\",\"delete\"\]\"}]'")
        create.set_defaults(func=RoleCmdLine.create)

        #List
        list = subparsers.add_parser("list", help="List all Roles")
        list.add_argument("--per-page", dest="per_page", help="Number of items per page")
        list.add_argument("--page", dest="page", help="Page Number")
        list.set_defaults(func=RoleCmdLine.list)

        #View
        view = subparsers.add_parser("view", help="View a specific Role")
        view.add_argument("id", help="Numeric id of the Role")
        view.set_defaults(func=RoleCmdLine.view)

        #update
        update = subparsers.add_parser("update", help="Update a specific Role's name or policy")
        update.add_argument("id", help="Numeric id of the Role")
        update.add_argument("--name", dest="name", help="New name of the Role")
        update.add_argument("--policy", dest="policy",
                            help="Policy Statement example '[{\"access\":\"deny\", \"resource\": \"all\", \"action\": \"[\"create\",\"update\",\"delete\"\]\"}]'")
        update.set_defaults(func=RoleCmdLine.update)

        #Delete
        delete = subparsers.add_parser("delete", help="Delete a Role")
        delete.add_argument("id", help="Numeric id of the Role to be deleted")
        delete.set_defaults(func=RoleCmdLine.delete)

        #duplicate
        duplicate = subparsers.add_parser("duplicate", help="Duplicates/Clones a Role")
        duplicate.add_argument("id", help="Numeric id of the Role to be cloned")
        duplicate.add_argument("--name", dest="name", required=False, help="Name of the new Role")
        duplicate.add_argument("--policy", dest="policy", required=False,
                               help="Optional policy Statement example '[{\"access\":\"allow\", \"resource\": \"all\"}]'")
        duplicate.set_defaults(func=RoleCmdLine.duplicate)

        #Assign Role
        assign_role = subparsers.add_parser("assign-role", help="Assigns a Role to a Group")
        assign_role.add_argument("id", help="Numeric id of the Role")
        assign_role.add_argument("--group-id", dest="group_id", required=True, help="Numeric Id of the Group")
        assign_role.set_defaults(func=RoleCmdLine.assign_role)

        #UnAssign Role
        unassign_role = subparsers.add_parser("unassign-role", help="Unassigna a Role from a Group")
        unassign_role.add_argument("id", help="Numeric id of the Role")
        unassign_role.add_argument("--group-id", dest="group_id", required=True, help="Numeric Id of the Group")
        unassign_role.set_defaults(func=RoleCmdLine.unassign_role)

        #List groups
        list_groups = subparsers.add_parser("list-groups", help="List all Groups for a Role ")
        list_groups.add_argument("id", help="Numeric id of the Role")
        list_groups.set_defaults(func=RoleCmdLine.list_groups)

        return argparser

    @staticmethod
    def run(args):
        parser = RoleCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)

    @staticmethod
    def create(args):
        role = Role.create(name=args.name, policies=args.policy)
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
            options["policies"] = args.policy
        return json.dumps(Role.update(args.id, **options), sort_keys=True, indent=4)

    @staticmethod
    def delete(args):
        return json.dumps(Role.delete(args.id), sort_keys=True, indent=4)

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
    def list(page=None, per_page=None):
        conn = Qubole.agent()
        url_path = Role.rest_entity_path
        page_attr = []
        if page is not None:
            page_attr.append("page=%s" % page)
        if per_page is not None:
            page_attr.append("per_page=%s" % per_page)
        if page_attr:
            url_path = "%s?%s" % (Role.rest_entity_path, "&".join(page_attr))

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
    def delete(role_id):
        conn = Qubole.agent()
        url_path = "roles/%s" % role_id
        return conn.delete(url_path)

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


