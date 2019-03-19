import json

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser
from qds_sdk.commands import *

class DbTapCmdLine:
    """
    qds_sdk.ScheduleCmdLine is the interface used by qds.py.
    """

    @staticmethod
    def parsers():
        argparser = ArgumentParser(prog="qds.py dbtaps",
                                        description="DbTaps client for Qubole Data Service.")
        subparsers = argparser.add_subparsers()

        #Create
        create = subparsers.add_parser("create",
                                       help="Create a new schedule")
        create.add_argument("--name", dest="name", required=True,
                            help="Database Name")
        create.add_argument("--host", dest="host", required=True,
                            help="Host name or IP address of the database")
        create.add_argument("--user", dest="user", required=True,
                            help="Username")
        create.add_argument("--password", dest="password", required=True,
                            help="Password")
        create.add_argument("--port", dest="port",
                            help="Database Port")
        create.add_argument("--type", dest="type", choices=["mysql","vertica","mongo","postgresql","redshift","sqlserver"],
                            help="Type of database")
        create.add_argument("--location", dest="location", choices=["us-east-1", "us-west-2", "ap-southeast-1", "eu-west-1", "on-premise"],
                            help="Type of database", required=True)
        create.set_defaults(func=DbTapCmdLine.create)

        #List
        list = subparsers.add_parser("list",
                                     help="List all DbTaps")
        list.add_argument("--fields", nargs="*", dest="fields",
                          help="List of fields to show")
        list.add_argument("--per-page", dest="per_page",
                          help="Number of items per page")
        list.add_argument("--page", dest="page",
                          help="Page Number")
        list.set_defaults(func=DbTapCmdLine.list)

        #View
        view = subparsers.add_parser("view",
                                     help="View a specific DbTap")
        view.add_argument("id", help="Numeric id or name of the DbTap")
        view.add_argument("--fields", nargs="*", dest="fields",
                          help="List of fields to show")
        view.set_defaults(func=DbTapCmdLine.view)

        #Tables
        tables = subparsers.add_parser("tables",
                                        help="List tables in a DbTap")
        tables.add_argument("id", help="Numeric id of the DbTap")
        tables.set_defaults(func=DbTapCmdLine.tables)

        #Edit
        edit = subparsers.add_parser("edit",
                                       help="Edit a DbTap")
        edit.add_argument("id", help="Numeric id of the schedule")
        edit.add_argument("--name", dest="name",
                            help="Database Name")
        edit.add_argument("--host", dest="host",
                            help="Host name or IP address of the database")
        edit.add_argument("--user", dest="user",
                            help="Username")
        edit.add_argument("--password", dest="password",
                            help="Password")
        edit.add_argument("--port", dest="port", help="Database Port")
        edit.add_argument("--type", dest="type", choices=["mysql","vertica","mongo","postgresql","redshift","sqlserver"],
                            help="Type of database")
        edit.add_argument("--location", dest="location", choices=["us-east-1", "us-west-2", "ap-southeast-1", "eu-west-1", "on-premise"],
                            help="Type of database")
        edit.set_defaults(func=DbTapCmdLine.edit)

        #Kill
        kill = subparsers.add_parser("delete",
                                     help="Delete a DbTap")
        kill.add_argument("id", help="Numeric id of the schedule")
        kill.set_defaults(func=DbTapCmdLine.kill)
        return argparser

    @staticmethod
    def run(args):
        parser = DbTapCmdLine.parsers()
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
        dbtap = DbTap.create(db_name=args.name,
                             db_host=args.host,
                             db_user=args.user,
                             db_passwd=args.password,
                             db_type=args.type,
                             db_location=args.location,
                             port=args.port)

        return json.dumps(dbtap.attributes, sort_keys=True, indent=4)

    @staticmethod
    def list(args):
        taplist = DbTap.list(args.page, args.per_page)
        if args.fields:
            for s in taplist:
                s.attributes = DbTapCmdLine.filter_fields(s.attributes, args.fields)
        return json.dumps(taplist, default=lambda o: o.attributes, sort_keys=True, indent=4)

    @staticmethod
    def view(args):
        tap = DbTap.find(args.id)
        if args.fields:
            tap.attributes = DbTapCmdLine.filter_fields(tap.attributes, args.fields)
        return json.dumps(tap.attributes, sort_keys=True, indent=4)

    @staticmethod
    def tables(args):
        tap = DbTap.find(args.id)
        return json.dumps(tap.tables(), sort_keys=True, indent=4)

    @staticmethod
    def edit(args):
        tap = DbTap.find(args.id)
        """ Carefully setup a dict """
        options = {}
        if not args.name is None:
            options["db_name"]=args.name
        if args.host is not None:
            options["db_host"]=args.host
        if args.user is not None:
            options["db_user"]=args.user
        if args.password is not None:
            options["db_passwd"] = args.password
        if args.type is not None:
            options["db_type"] = args.type
        if args.location is not None:
            options["db_location"] = args.location
        if args.port is not None:
            options["port"] = args.port
        tap = tap.edit(**options)
        return json.dumps(tap.attributes, sort_keys=True, indent=4)

    @staticmethod
    def kill(args):
        tap = DbTap.find(args.id)
        return json.dumps(tap.delete(), sort_keys=True, indent=4)

class DbTap(Resource):
    """
    qds_sdk.Dbtap is the base Qubole DbTap class.
    """

    """ all commands use the /db_taps endpoint"""

    rest_entity_path = "db_taps"

    @staticmethod
    def list(page = None, per_page = None):
        conn = Qubole.agent()
        url_path = DbTap.rest_entity_path
        page_attr = []
        if page is not None:
            page_attr.append("page=%s" % page)
        if per_page is not None:
            page_attr.append("per_page=%s" % per_page)
        if page_attr:
            url_path = "%s?%s" % (DbTap.rest_entity_path, "&".join(page_attr))

        #Todo Page numbers are thrown away right now
        tapjson = conn.get(url_path)
        taplist = []
        for s in tapjson["db_taps"]:
            taplist.append(DbTap(s))
        return taplist

    def tables(self):
        conn = Qubole.agent()
        return conn.get("%s/tables" % self.element_path(self.id))

    def edit(self, **kwargs):
        conn = Qubole.agent()
        return DbTap(conn.put(self.element_path(self.id), data=kwargs))

    def delete(self):
        conn = Qubole.agent()
        return conn.delete(self.element_path(self.id))
