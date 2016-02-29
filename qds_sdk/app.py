"""
The app module contains methods for managing apps.
"""
from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
import argparse
import json


class AppCmdLine:
    """
    qds_sdk.AppCmdLine is the interface used by qds.py.
    """

    @staticmethod
    def parsers():
        argparser = argparse.ArgumentParser(
            prog="qds.py app",
            description="Client for managing apps on Qubole Data Service.")
        subparsers = argparser.add_subparsers(title="app operations")

        # Show information about an app
        show = subparsers.add_parser(
            "show", help="Show information about an app")
        show.add_argument("id", type=int, help="Numeric id of the app")
        show.set_defaults(func=AppCmdLine.show)

        # For listing information about all apps
        index = subparsers.add_parser(
            "list", help="List all available apps")
        index.set_defaults(func=AppCmdLine.index)

        def check_pair(pair):
            try:
                key, value = pair.split("=")
            except Exception:
                raise argparse.ArgumentTypeError(
                    "%s is an invalid key=value pair." % pair)
            return key, value

        # Create a new app
        create = subparsers.add_parser("create", help="Create a new app")
        create.add_argument("--name", required=True,
                            help="The name for the app")
        create.add_argument("--kind", default="spark", choices=['spark'],
                            help="The kind of the app. Default is spark")
        create.add_argument(
            "--config", nargs="*", type=check_pair,
            help="""Specify the config you want to set for this app as
            key=value pairs separated by spaces""")
        create.set_defaults(func=AppCmdLine.create)

        # Stop an app
        stop = subparsers.add_parser("stop", help="Stop an app")
        stop.add_argument("id", type=int, help="Numeric id of the app")
        stop.set_defaults(func=AppCmdLine.stop)

        # Delete an app
        delete = subparsers.add_parser("delete", help="Delete an app")
        delete.add_argument("id", type=int, help="Numeric id of the app")
        delete.set_defaults(func=AppCmdLine.delete)

        return argparser

    @staticmethod
    def run(args):
        parser = AppCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)

    @staticmethod
    def show(args):
        app = App.show(args.id)
        return json.dumps(app, indent=4)

    @staticmethod
    def index(args):
        result = App.index()
        return json.dumps(result, indent=4)

    @staticmethod
    def create(args):
        config = {}
        if args.config is not None:
            for key, value in args.config:
                config[key] = value
        result = App.create(name=args.name, config=config, kind=args.kind)
        return json.dumps(result, indent=4)

    @staticmethod
    def stop(args):
        result = App.stop(args.id)
        return json.dumps(result, indent=4)

    @staticmethod
    def delete(args):
        result = App.delete(args.id)
        return json.dumps(result, indent=4)


class App(Resource):
    """
    qds_sdk.App is the base Qubole Apps class.
    """

    """all apps use the /apps endpoint"""
    rest_entity_path = "apps"

    @classmethod
    def show(cls, app_id):
        """
        Shows an app by issuing a GET request to the /apps/ID endpoint.
        """
        conn = Qubole.agent()
        return conn.get(cls.element_path(app_id))

    @classmethod
    def index(cls):
        """
        Shows a list of all available apps by issuing a GET request to the
        /apps endpoint.
        """
        conn = Qubole.agent()
        return conn.get(cls.rest_entity_path)

    @classmethod
    def create(cls, name, config=None, kind="spark"):
        """
        Create a new app.

        Args:
            `name`: the name of the app

            `config`: a dictionary of key-value pairs

            `kind`: kind of the app (default=spark)
        """
        conn = Qubole.agent()
        return conn.post(cls.rest_entity_path,
                         data={'name': name, 'config': config, 'kind': kind})

    @classmethod
    def stop(cls, app_id):
        """
        Stops an app by issuing a PUT request to the /apps/ID/stop endpoint.
        """
        conn = Qubole.agent()
        return conn.put(cls.element_path(app_id) + "/stop")

    @classmethod
    def delete(cls, app_id):
        """
        Delete an app by issuing a DELETE request to the /apps/ID endpoint.
        """
        conn = Qubole.agent()
        return conn.delete(cls.element_path(app_id))
