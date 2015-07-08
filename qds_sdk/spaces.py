"""
The space module contains the definitions for basic CRUD operations on Space
"""

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser

import logging
import json

log = logging.getLogger("qds_space")


class SpaceCmdLine:
    """
    qds_sdk.SpaceCmdLine is the interface used a qds.py
    """

    @staticmethod
    def parsers():
        """
        Parse command line arguments to construct a dictionary of space
        parameters that can be used to create a space.

        Args:
            `args`: sequence of arguments

        Returns:
            Dictionary that can be used to create a space
        """
        argparser = ArgumentParser(prog="qds.py spaces",
                                        description="Spaces client for Qubole Data Service.")
        subparsers = argparser.add_subparsers()

        # Create
        create = subparsers.add_parser("create",
                                       help="Create a new space")
        create.add_argument("--name", dest="name",
                            help="create space with the this name")
        create.add_argument("--uri", dest="uri",
                            help="create space on this path")
        create.add_argument("--acl", dest="acl", default="private",
                            help="create space with this acl")
        create.set_defaults(func=SpaceCmdLine.create)

        # List
        list = subparsers.add_parser("list",
                                     help="List all spaces of the given account")
        list.set_defaults(func=SpaceCmdLine.list)

        # View
        view = subparsers.add_parser("view",
                                     help="View a specific Space")
        view.add_argument("id",
                          help="Numeric id of the Space")
        view.set_defaults(func=SpaceCmdLine.view)

        # Hivetables
        hivetables = subparsers.add_parser("hivetables",
                                           help="Get all hivetables published inside a space")
        hivetables.add_argument("id",
                          help="Numeric id of the Space")
        hivetables.set_defaults(func=SpaceCmdLine.hivetables)
        return argparser

    @staticmethod
    def run(args):
        parser = SpaceCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)

    @staticmethod
    def create(args):
        space = Space.create(name=args.name,
                                 uri=args.uri,
                                 acl=args.acl)

        return json.dumps(space.attributes, sort_keys=True, indent=4)

    @staticmethod
    def list(args):
        space_list = Space.list()
        return json.dumps(space_list, sort_keys=True, indent=4)

    @staticmethod
    def view(args):
        tap = Space.find(args.id)
        return json.dumps(tap.attributes, sort_keys=True, indent=4)

    @staticmethod
    def hivetables(args):
        tap = Space.hivetables(args.id)
        return json.dumps(tap, sort_keys=True, indent=4)


class Space(Resource):
    """
    qds_sdk.Space is the base Qubole Space class.
    """

    """ all commands use the /space endpoint"""
    rest_entity_path = "spaces"

    @staticmethod
    def list():
        conn = Qubole.agent()
        url_path = Space.rest_entity_path
        return conn.get(url_path)

    @staticmethod
    def hivetables(id):
        conn = Qubole.agent()
        url_path = Space.rest_entity_path + "/" + str(id) + "/hivetables"
        return conn.get(url_path)