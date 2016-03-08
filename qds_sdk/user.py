from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
import argparse
import json


class UserCmdLine:
    @staticmethod
    def parsers():
        argparser = argparse.ArgumentParser(
            prog="qds.py user",
            description="User level operations for Qubole Data Service.")
        subparsers = argparser.add_subparsers(title="user operations")

        # To invite a new user
        invite = subparsers.add_parser(
            "invite", help="Invite a new user")
        invite.add_argument(
            "--email", dest="invitee_email", required=True,
            help="Email address of the new user to be added to the Qubole account")
        invite.add_argument(
            "--account-id", dest="account", required=True,
            help="Account ID of the current user")
        invite.add_argument(
            "--groups", dest="groups", required=False,
            help="Add groups to the new user, defaults to system-user")
        invite.set_defaults(func=UserCmdLine.invite)

        # enable qbol user
        enable = subparsers.add_parser(
            "enable", help="Enable a qbol user")
        enable.add_argument(
            "--qbol-user-id", dest="qbol_user_id", required=True,
            help="ID of the qbol_user, who should be enabled")
        enable.set_defaults(func=UserCmdLine.enable)

        # disable qbol user
        disable = subparsers.add_parser(
            "disable", help="Disable a qbol user")
        disable.add_argument(
            "--qbol-user-id", dest="qbol_user_id", required=True,
            help="ID of the qbol_user, who should be disabled")
        disable.set_defaults(func=UserCmdLine.disable)

        return argparser

    @staticmethod
    def run(args):
        parser = UserCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)

    @staticmethod
    def invite(args):
        data = vars(args)
        data.pop("func")
        result = User.invite("invite_new", data)
        return json.dumps(result, indent=4)

    @staticmethod
    def enable(args):
        data = vars(args)
        data.pop("func")
        result = Accounts.enable_disable("enable_qbol_user", data)
        return json.dumps(result, indent=4)

    @staticmethod
    def disable(args):
        data = vars(args)
        data.pop("func")
        result = Accounts.enable_disable("disable_qbol_user", data)
        return json.dumps(result, indent=4)


class User(Resource):

    rest_entity_path = "users"

    @classmethod
    def invite(cls, path, data):
        conn = Qubole.agent()
        return conn.post(cls.element_path(path), data)


class Accounts(Resource):
    rest_entity_path = "accounts"

    @classmethod
    def enable_disable(cls, path, data):
        conn = Qubole.agent()
        return conn.post(cls.element_path(path), data)
