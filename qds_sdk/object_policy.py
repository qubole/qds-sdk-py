"""
Deals with accessing the granular level policy for a particular
resource or object.
"""

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser
import json

class ObjectPolicy(Resource):
    """
    qds_sdk.ObjectPolicy is the class which sets/updates the policy
    """
    rest_entity_path = "object_policy"
    api_version = "latest"

    @classmethod
    def _parse_object_policy(cls, args, action, object):
        """
        Args:
            `args`: sequence of arguments
            `action` : "get_object_policy" or "update_object_policy"

        Returns:
            Dictionary that contains parsed command line arguments.
        """
        argsParser = ArgumentParser(prog="%s %s" % (object, action))
        argsParser.set_defaults(source_type=object)
        subparsers = argsParser.add_subparsers()
        get_parser = subparsers.add_parser("get")
        get_parser.set_defaults(cmd="get")
        update_parser = subparsers.add_parser("update")
        update_parser.set_defaults(cmd="update")
        for parser in [get_parser, update_parser]:
            parser.add_argument("--%s-id" % (object.lower()), help="Id of the %s." % (object.lower()), required=True,
                                dest='id')

        update_parser.add_argument("--policy", required = True,
                                    help="Policy Statement example '[{\"access\":\"deny\", \"action\":"
                                         " [\"all\"], \"condition\": {\"qbol_users\": [user_id,], \"qbol_groups\": [group_ids,]} }]")
        arguments = argsParser.parse_args(args)
        return arguments

    @classmethod
    def process_object_policy(cls, arguments):
        if arguments.cmd == "get":
            return cls.get_object_policy(arguments.source_type, arguments.id)
        if arguments.cmd == "update":
            return cls.update_object_policy(arguments.source_type, arguments.id, arguments.policy)

    @classmethod
    def get_object_policy(cls, source_type, source_id):
        """
        View access control for a cluster.
        """
        conn = Qubole.agent(version=cls.api_version)
        data = {"source_id": source_id, "source_type": source_type}
        return conn.get(cls.element_path("get_object_policy"), data)

    @classmethod
    def update_object_policy(cls, source_type, source_id, policy):
        """
        Update/Set access control for a cluster.
        """
        conn = Qubole.agent(version=cls.api_version)
        data = {"source_id": source_id, "source_type": source_type,
                "policy": policy}
        return conn.put(cls.element_path("update_object_policy"), data)
