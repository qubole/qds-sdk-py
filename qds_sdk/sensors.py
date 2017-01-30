"""
The sensors module contains the base definition for a generic
sensor call and the implementation of all the specific sensors
"""

from __future__ import print_function
from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser

import logging
import json

log = logging.getLogger("qds_sensors")


class Sensor(Resource):
    """
    qds_sdk.Sensor is the base Qubole sensor class. Different types of Qubole
    sensors can subclass this.
    """

    @classmethod
    def check(cls, data):
        conn = Qubole.agent()
        return conn.post(cls.rest_entity_path, data=data)['status']

    @classmethod
    def check_cli(cls, args):
        parser = cls.parsers()
        parsed = parser.parse_args(args)
        conn = Qubole.agent()
        return conn.post(cls.rest_entity_path, data=json.loads(parsed.data))['status']

    @classmethod
    def parsers(cls):
        argparser = ArgumentParser(prog=cls.usage, description=cls.description)
        subparsers = argparser.add_subparsers()

        #Check
        check = subparsers.add_parser("check", help="Check a Sensor")
        check.add_argument("-d", "--data", dest="data", required=True,
                           help="String containing a valid json object")
        check.set_defaults(func=Sensor.check)
        return argparser


class FileSensor(Sensor):
    rest_entity_path = "sensors/file_sensor"

    usage = ("qds.py filesensor check -d 'json string'")
    description = "File Sensor client for Qubole Data Services"


class PartitionSensor(Sensor):
    rest_entity_path = "sensors/partition_sensor"

    usage = ("qds.py partitionsensor check -d 'json string'")
    description = "Hive Partition Sensor client for Qubole Data Services"



