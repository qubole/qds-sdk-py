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


class SensorCmdLine:

    @staticmethod
    def check(sensor_class, args):
        """
        Method to call Sensor.check after parsing args from cmdline
        :param sensor_class: sensor class
        :param args: inline arguments
        :return: True or False
        """
        parser = SensorCmdLine.parsers(sensor_class)
        parsed = parser.parse_args(args)
        return sensor_class.check(json.loads(parsed.data))

    @staticmethod
    def parsers(sensor_class):
        argparser = ArgumentParser(prog=sensor_class.usage, description=sensor_class.description)
        subparsers = argparser.add_subparsers()

        #Check
        check = subparsers.add_parser("check", help="Check a Sensor")
        check.add_argument("-d", "--data", dest="data", required=True,
                           help="String containing a valid json object")
        check.set_defaults(func=Sensor.check)
        return argparser


class Sensor(Resource):
    """
    qds_sdk.Sensor is the base Qubole sensor class. Different types of Qubole
    sensors can subclass this.
    """

    @classmethod
    def check(cls, data):
        """
        Method to call the sensors api with json payload
        :param data: valid json object
        :return: True or False
        """
        conn = Qubole.agent()
        return conn.post(cls.rest_entity_path, data=data)['status']


class FileSensor(Sensor):
    rest_entity_path = "sensors/file_sensor"

    usage = ("qds.py filesensor check -d 'json string'")
    description = "File Sensor client for Qubole Data Services"


class PartitionSensor(Sensor):
    rest_entity_path = "sensors/partition_sensor"

    usage = ("qds.py partitionsensor check -d 'json string'")
    description = "Hive Partition Sensor client for Qubole Data Services"



