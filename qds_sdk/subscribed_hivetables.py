__author__ = 'avinashj'


"""
The subscribed hivetables module contains the definitions for basic CRUD operations on SubscribedHivetable
"""

from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource
from argparse import ArgumentParser

import logging
import json

log = logging.getLogger("qds_published_hivetable")


class SubscribedHivetableCmdLine:
    """
    qds_sdk.PubkishedHivetableCmdLine is the interface used a qds.py
    """

    @staticmethod
    def run(args):
        parser = SubscribedHivetableCmdLine.parsers()
        parsed = parser.parse_args(args)
        return parsed.func(parsed)