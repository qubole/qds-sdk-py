"""
The cluster module contains the definitions for retrieving hadoop cluster
information from Qubole
"""

from qubole import Qubole
from resource import Resource
from exception import ParseError

import logging
import sys
import os

log = logging.getLogger("qds_cluster")

class Cluster(Resource):
  """qds_sdk.Cluster is the class for retrieving hadoop cluster information.
  """

  rest_entity_path = "hadoop_cluster"

  @classmethod
  def get(cls):
    conn = Qubole.agent()

    return (conn.get(cls.rest_entity_path))
