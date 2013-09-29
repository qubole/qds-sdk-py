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

class HadoopCluster(Resource):
  """qds_sdk.HadoopCluster is the class for retrieving hadoop cluster 
     information.
  """

  rest_entity_path = "hadoop_cluster"

  @classmethod
  def find(cls, name="default", **kwargs):
    if ((name is None) or (name == "default")):
      conn = Qubole.agent()
      return cls(conn.get(cls.rest_entity_path))
    else:
      raise ParseError("Bad name 'default'",
                       "Hadoop Clusters can only be named 'default' currently")
