#!/bin/env python

"""
Trivial example for using the Qubole SDK to run a Hadoop Streaming Command.
See: http://www.qubole.com/documentation/en/latest/quick-start-guide/running-hadoop-job.html
"""


from qds_sdk.qubole import Qubole
from qds_sdk.commands import HadoopCommand
import qds_sdk.exception

import sys
import traceback
import logging
import shlex

log = logging.getLogger("mr_1")

usage_str = ("Usage: mr_1 <api_token> <output-path-in-s3>")


def usage(code=1):
    sys.stderr.write(usage_str + "\n")
    sys.exit(code)


def main():

    logging.basicConfig(level=logging.WARN)

    if len(sys.argv) < 3:
        usage()

    if len(sys.argv) >= 2 and sys.argv[1] == "-h":
        usage(0)

    api_token = sys.argv[1]
    output_path = sys.argv[2]

    Qubole.configure(api_token=api_token)

    args = HadoopCommand.parse(shlex.split("streaming -files s3n://paid-qubole/HadoopAPIExamples/WordCountPython/mapper.py,s3n://paid-qubole/HadoopAPIExamples/WordCountPython/reducer.py -mapper mapper.py -reducer reducer.py -numReduceTasks 1 -input s3n://paid-qubole/default-datasets/gutenberg -output %s" % output_path))

    cmd = HadoopCommand.run(**args)

    print(("Streaming Job run via command id: %s, finished with status %s"
          % (cmd.id, cmd.status)))


if __name__ == '__main__':
    try:
        sys.exit(main())
    except qds_sdk.exception.Error as e:
        sys.stderr.write("Error: Status code %s (%s) from url %s\n" %
                         (e.request.status_code, e.__class__.__name__,
                          e.request.url))
        sys.exit(1)
    except qds_sdk.exception.ConfigError as e:
        sys.stderr.write("Configuration error: %s\n" % str(e))
        sys.exit(4)
    except qds_sdk.exception.ParseError as e:
        sys.stderr.write("Error: %s\n" % str(e))
        sys.stderr.write("Usage: %s\n" % e.usage)
        sys.exit(2)
    except Exception:
        traceback.print_exc(file=sys.stderr)
        sys.exit(3)
