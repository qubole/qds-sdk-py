#!/bin/env python

from qds_sdk.qubole import Qubole
from qds_sdk.commands import *

import os
import sys
import traceback
import logging
from optparse import OptionParser

log = logging.getLogger("qds")

def main():

    optparser = OptionParser()
    optparser.add_option("--token", dest="api_token", 
                         default=os.getenv('QDS_API_TOKEN'),
                         help="api token for accessing Qubole")
    optparser.add_option("--url", dest="api_url", 
                         default=os.getenv('QDS_API_URL'),
                         help="base url for QDS REST API")
    optparser.add_option("--version", dest="api_version", 
                         default=os.getenv('QDS_API_VERSION'),
                         help="version of REST API to access")
    optparser.add_option("--poll_interval", dest="poll_interval", 
                         default=os.getenv('QDS_POLL_INTERVAL'),
                         help="interval for polling API for completion and other events")

    optparser.add_option("--verbose", dest="verbose", action="store_true",
                         default=False,
                         help="verbose mode for debug output")

    optparser.disable_interspersed_args()
    (options, args) = optparser.parse_args()

    if options.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if options.api_token is None:
        raise Exception("No API Token provided")

    if options.api_url is None:
        options.api_url = "https://api.qubole.com/api/";

    if options.api_version is None:
        options.api_version = "v1.2";

    if options.poll_interval is None:
        options.poll_interval = 5;

        
    Qubole.configure(api_token=options.api_token,
                     api_url=options.api_url,
                     version=options.api_version,
                     poll_interval=options.poll_interval)
                     

    if len(args) != 1:
        raise Exception("Expecting query as argument")

    hc=HiveCommand.run(query=args[0])
    log.info("Completed Query, Id: %s, Status: %s" % (str(hc.id), hc.status))

    if (hc.status == "done"):
        print hc.get_results()
    else:
        sys.stderr.write(hc.getLog())
        return 1

    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

