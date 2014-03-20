#!/bin/env python

from qds_sdk.qubole import Qubole
from qds_sdk.scheduler import Scheduler
import qds_sdk.exception
from qds_sdk import argparse

import json
import os
import sys
import traceback
import logging
from optparse import OptionParser

log = logging.getLogger("sched_qds")

def main():
    argparser = argparse.ArgumentParser(description="Scheduler client for Qubole Data Service.")

    argparser.add_argument("--token", dest="api_token", 
                         default=os.getenv('QDS_API_TOKEN'),
                         help="api token for accessing Qubole. must be specified via command line or passed in via environment variable QDS_API_TOKEN")

    argparser.add_argument("--url", dest="api_url", 
                         default=os.getenv('QDS_API_URL'),
                         help="base url for QDS REST API. defaults to https://api.qubole.com/api ")

    argparser.add_argument("--version", dest="api_version", 
                         default=os.getenv('QDS_API_VERSION'),
                         help="version of REST API to access. defaults to v1.2")

    argparser.add_argument("-v", dest="verbose", action="store_true",
                         default=False,
                         help="verbose mode - info level logging")

    argparser.add_argument("--vv", dest="chatty", action="store_true",
                         default=False,
                         help="very verbose mode - debug level logging")
    subparsers = argparser.add_subparsers()
    scheduler = Scheduler(subparsers)

    args = argparser.parse_args()

    if args.chatty:
        logging.basicConfig(level=logging.DEBUG)
    elif args.verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        # whatever is dictated by logging config
        pass

    if args.api_token is None:
        raise Exception("No API Token provided")

    if args.api_url is None:
        args.api_url = "https://api.qubole.com/api/";

    if args.api_version is None:
        args.api_version = "v1.2";
        
    Qubole.configure(api_token=args.api_token,
                     api_url=args.api_url,
                     version=args.api_version)
                         
    args.func(args)
       
if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc(file=sys.stderr)
        sys.exit(3)
