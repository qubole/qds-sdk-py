#!/bin/env python

from qds_sdk.qubole import Qubole
from qds_sdk.schedule import *
import qds_sdk.exception

import json
import os
import sys
import traceback
import logging
from optparse import OptionParser

log = logging.getLogger("sched_qds")

usage_str = ("\nUsage: \n"
             "sched_qds [options] "
             "schedule <list|view|listinstance|viewcommand|getresult|getlog|suspend|resume|kill> [args .. ]\n"
             "\tlist : List all schedules \n"
             "\tview [cmd-specific-args .. ] : View all schedules \n"
             "\tlistinstance [cmd-specific-args .. ] : List all schedule instances\n"
             "\tviewcommand [cmd-specific-args .. ] : View a schedule command\n"
             "\tgetresult [cmd-specific-args .. ] : Get Results of schedule instance\n"
             "\tgetlog [cmd-specific-args .. ] : Get Logs of schedule instance\n"
             "\tsuspend [cmd-specific-args .. ] : Suspend a schedule\n"
             "\tresume [cmd-specific-args .. ] : Resume a schedule\n"
             "\tkill [cmd-specific-args .. ] : Kill a schedule\n")

def usage():
    sys.stderr.write(usage_str)
    sys.exit(1)

def listaction(cmdclass, args):
    results =  (cmdclass.list_schedulers())
    print json.dumps(results, indent = 4, separators=(',',': '))
    
def viewaction(cmdclass, args):
    results =  (cmdclass.view_schedulers(args['schid']))
    print json.dumps(results, indent = 4, separators=(',',': '))
    
def suspendaction(cmdclass, args):
    results =  (cmdclass.suspend_scheduler(args['schid']))
    print results    
    
def resumeaction(cmdclass, args):
    results =  (cmdclass.resume_scheduler(args['schid']))
    print results
    
def killaction(cmdclass, args):
    results =  (cmdclass.kill_scheduler(args['schid']))
    print results
   
def listinstanceaction(cmdclass, args):
    results =  (cmdclass.list_instance(args['schid']))
    print json.dumps(results, indent = 4, separators=(',',': '))

def viewcommandaction(cmdclass, args):
    results =  (cmdclass.view_command(args['schid'], args['instid']))
    print json.dumps(results, indent = 4, separators=(',',': '))
    
def getresultaction(cmdclass, args):
    results =  (cmdclass.get_results(args['schid'], args['instid']))
    print json.dumps(results, indent = 4, separators=(',',': '))
    
def getlogaction(cmdclass, args):
    results =  (cmdclass.get_logs(args['schid'], args['instid']))
    print results
    
def cmdmain(cmd, args):

    cmdclassname = cmd[0].upper() + cmd[1:]
        
    actionset = set(["list", "view", "listinstance", "viewcommand", "getresult", "getlog", "suspend" , "resume" , "kill" ])
    if len(args) < 1:
        sys.stderr.write("missing argument containing action\n")
        usage()
    
    action = args.pop(0)
    if action not in actionset:
        sys.stderr.write("action must be one of <%s>\n" % "|".join(actionset))
        usage()
     
    cmdclass = globals()[cmdclassname](action)
    #Call the parse method to parse the arguments and store the dict back in args 
    args = cmdclass.parse(args)
    return globals()[action + "action"](cmdclass, args)


def main():

    optparser = OptionParser(usage=usage_str)
    optparser.add_option("--token", dest="api_token", 
                         default=os.getenv('QDS_API_TOKEN'),
                         help="api token for accessing Qubole. must be specified via command line or passed in via environment variable QDS_API_TOKEN")

    optparser.add_option("--url", dest="api_url", 
                         default=os.getenv('QDS_API_URL'),
                         help="base url for QDS REST API. defaults to https://api.qubole.com/api ")

    optparser.add_option("--version", dest="api_version", 
                         default=os.getenv('QDS_API_VERSION'),
                         help="version of REST API to access. defaults to v1.2")

    optparser.add_option("-v", dest="verbose", action="store_true",
                         default=False,
                         help="verbose mode - info level logging")

    optparser.add_option("--vv", dest="chatty", action="store_true",
                         default=False,
                         help="very verbose mode - debug level logging")


    optparser.disable_interspersed_args()
    (options, args) = optparser.parse_args()

    if options.chatty:
        logging.basicConfig(level=logging.DEBUG)
    elif options.verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        # whatever is dictated by logging config
        pass

    if options.api_token is None:
        raise Exception("No API Token provided")

    if options.api_url is None:
        options.api_url = "https://api.qubole.com/api/";

    if options.api_version is None:
        options.api_version = "v1.2";
        
    Qubole.configure(api_token=options.api_token,
                     api_url=options.api_url,
                     version=options.api_version)
                         
    if len(args) < 1:
        sys.stderr.write("Missing first argument \'schedule\' \n")
        usage()
        
    cmd = args.pop(0)
    
    if cmd.lower()=="schedule" is False:  
        sys.stderr.write("First command must be \'schedule\'")
        usage()
        
    return cmdmain(cmd.lower(), args)
       
if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc(file=sys.stderr)
        sys.exit(3)
