#!/bin/env python

from qds_sdk.qubole import Qubole
from qds_sdk.commands import *
from qds_sdk.cluster import *
from qds_sdk.hadoop_cluster import *
import qds_sdk.exception
from qds_sdk.scheduler import SchedulerCmdLine
from qds_sdk.report import ReportCmdLine
from qds_sdk.dbtaps import DbTapCmdLine

import os
import sys
import traceback
import logging
import json
from optparse import OptionParser

log = logging.getLogger("qds")
CommandClasses = {
    "hivecmd": HiveCommand,
    "pigcmd":  PigCommand,
    "hadoopcmd": HadoopCommand,
    "shellcmd": ShellCommand,
    "dbexportcmd": DbExportCommand,
    "dbimportcmd": DbImportCommand,
    "prestocmd": PrestoCommand
}

usage_str = ("Usage: \n"
             "qds [options] <CmdArgs|ClusterArgs|ReportArgs>\n"
             "\nCmdArgs:\n" +
             "  <hivecmd|hadoopcmd|prestocmd|pigcmd|shellcmd|dbexportcmd> <submit|run|check|cancel|getresult|getlog> [args .. ]\n"
             "  submit [cmd-specific-args .. ] : submit cmd & print id \n"
             "  run [cmd-specific-args .. ] : submit cmd & wait. print results \n"
             "  check <id> : print the cmd object for this Id\n"
             "  cancel <id> : cancels the cmd with this Id\n"
             "  getresult <id> : get the results for the cmd with this Id\n"
             "  getlog <id> : get the logs for the cmd with this Id\n"
             "\nClusterArgs:\n" +
             "  cluster <create|delete|update|list|start|terminate|status|reassign_label> [args .. ]\n"
             "  create [cmd-specific-args ..] : create a new cluster\n"
             "  delete [cmd-specific-args ..] : delete an existing cluster\n"
             "  update [cmd-specific-args ..] : update the settings of an existing cluster\n"
             "  list [cmd-specific-args ..] : list existing cluster(s)\n"
             "  start [cmd-specific-args ..] : start an existing cluster\n"
             "  terminate [cmd-specific-args ..] : terminate a running cluster\n"
             "  status [cmd-specific-args ..] : show whether the cluster is up or down\n" +
             "  reassign_label [cmd-specific-args ..] : reassign label from one cluster to another\n" +
             "\nDbTap:\n" +
             "  dbtap --help\n" +
             "\nReportArgs:\n" +
             "  report (<report-name> [options] | list)\n" +
             "\nScheduler:\n" +
             "  scheduler --help\n")


def usage(parser=None):
    if parser is None:
        sys.stderr.write(usage_str)
    else:
        parser.print_help()
    sys.exit(1)


def checkargs_id(args):
    if len(args) != 1:
        sys.stderr.write("expecting single argument command id\n")
        usage()


def submitaction(cmdclass, args):
    args = cmdclass.parse(args)
    if args is not None:
        cmd = cmdclass.create(**args)
        print "Submitted %s, Id: %s" % (cmdclass.__name__, cmd.id)
        return 0


def _getresult(cmdclass, cmd):
    if Command.is_success(cmd.status):
        log.info("Fetching results for %s, Id: %s" % (cmdclass.__name__, cmd.id))
        cmd.get_results(sys.stdout, delim='\t')
        return 0
    else:
        log.error("Cannot fetch results - command Id: %s failed with status: %s" % (cmd.id, cmd.status))
        return 1


def runaction(cmdclass, args):
    args = cmdclass.parse(args)
    if args is not None:
        cmd = cmdclass.run(**args)
        return _getresult(cmdclass, cmd)


def checkaction(cmdclass, args):
    checkargs_id(args)
    o = cmdclass.find(args.pop(0))
    print str(o)
    return 0


def cancelaction(cmdclass, args):
    checkargs_id(args)
    r = cmdclass.cancel_id(args.pop(0))
    skey = 'kill_succeeded'
    if r.get(skey) is None:
        sys.stderr.write("Invalid Json Response %s - missing field '%s'" % (str(r), skey))
        return 11
    elif r['kill_succeeded']:
        print "Command killed successfully"
        return 0
    else:
        sys.stderr.write("Cancel failed with reason '%s'\n" % r.get('result'))
        return 12


def getresultaction(cmdclass, args):
    checkargs_id(args)
    cmd = cmdclass.find(args.pop(0))
    return _getresult(cmdclass, cmd)


def getlogaction(cmdclass, args):
    checkargs_id(args)
    print cmdclass.get_log_id(args.pop(0))
    return 0


def cmdmain(cmd, args):
    cmdclass = CommandClasses[cmd]

    actionset = set(["submit", "run", "check", "cancel", "getresult", "getlog"])
    if len(args) < 1:
        sys.stderr.write("missing argument containing action\n")
        usage()

    action = args.pop(0)
    if action not in actionset:
        sys.stderr.write("action must be one of <%s>\n" % "|".join(actionset))
        usage()

    return globals()[action + "action"](cmdclass, args)


def checkargs_cluster_id_label(args):
    if len(args) != 1:
        sys.stderr.write("expecting single argument cluster id or cluster label\n")
        usage()


def cluster_create_action(clusterclass, args):
    arguments = clusterclass._parse_create_update(args, action="create")
    cluster_info = _create_cluster_info(arguments)
    result = clusterclass.create(cluster_info.minimal_payload())
    print json.dumps(result, indent=4)
    return 0


def cluster_update_action(clusterclass, args):
    arguments = clusterclass._parse_create_update(args, action="update")
    cluster_info = _create_cluster_info(arguments)
    result = clusterclass.update(arguments.cluster_id_label, cluster_info.minimal_payload())
    print json.dumps(result, indent=4)
    return 0


def _create_cluster_info(arguments):
    cluster_info = ClusterInfo(arguments.label,
                               arguments.aws_access_key_id,
                               arguments.aws_secret_access_key,
                               arguments.disallow_cluster_termination,
                               arguments.enable_ganglia_monitoring,
                               arguments.node_bootstrap_file,)

    cluster_info.set_ec2_settings(arguments.aws_region,
                                  arguments.aws_availability_zone)

    custom_config = None
    if arguments.custom_config_file is not None:
        try:
            custom_config = open(arguments.custom_config_file).read()
        except IOError, e:
            sys.stderr.write("Unable to read custom config file: %s\n" %
                             str(e))
            usage()

    cluster_info.set_hadoop_settings(arguments.master_instance_type,
                                     arguments.slave_instance_type,
                                     arguments.initial_nodes,
                                     arguments.max_nodes,
                                     custom_config,
                                     arguments.slave_request_type)

    cluster_info.set_spot_instance_settings(
          arguments.maximum_bid_price_percentage,
          arguments.timeout_for_request,
          arguments.maximum_spot_instance_percentage)

    cluster_info.set_stable_spot_instance_settings(
          arguments.stable_maximum_bid_price_percentage,
          arguments.stable_timeout_for_request,
          arguments.stable_allow_fallback)

    fairscheduler_config_xml = None
    if arguments.fairscheduler_config_xml_file is not None:
        try:
            fairscheduler_config_xml = open(arguments.fairscheduler_config_xml_file).read()
        except IOError, e:
            sys.stderr.write("Unable to read config xml file: %s\n" %
                             str(e))
            usage()
    cluster_info.set_fairscheduler_settings(fairscheduler_config_xml,
                                            arguments.default_pool)

    customer_ssh_key = None
    if arguments.customer_ssh_key_file is not None:
        try:
            customer_ssh_key = open(arguments.customer_ssh_key_file).read()
        except IOError, e:
            sys.stderr.write("Unable to read customer ssh key file: %s\n" %
                             str(e))
            usage()
    cluster_info.set_security_settings(arguments.encrypted_ephemerals,
                                       customer_ssh_key)

    presto_custom_config = None
    if arguments.presto_custom_config_file is not None:
        try:
            presto_custom_config = open(arguments.presto_custom_config_file).read()
        except IOError, e:
            sys.stderr.write("Unable to read presto custom config file: %s\n" %
                             str(e))
            usage()
    cluster_info.set_presto_settings(arguments.enable_presto,
                                     presto_custom_config)

    return cluster_info


def cluster_delete_action(clusterclass, args):
    checkargs_cluster_id_label(args)
    result = clusterclass.delete(args.pop(0))
    print json.dumps(result, indent=4)
    return 0


def cluster_list_action(clusterclass, args):
    arguments = clusterclass._parse_list(args)
    if arguments['cluster_id'] is not None:
        result = clusterclass.show(arguments['cluster_id'])
    elif arguments['label'] is not None:
        result = clusterclass.show(arguments['label'])
    elif arguments['state'] is not None:
        result = clusterclass.list(state=arguments['state'])
    else:
        result = clusterclass.list()
    print json.dumps(result, indent=4)
    return 0


def cluster_start_action(clusterclass, args):
    checkargs_cluster_id_label(args)
    result = clusterclass.start(args.pop(0))
    print json.dumps(result, indent=4)
    return 0


def cluster_terminate_action(clusterclass, args):
    checkargs_cluster_id_label(args)
    result = clusterclass.terminate(args.pop(0))
    print json.dumps(result, indent=4)
    return 0


def cluster_status_action(clusterclass, args):
    checkargs_cluster_id_label(args)
    result = clusterclass.status(args.pop(0))
    print json.dumps(result, indent=4)
    return 0


def cluster_reassign_label_action(clusterclass, args):
    arguments = clusterclass._parse_reassign_label(args)
    result = clusterclass.reassign_label(arguments.destination_cluster,
                                         arguments.label)
    print json.dumps(result, indent=4)
    return 0


def cluster_check_action(clusterclass, args):
    name = args.pop(0) if (len(args) >= 1) else None
    o = clusterclass.find(name=name)
    print str(o)
    return 0


def clustermain(dummy, args):
    if dummy == "hadoop_cluster":
        log.warn("'hadoop_cluster check' command is deprecated and will be" \
                         " removed in the next version. Please use 'cluster status'" \
                         " instead.\n")
        clusterclass = HadoopCluster
        actionset = set(["check"])

        if len(args) < 1:
            sys.stderr.write("missing argument containing action\n")
            usage()

        action = args.pop(0)
        if action not in actionset:
            sys.stderr.write("action must be one of <%s>\n" % "|".join(actionset))
            usage()
        return globals()["cluster_" + action + "_action"](clusterclass, args)

    else:
        clusterclass = Cluster
        actionset = set(["create", "delete", "update", "list", "start", "terminate", "status", "reassign_label"])

        if len(args) < 1:
            sys.stderr.write("missing argument containing action\n")
            usage()

        action = args.pop(0)
        if action not in actionset:
            sys.stderr.write("action must be one of <%s>\n" % "|".join(actionset))
            usage()
        return globals()["cluster_" + action + "_action"](clusterclass, args)


def reportmain(args):
    result = ReportCmdLine.run(args)
    print result


def schedulermain(args):
    result = SchedulerCmdLine.run(args)
    print result

def dbtapmain(args):
    result = DbTapCmdLine.run(args)
    print result

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

    optparser.add_option("--poll_interval", dest="poll_interval",
                         default=os.getenv('QDS_POLL_INTERVAL'),
                         help="interval for polling API for completion and other events. defaults to 5s")

    optparser.add_option("--skip_ssl_cert_check", dest="skip_ssl_cert_check", action="store_true",
                         default=False,
                         help="skip verification of server SSL certificate. Insecure: use with caution.")

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
        logging.basicConfig(level=logging.WARN)

    if options.api_token is None:
        sys.stderr.write("No API Token provided\n")
        usage(optparser)

    if options.api_url is None:
        options.api_url = "https://api.qubole.com/api/"

    if options.api_version is None:
        options.api_version = "v1.2"

    if options.poll_interval is None:
        options.poll_interval = 5

    if options.skip_ssl_cert_check is None:
        options.skip_ssl_cert_check = False
    elif options.skip_ssl_cert_check:
        log.warn("Insecure mode enabled: skipping SSL cert verification\n")

    Qubole.configure(api_token=options.api_token,
                     api_url=options.api_url,
                     version=options.api_version,
                     poll_interval=options.poll_interval,
                     skip_ssl_cert_check=options.skip_ssl_cert_check)

    if len(args) < 1:
        sys.stderr.write("Missing first argument containing command type\n")
        usage(optparser)

    a0 = args.pop(0)
    if a0 in CommandClasses:
        return cmdmain(a0, args)

    if a0 == "hadoop_cluster" or a0 == "cluster":
        return clustermain(a0, args)

    if a0 == "scheduler":
        return schedulermain(args)

    if a0 == "report":
        return reportmain(args)

    if a0 == "dbtap":
        return dbtapmain(args)

    cmdset = set(CommandClasses.keys())
    sys.stderr.write("First command must be one of <%s>\n" %
                     "|".join(cmdset.union(["cluster", "scheduler", "report", "dbtap"])))
    usage(optparser)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except qds_sdk.exception.Error as e:
        sys.stderr.write("Error: Status code %s (%s) from url %s\n" %
                         (e.request.status_code, e.__class__.__name__,
                          e.request.url))
        sys.exit(1)
    except qds_sdk.exception.ParseError as e:
        sys.stderr.write("Error: %s\n" % str(e))
        sys.stderr.write("Usage: %s\n" % e.usage)
        sys.exit(2)
    except Exception:
        traceback.print_exc(file=sys.stderr)
        sys.exit(3)
