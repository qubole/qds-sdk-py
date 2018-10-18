#!/bin/env python

from __future__ import print_function
from qds_sdk.commands import *
from qds_sdk.cluster import *
import qds_sdk.exception
from qds_sdk.scheduler import SchedulerCmdLine
from qds_sdk.actions import ActionCmdLine
from qds_sdk.report import ReportCmdLine
from qds_sdk.dbtaps import DbTapCmdLine
from qds_sdk.role import RoleCmdLine
from qds_sdk.group import GroupCmdLine
from qds_sdk.account import AccountCmdLine
from qds_sdk.app import AppCmdLine
from qds_sdk.nezha import NezhaCmdLine
from qds_sdk.user import UserCmdLine
from qds_sdk.template import TemplateCmdLine
from qds_sdk.clusterv2 import ClusterCmdLine
from qds_sdk.sensors import *
import os
import sys
import traceback
import logging
import json
from optparse import OptionParser

log = logging.getLogger("qds")
CommandClasses = {
    "hivecmd": HiveCommand,
    "sparkcmd": SparkCommand,
    "dbtapquerycmd": DbTapQueryCommand,
    "pigcmd":  PigCommand,
    "hadoopcmd": HadoopCommand,
    "shellcmd": ShellCommand,
    "dbexportcmd": DbExportCommand,
    "dbimportcmd": DbImportCommand,
    "prestocmd": PrestoCommand
}

SensorClasses = {
    "filesensor": FileSensor,
    "partitionsensor": PartitionSensor
}

usage_str = (
    "Usage: qds.py [options] <subcommand>\n"
    "\nCommand subcommands:\n"
    "  <hivecmd|hadoopcmd|prestocmd|pigcmd|shellcmd|dbexportcmd|dbimportcmd|dbtapquerycmd|sparkcmd> <action>\n"
    "    submit [cmd-specific-args .. ] : submit cmd & print id\n"
    "    run [cmd-specific-args .. ] : submit cmd & wait. print results\n"
    "    check <id> <include-query-properties> : id -> print the cmd object for this id\n"
    "                                            include-query-properties(true/false) -> to include query properties like\n"
    "                                            tags, comments and user actions\n"
    "    cancel <id> : cancels the cmd with this id\n"
    "    getresult <id> <include_header>: id -> get the results for the cmd with this id\n"
    "                                     include_header -> to include headers in results(true/false)\n"
    "    getlog <id> : get the logs for the cmd with this id\n"
    "\nCluster subcommand:\n"
    "  cluster <action>\n"
    "    create: create a new cluster\n"
    "    delete: delete an existing cluster\n"
    "    update: update the settings of an existing cluster\n"
    "    clone: clone a cluster from an existing one\n"
    "    list: list existing cluster(s)\n"
    "    start: start an existing cluster\n"
    "    terminate: terminate a running cluster\n"
    "    status: show whether the cluster is up or down\n"
    "    master: returns details of the master node of the cluster\n"
    "    reassign_label: reassign label from one cluster to another\n"
    "    snapshot: take snapshot of cluster\n"
    "    restore_point: restore cluster from snapshot\n"
    "    add_node: add a node to existing cluster\n"
    "    remove_node: remove a node to existing cluster\n"
    "    update_node: update a node on a existing cluster\n"
    "    get_snapshot_schedule: get details of scheduled snapshots on a hbase cluster\n"
    "    update_snapshot_schedule: update scheduled snapshots on a hbase cluster\n"
    "\nDbTap subcommand:\n"
    "  dbtap --help\n"
    "\nReport subcommand:\n"
    "  report --help\n"
    "\nGroup subcommand:\n"
    "  group --help\n"
    "\nRole subcommand:\n"
    "  role --help\n"
    "\nApp subcommand:\n"
    "  app --help\n"
    "\nAction subcommand:\n"
    "  action --help\n"
    "\nScheduler subcommand:\n"
    "  scheduler --help\n"
    "\nTemplate subcommand:\n"
    "  template --help\n"
    "\nAccount subcommand:\n"
    "  account --help\n"
    "\nNezha subcommand:\n"
    "  nezha --help\n"
    "\nUser subcommad:\n"
    "  user --help\n"
    "\nSensor subcommand:\n"
    " <filesensor|partitionsensor> --help\n")


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
        args.pop("print_logs") # This is only useful while using the 'run' action.
        args.pop("print_logs_live") # This is only useful while using the 'run' action.
        cmd = cmdclass.create(**args)
        print("Submitted %s, Id: %s" % (cmdclass.__name__, cmd.id))
        return 0


def _getresult(cmdclass, cmd, args=[]):
    if Command.is_success(cmd.status):
        log.info("Fetching results for %s, Id: %s" % (cmdclass.__name__, cmd.id))
        cmd.get_results(sys.stdout, delim='\t', qlog=cmd.qlog, arguments=args)
        return 0
    else:
        log.error("Cannot fetch results - command Id: %s failed with status: %s" % (cmd.id, cmd.status))
        return 1


def runaction(cmdclass, args):
    args = cmdclass.parse(args)
    if args is not None:
        print_logs = args.pop("print_logs") # We don't want to send this to the API.
        cmd = cmdclass.run(**args)
        if print_logs:
            sys.stderr.write(cmd.get_log())
        return _getresult(cmdclass, cmd)


def checkaction(cmdclass, args):
    if len(args) > 2:
        sys.stderr.write("expecting not more than 2 arguments\n")
        usage()

    conn = Qubole.agent()
    id = args.pop(0)
    include_query_properties="false"
    if len(args) == 1:
        include_query_properties=args.pop(0)
        if include_query_properties not in ('true', 'false'):
            raise ParseError("include-query-properties can be either true or false")

    r = Resource(conn.get(cmdclass.element_path(id), {'include_query_properties': include_query_properties}))
    print(str(r))
    return 0


def cancelaction(cmdclass, args):
    checkargs_id(args)
    r = cmdclass.cancel_id(args.pop(0))
    skey = 'kill_succeeded'
    if r.get(skey) is None:
        sys.stderr.write("Invalid Json Response %s - missing field '%s'" % (str(r), skey))
        return 11
    elif r['kill_succeeded']:
        print("Command killed successfully")
        return 0
    else:
        sys.stderr.write("Cancel failed with reason '%s'\n" % r.get('result'))
        return 12


def getresultaction(cmdclass, args):
    if len(args) > 2:
        sys.stderr.write("expecting not more than 2 arguments\n")
        usage()

    cmd = cmdclass.find(args.pop(0))
    return _getresult(cmdclass, cmd, args)


def getlogaction(cmdclass, args):
    checkargs_id(args)
    print(cmdclass.get_log_id(args.pop(0)))
    return 0


def getjobsaction(cmdclass, args):
    checkargs_id(args)
    cmd = cmdclass.find(args.pop(0))
    if Command.is_done(cmd.status):
        log.info("Fetching jobs for %s, Id: %s" % (cmdclass.__name__, cmd.id))
        print(cmdclass.get_jobs_id(cmd.id))
        return 0
    else:
        log.error("Cannot fetch jobs - command Id: %s is not done. Status: %s" % (cmd.id, cmd.status))
        return 1


def cmdmain(cmd, args):
    cmdclass = CommandClasses[cmd]

    actionset = set(["submit", "run", "check", "cancel", "getresult", "getlog", "getjobs"])
    if len(args) < 1:
        sys.stderr.write("missing argument containing action\n")
        usage()

    action = args.pop(0)
    if action not in actionset:
        sys.stderr.write("action must be one of <%s>\n" % "|".join(actionset))
        usage()

    return globals()[action + "action"](cmdclass, args)


def sensormain(sensor, args):
    sensor_class = SensorClasses[sensor]
    print(SensorCmdLine.check(sensor_class, args))
    return 0


def checkargs_cluster_id_label(args):
    if len(args) != 1:
        sys.stderr.write("expecting single argument cluster id or cluster label\n")
        usage()


def cluster_create_action(clusterclass, args, api_version=1.2):
    arguments = clusterclass._parse_create_update(args, "create", api_version)
    cluster_info = _create_cluster_info(arguments, api_version)
    result = clusterclass.create(cluster_info.minimal_payload())
    print(json.dumps(result, indent=4))
    return 0

def cluster_update_action(clusterclass, args, api_version=1.2):
    arguments = clusterclass._parse_create_update(args, "update", api_version)
    cluster_info = _create_cluster_info(arguments, api_version)
    result = clusterclass.update(arguments.cluster_id_label, cluster_info.minimal_payload())
    print(json.dumps(result, indent=4))
    return 0

def cluster_clone_action(clusterclass, args, api_version=1.2):
    arguments = clusterclass._parse_create_update(args, "clone", api_version)
    cluster_info = _create_cluster_info(arguments, api_version)
    result = clusterclass.clone(arguments.cluster_id_label, cluster_info.minimal_payload())
    print(json.dumps(result, indent=4))
    return 0

def _create_cluster_info(arguments, api_version):
    custom_config = _read_file(arguments.custom_config_file, "custom config file")
    presto_custom_config = _read_file(arguments.presto_custom_config_file, "presto custom config file")
    fairscheduler_config_xml = _read_file(arguments.fairscheduler_config_xml_file, "config xml file")
    customer_ssh_key = _read_file(arguments.customer_ssh_key_file, "customer ssh key file")

    cluster_info = None
    if api_version >= 1.3:
        cluster_info = ClusterInfoV13(arguments.label, api_version)
        cluster_info.set_cluster_info(aws_access_key_id=arguments.aws_access_key_id,
                                      aws_secret_access_key=arguments.aws_secret_access_key,
                                      aws_region=arguments.aws_region,
                                      aws_availability_zone=arguments.aws_availability_zone,
                                      vpc_id=arguments.vpc_id,
                                      subnet_id=arguments.subnet_id,
                                      master_elastic_ip=arguments.master_elastic_ip,
                                      disallow_cluster_termination=arguments.disallow_cluster_termination,
                                      enable_ganglia_monitoring=arguments.enable_ganglia_monitoring,
                                      node_bootstrap_file=arguments.node_bootstrap_file,
                                      master_instance_type=arguments.master_instance_type,
                                      slave_instance_type=arguments.slave_instance_type,
                                      initial_nodes=arguments.initial_nodes,
                                      max_nodes=arguments.max_nodes,
                                      slave_request_type=arguments.slave_request_type,
                                      fallback_to_ondemand=arguments.fallback_to_ondemand,
                                      custom_config=custom_config,
                                      use_hbase=arguments.use_hbase,
                                      custom_ec2_tags=arguments.custom_ec2_tags,
                                      use_hadoop2=arguments.use_hadoop2,
                                      use_spark=arguments.use_spark,
                                      use_qubole_placement_policy=arguments.use_qubole_placement_policy,
                                      maximum_bid_price_percentage=arguments.maximum_bid_price_percentage,
                                      timeout_for_request=arguments.timeout_for_request,
                                      maximum_spot_instance_percentage=arguments.maximum_spot_instance_percentage,
                                      stable_maximum_bid_price_percentage=arguments.stable_maximum_bid_price_percentage,
                                      stable_timeout_for_request=arguments.stable_timeout_for_request,
                                      stable_allow_fallback=arguments.stable_allow_fallback,
                                      ebs_volume_count=arguments.ebs_volume_count,
                                      ebs_volume_type=arguments.ebs_volume_type,
                                      ebs_volume_size=arguments.ebs_volume_size,
                                      fairscheduler_config_xml=fairscheduler_config_xml,
                                      default_pool=arguments.default_pool,
                                      encrypted_ephemerals=arguments.encrypted_ephemerals,
                                      ssh_public_key=customer_ssh_key,
                                      persistent_security_group=arguments.persistent_security_group,
                                      enable_presto=arguments.enable_presto,
                                      bastion_node_public_dns=arguments.bastion_node_public_dns,
                                      role_instance_profile=arguments.role_instance_profile,
                                      presto_custom_config=presto_custom_config,
                                      is_ha=arguments.is_ha)
    else:
        cluster_info = ClusterInfo(arguments.label,
                                   arguments.aws_access_key_id,
                                   arguments.aws_secret_access_key,
                                   arguments.disallow_cluster_termination,
                                   arguments.enable_ganglia_monitoring,
                                   arguments.node_bootstrap_file,)

        cluster_info.set_ec2_settings(arguments.aws_region,
                                      arguments.aws_availability_zone,
                                      arguments.vpc_id,
                                      arguments.subnet_id,
                                      arguments.master_elastic_ip,
                                      arguments.role_instance_profile,
                                      arguments.bastion_node_public_dns)

        cluster_info.set_hadoop_settings(arguments.master_instance_type,
                                         arguments.slave_instance_type,
                                         arguments.initial_nodes,
                                         arguments.max_nodes,
                                         custom_config,
                                         arguments.slave_request_type,
                                         arguments.use_hbase,
                                         arguments.custom_ec2_tags,
                                         arguments.use_hadoop2,
                                         arguments.use_spark,
                                         arguments.is_ha)

        cluster_info.set_spot_instance_settings(
              arguments.maximum_bid_price_percentage,
              arguments.timeout_for_request,
              arguments.maximum_spot_instance_percentage)

        cluster_info.set_stable_spot_instance_settings(
              arguments.stable_maximum_bid_price_percentage,
              arguments.stable_timeout_for_request,
              arguments.stable_allow_fallback)

        cluster_info.set_fairscheduler_settings(fairscheduler_config_xml,
                                            arguments.default_pool)

        cluster_info.set_security_settings(arguments.encrypted_ephemerals,
                                           customer_ssh_key,
                                           arguments.persistent_security_group)

        cluster_info.set_presto_settings(arguments.enable_presto,
                                         presto_custom_config)

    return cluster_info

def _read_file(file_path, file_name):
    file_content = None
    if file_path is not None:
        try:
            file_content = open(file_path).read()
        except IOError as e:
            sys.stderr.write("Unable to read %s: %s\n" % (file_name, str(e)))
            usage()
    return file_content

def cluster_delete_action(clusterclass, args):
    checkargs_cluster_id_label(args)
    result = clusterclass.delete(args.pop(0))
    print(json.dumps(result, indent=4))
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
    print(json.dumps(result, indent=4))
    return 0


def cluster_start_action(clusterclass, args):
    checkargs_cluster_id_label(args)
    result = clusterclass.start(args.pop(0))
    print(json.dumps(result, indent=4))
    return 0


def cluster_terminate_action(clusterclass, args):
    checkargs_cluster_id_label(args)
    result = clusterclass.terminate(args.pop(0))
    print(json.dumps(result, indent=4))
    return 0


def cluster_status_action(clusterclass, args):
    checkargs_cluster_id_label(args)
    result = clusterclass.status(args.pop(0))
    print(json.dumps(result, indent=4))
    return 0


def cluster_master_action(clusterclass, args):
    checkargs_cluster_id_label(args)
    result = clusterclass.master(args.pop(0))
    print(json.dumps(result, indent=4))
    return 0


def cluster_reassign_label_action(clusterclass, args):
    arguments = clusterclass._parse_reassign_label(args)
    result = clusterclass.reassign_label(arguments.destination_cluster,
                                         arguments.label)
    print(json.dumps(result, indent=4))
    return 0

def cluster_snapshot_action(clusterclass, args):
    arguments = clusterclass._parse_snapshot_restore_command(args, "snapshot")
    result = clusterclass.snapshot(arguments.cluster_id or arguments.label, arguments.s3_location, arguments.backup_type)
    print(json.dumps(result, indent=4))
    return 0

def cluster_restore_point_action(clusterclass, args):
    arguments = clusterclass._parse_snapshot_restore_command(args, "restore_point")
    result = clusterclass.restore_point(arguments.cluster_id or arguments.label, arguments.s3_location, arguments.backup_id, arguments.table_names, arguments.no_overwrite, arguments.no_automatic)
    print(json.dumps(result, indent=4))
    return 0

def cluster_get_snapshot_schedule_action(clusterclass, args):
    arguments = clusterclass._parse_get_snapshot_schedule(args)
    result = clusterclass.get_snapshot_schedule(arguments.cluster_id or arguments.label)
    print(json.dumps(result, indent=4))
    return 0

def cluster_update_snapshot_schedule_action(clusterclass, args):
    arguments = clusterclass._parse_update_snapshot_schedule(args)
    result = clusterclass.update_snapshot_schedule(arguments.cluster_id or arguments.label, arguments.s3_location, arguments.frequency_unit, arguments.frequency_num, arguments.status)
    print(json.dumps(result, indent=4))
    return 0

def cluster_add_node_action(clusterclass, args):
    arguments = clusterclass._parse_cluster_manage_command(args, action = "add")
    result = clusterclass.add_node(arguments.cluster_id or arguments.label)
    print(json.dumps(result, indent=4))
    return 0

def cluster_remove_node_action(clusterclass, args):
    arguments = clusterclass._parse_cluster_manage_command(args, action = "remove")
    result = clusterclass.remove_node(arguments.cluster_id or arguments.label, arguments.private_dns)
    print(json.dumps(result, indent=4))
    return 0

def cluster_update_node_action(clusterclass, args):
    arguments = clusterclass._parse_cluster_manage_command(args, action = "update")
    result = clusterclass.update_node(arguments.cluster_id or arguments.label, arguments.command, arguments.private_dns)
    print(json.dumps(result, indent=4))
    return 0

def clustermain(args, api_version):
    clusterclass = Cluster
    actionset = set(["create", "delete", "update", "clone", "list", "start", "terminate", "status", "master", "reassign_label", "add_node", "remove_node", "update_node", "snapshot", "restore_point", "get_snapshot_schedule", "update_snapshot_schedule"])

    if len(args) < 1:
        sys.stderr.write("missing argument containing action\n")
        usage()

    action = args.pop(0)
    if action not in actionset:
        sys.stderr.write("action must be one of <%s>\n" % "|".join(actionset))
        usage()
    elif action in set(["create", "update", "clone"]):
        return globals()["cluster_" + action + "_action"](clusterclass, args, api_version)
    else:
        return globals()["cluster_" + action + "_action"](clusterclass, args)

def clustermainv2(args):
    action = args[0]
    actionset = set(
        ["create", "delete", "update", "list", "clone", "start", "terminate", "status", "reassign_label", "add_node",
         "remove_node", "update_node", "snapshot", "restore_point", "get_snapshot_schedule",
         "update_snapshot_schedule"])

    result = None
    if action not in actionset:
        sys.stderr.write("action must be one of <%s>\n" % "|".join(actionset))
        usage()
    elif action in set(["create", "update", "clone", "list"]):
        result =  ClusterCmdLine.run(args)
    else:
        action = args.pop(0)
        result = globals()["cluster_" + action + "_action"](Cluster, args)
    print(result)

def accountmain(args):
    result = AccountCmdLine.run(args)
    print(result)

def usermain(args):
    result = UserCmdLine.run(args)
    print(result)

def reportmain(args):
    result = ReportCmdLine.run(args)
    print(result)


def actionmain(args):
    result = ActionCmdLine.run(args)
    print(result)

def schedulermain(args):
    result = SchedulerCmdLine.run(args)
    print(result)

def dbtapmain(args):
    result = DbTapCmdLine.run(args)
    print(result)

def rolemain(args):
    result = RoleCmdLine.run(args)
    print(result)

def groupmain(args):
    result = GroupCmdLine.run(args)
    print(result)

def appmain(args):
    result = AppCmdLine.run(args)
    print(result)

def nezhamain(args):
    result = NezhaCmdLine.run(args)
    print(result)

def templatemain(args):
    result = TemplateCmdLine.run(args)
    print(result)


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
                         type=int,
                         default=os.getenv('QDS_POLL_INTERVAL'),
                         help="interval for polling API for completion and other events. defaults to 5s")

    optparser.add_option("--skip_ssl_cert_check", dest="skip_ssl_cert_check", action="store_true",
                         default=False,
                         help="skip verification of server SSL certificate. Insecure: use with caution.")

    optparser.add_option("--cloud_name", dest="cloud_name",
                         default=os.getenv('CLOUD_PROVIDER'),
                         help="cloud", choices=["AWS", "AZURE", "ORACLE_BMC", "ORACLE_OPC"])

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

    if options.cloud_name is None:
        options.cloud_name = "AWS"

    if options.skip_ssl_cert_check is None:
        options.skip_ssl_cert_check = False
    elif options.skip_ssl_cert_check:
        log.warn("Insecure mode enabled: skipping SSL cert verification\n")

    Qubole.configure(api_token=options.api_token,
                     api_url=options.api_url,
                     version=options.api_version,
                     poll_interval=options.poll_interval,
                     skip_ssl_cert_check=options.skip_ssl_cert_check,
                     cloud_name=options.cloud_name)

    if len(args) < 1:
        sys.stderr.write("Missing first argument containing subcommand\n")
        usage(optparser)

    a0 = args.pop(0)
    if a0 in CommandClasses:
        return cmdmain(a0, args)

    if a0 in SensorClasses:
        return sensormain(a0, args)

    if a0 == "account":
        return accountmain(args)

    if a0 == "cluster":
        api_version_number = float(options.api_version[1:])
        if api_version_number >= 2.0:
            return clustermainv2(args)
        else:
            return clustermain(args, api_version_number)

    if a0 == "action":
        return actionmain(args)

    if a0 == "scheduler":
        return schedulermain(args)

    if a0 == "report":
        return reportmain(args)

    if a0 == "dbtap":
        return dbtapmain(args)

    if a0 == "group":
        return groupmain(args)

    if a0 == "role":
        return rolemain(args)

    if a0 == "app":
        return appmain(args)

    if a0 == "nezha":
        return nezhamain(args)

    if a0 == "user":
        return usermain(args)
    if a0 == "template":
        return templatemain(args)

    cmdset = set(CommandClasses.keys())
    sys.stderr.write("First command must be one of <%s>\n" %
                     "|".join(cmdset.union(["cluster", "action", "scheduler", "report",
                       "dbtap", "role", "group", "app", "account", "nezha", "user", "template"])))
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
