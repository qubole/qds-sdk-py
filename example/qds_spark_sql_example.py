"""
This is a sample code used for submitting a SQL query as a SparkCommand and getting the result.
"""
import logging

from qds_sdk.qubole import Qubole
from qds_sdk.commands import SparkCommand
import time


def get_results_filename(command_id):
    """
    A helper method to generate a file name to write the downloaded result
    :param command_id:
    :return:
    """
    return "/tmp/result_{}.tsv".format(command_id)


def execute_sql_query(cluster_label, query, arguments=None):
    """
    Helper method to execute a script
    :param cluster_label:
    :param query:
    :param arguments:
    :return:
    """
    if query is None or query == "":
        raise RuntimeError("Script to be executed cannot be None or empty")

    # A SQL command needs to be invoked in this fashion
    cmd = SparkCommand.create(label=cluster_label, sql=query, arguments=arguments)

    while not SparkCommand.is_done(cmd.status):
        logging.info("Waiting for completion of command : {}".format(cmd.id))
        cmd = SparkCommand.find(cmd.id)
        time.sleep(5)

    if SparkCommand.is_success(cmd.status):
        print("\nCommand Executed: Completed successfully")
    else:
        raise RuntimeError("Command {} has failed. The following are the command logs {}".format(cmd.id, cmd.get_log()))
    return cmd


def get_results(command):
    """
    A helper method to get the results
    :param command:
    :return:
    """
    if not command:
        raise RuntimeError("command cannot be None. Please provide a valid SparkCommand object")

    results_file_name = get_results_filename(command.id)
    fp = open(results_file_name, 'w')

    command.get_results(fp, delim="\n")
    logging.info("results are written to {}".format(results_file_name))


if __name__ == '__main__':
    # Set the API token. If you are using any other environment other then api.qubole.com then set api_url to that url
    # as <env_url>/api
    Qubole.configure(api_token='<api_token>')

    # the following are mandatory parameters while submitting the SparkCommand
    cluster_label = "<your cluster label>"  # the cluster on which the command will run

    # the following are optional parameters that can be supplied to a SparCommand
    arguments = None  # spark configuration for your program for ex : "--conf spark.executor.memory=1024M"

    # Running a SQL command
    script = "show tables"
    command = execute_sql_query(cluster_label, script, arguments=arguments)
    get_results(command)
