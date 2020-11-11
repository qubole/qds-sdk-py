"""
The following code example submits a Spark program written in either Scala, Python or R using SparkCommand
command type.
The input program/script is made available using a local file path and the results are written to a temporary file.
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


def get_content(script_file_path):
    """
    Helper method to read a script file in the given location.
    :param script_file_path:
    :return:
    """
    with open(script_file_path, 'r') as content_file:
        content = content_file.read()
    return content


def execute_script(cluster_label, cmd_to_run, language, user_program_arguments=None, arguments=None):
    """
    Helper method to execute a script
    :param cluster_label:
    :param cmd_to_run:
    :param language:
    :param user_program_arguments:
    :param arguments:
    :return:
    """
    if script is None or script == "":
        raise RuntimeError("script to be executed cannot be None or empty")

    if not language:
        raise RuntimeError("Language is a mandatory parameter. Please set the correct language for your script.")

    # A python, R or scala command needs to be invoked in this fashion.
    cmd = SparkCommand.create(label=cluster_label, program=cmd_to_run, language=language, arguments=arguments,
                              user_program_arguments=user_program_arguments)

    while not SparkCommand.is_done(cmd.status):
        logging.info("Waiting for completion of command : {}".format(cmd.id))
        cmd = SparkCommand.find(cmd.id)
        time.sleep(5)

    if SparkCommand.is_success(cmd.status):
        logging.info("\nCommand Executed: Completed successfully")
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
        raise RuntimeError("Command cannot be None. Please provide a valid SparkCommand object")

    results_file_name = get_results_filename(command.id)
    fp = open(results_file_name, 'w')

    command.get_results(fp, delim="\n")
    logging.info("results are written to {}".format(results_file_name))


if __name__ == '__main__':
    # Set the API token. If you are using any other environment other then api.qubole.com then set api_url to that url
    # as http://<env_url>/api
    Qubole.configure(api_token='<api_token>', api_url="<api_url if your environment is other than api.qubole.com>")

    # the following are mandatory parameters while submitting the SparkCommand
    cluster_label = "<your cluster label>"  # the label of the cluster on which the command will run
    filename = "<path to the local script file containing Spark program>"
    script_language = "<Set the language of the script here>"  # Script language.. python, R or scala

    # the following are optional parameters that can be supplied to a SparCommand
    user_program_arguments = None  # arguments for your script
    arguments = None  # spark configuration for your program for ex : "--conf spark.executor.memory=1024M"

    # read the content of the script file into a variable.
    script = get_content(filename)
    command = execute_script(cluster_label, script, script_language, user_program_arguments=user_program_arguments,
                             arguments=arguments)
    get_results(command)
