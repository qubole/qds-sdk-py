"""
This is the sample code used for submitting a Presto query (PrestoCommand) and getting the result back to local file.
Similar way can be followed for HiveCommand etc.
"""
 
import logging, sys, string, time
from tempfile import NamedTemporaryFile
from configparser import SafeConfigParser
from qds_sdk.qubole import Qubole
from qds_sdk.commands import PrestoCommand
import boto
import pandas as pd

# Setting up the logger
logging.basicConfig(stream=sys.stdout,
                    format='[%(asctime)s] [%(filename)s] [%(levelname)s] %(message)s',
                    level=logging.WARN)
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


#######################
# PRIVATE FUNCTIONS
#######################
def _wait_for_cmd(cmd, update=True):
    while not PrestoCommand.is_done(cmd.status):
        if update: cmd = PrestoCommand.find(cmd.id) # Updating the Python object through HTTP
        time.sleep(3)
    if PrestoCommand.is_success(cmd.status):
        log.info("Command completed successfully")
    else:
        log.error(f"Command failed! The status returned is: {cmd.status}")
    return cmd

def _get_results_locally(cmd):
    filename = f"/tmp/result_{int(time.time())}.tsv"
    log.debug(f"Fetching results locally into {filename} ...")
    with open(filename, 'w') as f:
        cmd.get_results(f, delim="\t", inline=True, arguments=['true'])
        _wait_for_cmd(cmd, update=False)
    return filename


#######################
# PUBLIC FUNCTIONS
#######################
def execute_presto_query(query, cluster_label='presto'):
    assert query is not None
    cmd = PrestoCommand.create(query=query, label=cluster_label)
    log.debug(f"Starting Presto Command with id: {cmd.id}"),
    cmd = _wait_for_cmd(cmd)
    log.debug(f"Command {cmd.id} done. Logs are :"),
    print(cmd.get_log())
    return cmd

def get_raw_results(cmd, column_names = None):
    assert cmd is not None
    assert type(cmd) is PrestoCommand
    with open(_get_results_locally(cmd), 'r') as content_file:
        content = content_file.read()
    return content
 
def get_dataframe(command, column_names = None, **kwargs):
    assert command is not None
    assert type(command) is PrestoCommand
    
    _NA_VALUES = list(pd.io.common._NA_VALUES) + ['\\N'] # The NA Values that should be considered for Presto
    filename = _get_results_locally(command)
    
    with open(filename) as f:
        firstline = f.readline()
    if firstline.strip().split('\t') == column_names :
        log.debug('It seems that the file already got the right column names...')
        return pd.read_csv(filename, delimiter='\t', na_values=_NA_VALUES, **kwargs)
    else :
        log.debug('It seems that the column names are not present in the file. Adding them :')
        return pd.read_csv(filename, delimiter='\t', na_values=_NA_VALUES, header=None, names=column_names, **kwargs)
    
 
