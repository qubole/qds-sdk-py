Qubole Data Service Python SDK
==============================

A Python module that provides the tools you need to authenticate with, and use the Qubole Data Service API.


Installation
------------

$ python setup.py install # may need to do this as root


This should place a command line utility 'qds.py' somewhere in your path

$ which qds.py
/usr/bin/qds.py 


CLI
---

qds.py allows running Hive, Hadoop and Pig commands against QDS. Users can run commands synchronously - or submit a command and check it's status.

$ qds.py -h # will print detailed usage

Examples:

// run a hive query and print the results
$ qds.py --token 'xxyyzz' hivecmd run --query "show tables"

// pass in api token from bash var 
$ export QDS_API_TOKEN=xxyyzz

// run the example hadoop command
$ qds.py hadoopcmd run jar 's3://paid-qubole/HadoopAPIExamples/jars/hadoop-0.20.1-dev-streaming.jar' -files 's3n://paid-qubole/HadoopAPIExamples/WordCountPython/mapper.py,s3n://paid-qubole/HadoopAPIExamples/WordCountPython/reducer.py' -mapper mapper.py -reducer reducer.py -numReduceTasks 1 -input 's3n://paid-qubole/default-datasets/gutenberg' -output 's3n://example.bucket.com/wcout'

// check the status of command # 12345678
$ qds.py hivecmd check 12345678
{"status": "done", ... }


SDK API
-------

An example Python application needs to do the following:

1.  Set the api_token:

        from qubole import Qubole

        Qubole.configure(api_token='ksbdvcwdkjn123423')

2.  Use the Command classes defined in commands.py to execute commands. To run Hive Command:

        from commands import *

        hc=HiveCommand.create(query='show tables')
        print "Id: %s, Status: %s" % (str(hc.id), hc.status)
