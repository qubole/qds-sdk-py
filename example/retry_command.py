from qds_sdk.qubole import Qubole

Qubole.configure(api_token='BjnuEktAoiyFiHZCuLSZxdUFFsrzn15h3Hj9an3xjACBkwqiYCNJcNYygsaLvAFg',api_url="http://localhost:3000/api/")

from qds_sdk.commands import *

hc=HiveCommand.create(query='show tables',retry='4')
print "Id: %s, Status: %s" % (str(hc.id), hc.status)