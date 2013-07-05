""" 
The Accounts module contains the base definition for a Qubole account object
"""

from resource import SingletonResource

class Account(SingletonResource):

    # the below code is temporary - only for backwards compatibility with
    # undocumented/beta versions of the account api

    @property
    def storage_access_key(self):
        try:
            return self.attributes['storage_access_key']
        except KeyError:
            if (self.attributes['storage_type'] == 'QUBOLE_MANAGED'):
                return self.attributes['iam_access_key']
            else:
                return self.attributes['acc_key']


    @property
    def storage_secret_key(self):
        try:
            return self.attributes['storage_secret_key']
        except KeyError:
            if (self.attributes['storage_type'] == 'QUBOLE_MANAGED'):
                return self.attributes['iam_secret']
            else: 
                return self.attributes['secret']



