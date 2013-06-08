from qubole import Qubole
from resource import Resource
import time
import logging

log = logging.getLogger("qds_commands")

class Command(Resource):

    rest_entity_path="commands"

    @staticmethod
    def is_done(status):
        return (status == "cancelled" or status == "done" or status == "error")
    

    @classmethod
    def create(cls, **kwargs):
        conn=Qubole.agent()
        if kwargs.get('command_type') is None:
            kwargs['command_type'] = cls.__name__

        return cls(conn.post(cls.rest_entity_path, data=kwargs))


    @classmethod
    def run(cls, **kwargs):
        cmd = cls.create(**kwargs)
        while not Command.is_done(cmd.status):
            time.sleep(Qubole.poll_interval)
            cmd = cls.find(cmd.id)

        return cmd

    def cancel(self):
        conn=Qubole.agent()
        data={"status":"kill"}
        conn.put(self.my_element_path, data)


    def get_log(self):
        log_path = self.meta_data['logs_resource']
        conn=Qubole.agent()
        r=conn.get_raw(log_path)
        return r.text

    def get_results(self):
        result_path = self.meta_data['results_resource']
        conn=Qubole.agent()
        r = conn.get(result_path)
        if r.get('inline'):
            return r['results'] 
        else:
            # TODO - this will be implemented in future
            log.error("Unable to download results, please fetch from S3")


class HiveCommand(Command):
    pass

class HadoopCommand(Command):
    pass

class PigCommand(Command):
    pass

class DbImportCommand(Command):
    pass
