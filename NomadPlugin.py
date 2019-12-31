# -*- coding: utf-8 -*-

from airflow.plugins_manager import AirflowPlugin
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults

import json
import nomad
import time 

class NomadHook(BaseHook):
    """
    Interact with a Nomad cluster
    :param nomad_conn_id: ID of the Airflow connection where
        credentials and extra configuration are stored
    :type nomad_conn_id: str
    """
    def __init__(self,
                 nomad_conn_id='nomad_default'
                 ):

        conn = self.get_connection(nomad_conn_id)
        if not conn.host:
            raise AirflowException('No Nomad URL provided')
        self.__host = conn.host
        self.__secure = False
        
    def get_conn(self):
        client = nomad.Nomad(
            host = self.__host, 
            secure = self.__secure
        )
        return client

class NomadBatchOperator(BaseOperator):
    """
    Run a nomad batch job.
 
    :poram job_spec: the nomad job spec in JSON format. Note HCL is NOT currently supported. 
    :type job_spec: str
    :param nomad_conn_id: the airflow connection details for the nomad cluster 
    :type job_spec str:
    :param secure: should the connection to the nomad apis be secure
    :type secure: bool
    """
    @apply_defaults
    def __init__(self,
                 job_spec,
                 nomad_url = "127.0.0.1", 
                 nomad_conn_id=None,
                 secure = False,
                 ttl = 300,
                 failed_fraction = 0.0,
                 *args,
                 **kwargs
                 ):
        super().__init__(*args, **kwargs)

        self.job_spec = job_spec
        self.nomad_conn_id = nomad_conn_id
        self.nomad = None
        self.nomad_url = nomad_url
        self.secure = secure 
        self.ttl = ttl
        self.failed_fraction = failed_fraction
        print("************* Hello")

    def get_hook(self):
        return NomadHook(self.nomad_conn_id)

    def normalize_job_spec(self, job_spec):
        if job_spec.endswith(".nomad"):
            # assume its a hcl file
            with open(job_spec, "r") as fh:
                try:
                    job_raw_nomad = fh.read()
                    job_dict = self.nomad.jobs.parse(job_raw_nomad)
                except nomad.api.exceptions.BadRequestNomadException as err:
                    raise AirflowException('Unable to parse HCL file' + err.nomad_resp.text)

            return job_dict
        elif type(job_spec)==str:
            # assume its json 
            return json.loads(job_spec)
        elif type(job_spec)==dict:
            return job_spec
        else:
            raise AirflowException('Unknown job format')
        
    def execute(self, context):
        self.log.info('Starting nomad batch job')
        if self.nomad_conn_id:
            self.nomad = self.get_hook().get_conn()
        else:
            self.nomad = nomad.Nomad(
                host = self.nomad_url,
                secure = self.secure
                )

        job_json = self.normalize_job_spec(self.job_spec)
        job_id = job_json["ID"]
        self.log.info("Starting the nomad job %s" % job_id)
        self.nomad.jobs.register_job({"Job" : job_json})
            
        start = time.time()

        self.log.info("waiting for nomad job to complete %s" % job_id)

        while True:
            summary = self.nomad.job.get_summary(job_id)
            last_check = time.time()

            states = []
            for group in summary["Summary"].values():
                inprogress = sum([ group[x] for x in ["Queued", "Running", "Starting"]])
                failed = sum([ group[x] for x in ["Failed", "Lost"]]) 
                done = sum([ group[x] for x in ["Complete"]]) 
                total_jobs = done + failed + inprogress
                if inprogress == 0 and float(done)/target > self.failed_fraction:
                    # complete     
                    
                break;
            elif last_check-start > self.ttl:
                #self.nomad.job.deregister_job(job_id)
                raise  AirflowException('nomad job out of time')

        if inprogress == 0 and float(failed) / total_jobs > self.failed_fraction:
            raise AirflowException(f'{failed} out of {total_jobs} nomad batch jobs failed' )
        else:
            # good enough!
            return
            
            
            
        

class NomadPlugin(AirflowPlugin):
    name = "nomad_plugin"
    operators = [NomadBatchOperator]
