import os
import json # TODO: use orjson?
import base64
import time
import random

from celery import Celery # docs: https://docs.celeryq.dev/en/stable/userguide/tasks.html#basics
from celery.signals import after_task_publish

from pathlib import Path

from .CadScript import CadScriptResult
from .models import ModelResult

from dotenv import dotenv_values
CONFIG = dotenv_values()  

celery = Celery(__name__) # celery app
celery.conf.broker_url = CONFIG.get('CELERY_BROKER_URL') or 'amqp://guest:pass@localhost:5672'
celery.conf.result_backend = CONFIG.get('CELERY_RESULT_BACKEND') or 'rpc://localhost:5672' # for RMQ as backend see: https://github.com/celery/celery/issues/6384 
celery.conf.task_routes = {
            'cadquery.*': { 'queue': 'cadquery', 'routing_key' : 'cadquery' }, # default exchange but different key
            'archiyou.*': { 'queue': 'archiyou', 'routing_key' : 'archiyou' }
        }
celery.conf.task_default_exchange = 'cadquery'
celery.conf.task_default_exchange_type = 'direct'
celery.conf.task_default_routing_key = 'cadquery'

# IMPORTANT: set working directory outside the project dir 
# we presume that cqworker will run in a docker container
Path("/cqworkertmp").mkdir(parents=True, exist_ok=True)
os.chdir('/cqworkertmp')

# Only import cadquery if enabled in .env
if CONFIG.get('OCCI_CADQUERY') == '1':
    import cadquery
    import cadquery.cqgi as cqgi

@celery.task(name='cadquery.compute', bind=True, delivery_mode=1) # delivery mode 1 for non persistence
def compute_job_cadquery(self,script:str): # json of CadScript 
    time_start = time.time()
    script_result = CadScriptResult(**json.loads(script)) # parse CadScriptRequest json as CadScciptResult
    result_response = ModelResult()

    #### REAL EXECUTION IN CADQUERY ####
    # !!!! ONLY TRUSTED CODE !!!!
    '''
        Some NOTES to extend the worker:
        - capture Exception and add to script.result.errors
        
    '''
    if script_result.cad_engine == 'cadquery':
        param_values = script_result.get_param_values_dict()
        build_result = cqgi.parse(script_result.code).build(build_parameters=param_values, build_options={} )
        # See docs for BuiltResult: https://github.com/CadQuery/cadquery/blob/master/cadquery/cqgi.py
        
        if build_result.success is True:
            # TODO: multiple calling show_object() populates a list of results: can we handle those?
            result = build_result.results[-1].shape  # for now use the last result


            # output main formats: step (text), gltf (binary) and stl (binary) 
            # NOTE: To be safe add extra random token to filename for collisions between making the file and deleting (for example when requests are cancelled)
            random_token = random.randint(0, 1000000)
            local_step_file = f'result_{script_result.request.hash}_{random_token}.step' 
            local_stl_file = f'result_{script_result.request.hash}_{random_token}.stl'

            cadquery.exporters.export(result, local_step_file, cadquery.exporters.ExportTypes.STEP)
            #cadquery.exporters.export(build_result, 'result.gltf', cadquery.exporters.ExportTypes.GLTF) # not in yet?
            cadquery.exporters.export(result, local_stl_file , cadquery.exporters.ExportTypes.STL)
            

            with open(local_step_file, 'r') as f:
                result_response.models['step'] = f.read()
            os.remove(local_step_file) # to be sure: clean up

            '''
            with open('result.gltf', 'rb') as f:
                result_response.models['gltf'] = base64.b64encode(f.read()).decode('utf-8')
            os.remove('result.gltf') # to be sure: clean up
            '''
    
            with open(local_stl_file, 'rb') as f:
                result_response.models['stl'] = base64.b64encode(f.read()).decode('utf-8')
            os.remove(local_stl_file) # to be sure: clean up
            result_response.success = True
        
        # there was an error
        else:
            result_response.success = False
            result_response.errors.append(str(build_result.exception))

        # finalize and return CadScriptResult as json
        result_response.duration = round((time.time() - time_start) * 1000) # in ms 
        result_response.task_id = str(self.request.id)
        script_result.results = result_response
        

    #### END EXECUTION

    return script_result.dict()

#### DUMMY ARCHIYOU COMPUTE TASK ####
@celery.task(name='archiyou.compute', bind=True, delivery_mode=1)
def compute_job_archiyou(self,script:str):
    # dummy for sending to broker: real work is done by archiyou nodejs worker
    return None


#### EXTRA SIGNAL WHEN PUBLISHED ####
'''
    To identify (un)known task_ids
    see: https://stackoverflow.com/questions/9824172/find-out-whether-celery-task-exists
'''

@after_task_publish.connect
def update_sent_state(sender=None, headers=None, **kwargs):
    task = celery.tasks.get(sender)
    backend = task.backend if task else celery.backend
    backend.store_result(headers['id'], None, "SENT")

