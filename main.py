import os
import uvicorn as uvicorn

from fastapi import FastAPI, HTTPException, Depends, Response, status
from fastapi.middleware.cors import CORSMiddleware

from celery.result import AsyncResult
from dotenv import dotenv_values

from typing import List, Dict

from occilib.CadLibrary import CadLibrary
from occilib.CadScript import CadScriptResult
from occilib.ApiGenerator import ApiGenerator
from occilib.models import SearchQueryInput
from occilib.Admin import Admin

#### IMPORTANT NOTICES ####

""" 
    Because currently the 'database' of a library and its cache is on server disk 
    it is seriously recommended to run one FastAPI worker to avoid concurrent writing issues.
    For high-performant contexts we need to move the scriptslibrary to a database and the cache 
    to a cloud storage. 
"""

#### CONFIG HANDLING - FROM ENV VARIABLES (SET IN DOCKER CONTAINER) OR .ENV FILE (FOR LOCAL DEBUG) ####

CONFIG = {
    **os.environ, # From Docker
    **dotenv_values('.env'), # From local .env file
    # NOTE: during dev os.environ can have preset values, so we need to overwrite them with .env values
    # TODO: Check if this works the best in Docker
}

#### START MAIN INSTANCES ####

requested_workers = []
if CONFIG.get('OCCI_CADQUERY') == '1': requested_workers.append('cadquery')
if CONFIG.get('OCCI_ARCHIYOU') == '1': requested_workers.append('archiyou') 

library = CadLibrary(rel_path='./scriptlibrary', workers=requested_workers)
scripts = library.scripts
no_workers = CONFIG.get('LOCAL_DEBUG_MODE' ) == '1' or (CONFIG.get('OCCI_CADQUERY') == '0' and CONFIG.get('OCCI_ARCHIYOU') == '0')
api_generator = ApiGenerator(library, no_workers=no_workers)

#### CHECK CONNECTION TO RMQ ####

if not no_workers and api_generator.request_handler.check_celery() is False:
    raise Exception('*** RESTART API - No Celery connection and/or missing workers: Restart API ****') 

app = FastAPI(openapi_tags=api_generator.get_api_tags(scripts))
# enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
api_generator.generate_endpoints(api=app, scripts=scripts)
library.set_api_generator(api_generator)

admin = Admin(app, api_generator, passphrase=CONFIG.get('OCCI_ADMIN_PASSPHRASE'))


@app.get("/")
async def index():
    return {
        'library': CONFIG.get('OCCI_LIBRARY_NAME', 'unnamed OCCI library. See settings in .env'),
        'maintainer': CONFIG.get('OCCI_LIBRARY_MAINTAINER'),
        'maintainer_email': CONFIG.get('OCCI_LIBRARY_MAINTAINER_EMAIL'),
    }

#### COMPUTING JOB STATUS ####

@app.get('/{script_org}/{script_name}/{script_version}/{script_instance_hash}/job/{celery_task_id}')
async def get_model_compute_task(script_org:str, script_name:str, script_version:str, script_instance_hash:str, celery_task_id:str, response: Response):
    """
        If a compute takes longer then a defined time (see ModelRequestHandler.WAIT_FOR_COMPUTE_RESULT_UNTILL_REDIRECT)
        The user is redirected to this url which supplies information on the job
        The client then needs to refresh that job endpoint until a result or error is given 
        We leverage the result system of Celery (timeout 1 day) to have cache compute results for the infite compute jobs
        The finite compute jobs are cached on disk

        We decided not to do redirects after result is done because its simpler, less calls for client , more robust
        and we can leverage Celery cache for 'infinite' script results. 
        Otherwise we have to maintain a tmp cache for these results on disk

        Multiple clients can be refered to the same compute job URL without creating a new celery task.
        But using the Celery task_id keeps the results available 
        If the compute is done the .compute file is cleaned automatically

        TODO: introduce a scenario for parallel API workers with centralized storage of compute status flag
    """
    
    celery_task_result:AsyncResult = AsyncResult(celery_task_id)

    if celery_task_result.state in ['PENDING', 'FAILURE']: # pending means unknown because we directly set state to SEND (see ModelRequestHandler.setup_celery_publish_status())
        raise HTTPException(status_code=404, detail="Compute task not found or in error state. Please go back to original request url!")

    elif celery_task_result.ready():
        '''
        NOTE: we lean on the Celery result system to have cache for 'infinite' script variants
         results are automatically reset after one day: https://docs.celeryq.dev/en/stable/userguide/configuration.html#std-setting-result_expires
        ''' 
        script_result_dict = celery_task_result.result
        script_result = CadScriptResult(**script_result_dict)
        library._apply_single_model_format(script_result)
        
        return script_result.dict() # output as dict
    else:
        # the job info we can get from a temporary file (.compute) in directory
        script = library.get_script_request(org=script_org, name=script_name, version=script_version )
        job = library.check_script_model_computing_job(script, script_instance_hash)
        if not job:
           raise HTTPException(status_code=404, detail="Compute task not found or in error state. Please go back to original request url!") 
        job.celery_task_status = celery_task_result.status
        
        response.status_code = status.HTTP_202_ACCEPTED # special code to signify the job is still being processed
        return job.dict() # return status of job 
            

#### SEARCH ####
@app.get('/search')
async def search(inp:SearchQueryInput = Depends()) -> List[Dict]:
    if inp.q is None: # return all scripts
        return [s.dict() for s in library.scripts]
    else:
        return library.search(inp.q)

@app.post('/search')
async def search(inp:SearchQueryInput) -> List[Dict]:
    if inp.q is None: # return all scripts
        return [s.dict() for s in library.scripts]
    else:
        return library.search(inp.q)


#### TEST SERVER ####
if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8090, workers=1)

