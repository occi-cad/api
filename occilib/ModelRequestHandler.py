""" 

    RequestHandler.py

    Handle a model request (with CadScriptRequest) from API:
    1. Checking file system cache 
    2. Submitting the request to compute queue (RMQ)

"""

import os
import re
import logging
import base64
import tempfile

from fastapi import HTTPException
from fastapi.responses import Response, RedirectResponse, JSONResponse, FileResponse

from typing import Dict, Any

import asyncio
from asgiref.sync import sync_to_async

import nest_asyncio # see:  https://github.com/erdewit/nest_asyncio
nest_asyncio.apply() # enables us to plug into running loop of FastApi

from contextlib import suppress

from celery.result import AsyncResult

from .models import ModelRequestInput
from .Param import ParamConfigBase
from .CadScript import CadScriptRequest, CadScriptResult
from .CadLibrary import CadLibrary

from kombu import Exchange, Queue

class ModelRequestHandler():

    #### SETTINGS ####
    WAIT_FOR_COMPUTE_RESULT_UNTIL_REDIRECT = 30 # in seconds
    REDIRECTING_COMPUTING_STATE = 'job'
    CAD_SCRIPT_ENGINES = { 'cadquery' : 'OCCI_CADQUERY', 
                           'archiyou' : 'OCCI_ARCHIYOU' } # execution engines and their flags in .env for turning on or off

    #### END SETTINGS
    no_workers:bool = False
    library:CadLibrary
    celery = None
    celery_connected:bool = False
    available_scriptengine_workers = [] # cadquery, archiyou = queue names

    def __init__(self, library:CadLibrary, no_workers:bool=False):
            
        self._setup_logger()

        self.no_workers = no_workers
        self.library = library

        if not isinstance(self.library, CadLibrary):
            self.logger.error('ModelRequestHandler::__init__(library): Please supply library!') 

        # setup workers
        if not no_workers:
            # only import in connected mode because cadquery and OpenCascade (OCP) trips up locally a lot
            from .celery_tasks import celery as celery_app
            self.celery = celery_app # from import celery_tasks

            self.setup_celery_exchanges()

            if self.check_celery() is False:
                self.logger.error('ModelRequestHandler::__init__(library): Celery is not connected. We cannot send requests to compute! Check .env config.') 
            else:
                self.logger.info('ModelRequestHandler::__init__(library): Celery is connected to RMQ succesfully!')
        else:
            self.logger.info('ModelRequestHandler::__init__(library): **** Celery is disabled because workers are turned off! See no_workers flag ****')
        

    def setup_celery_exchanges(self):
        '''
            Manually set up exchanges and bindings
            This is somewhat hacky. Archiyou exhange is not setup automatically (because worker is running nodejs)
        '''
        conn = self.celery._acquire_connection()                                                                                                                                                                       
        exchange = Exchange("archiyou", type="direct", channel=conn.channel())                                                                                                                                                                                      
        queue = Queue(name="archiyou", exchange=exchange, routing_key="archiyou")                                                                                                                                                                   
        queue.maybe_bind(conn)                                                                                                                                                                                                                      
        queue.declare() 


    def check_celery(self) -> bool:
        '''
            Check if Celery can connect to its backend(s) and if there are workers for both cad script engines
        '''

        self.logger.info('**** CHECKING CELERY CONNECTIONS ****')

        try:
            self.celery.control.inspect(timeout=1.0).ping()
            self.celery_connected = True
            self.logger.info('ModelRequestHandler::check_celery: Connected to RMQ backend!')
            
            # check connected workers and their queues (see: https://docs.celeryq.dev/en/stable/_modules/celery/app/control.html#Inspect)
            if self.celery.control.inspect().active_queues() is None:
                self.logger.error('ModelRequestHandler::check_celery: No workers connected to compute queues')
                return False

            # NOTE: inspecting active queues do not work with archiyou node-celery worker
            ay_flag = os.environ.get(self.CAD_SCRIPT_ENGINES['archiyou'])
            if ay_flag == '1' and self.test_archiyou_worker() is True:
                if 'archiyou' not in self.available_scriptengine_workers:
                    self.available_scriptengine_workers.append('archiyou') 
            
            # test CQ connection through Celery queues
            cq_flag = os.environ.get(self.CAD_SCRIPT_ENGINES['cadquery'])
            if cq_flag == '1':
                for worker_host, worker_queue_info in self.celery.control.inspect().active_queues().items():
                    queue_name = worker_queue_info[0].get('name') if len(worker_queue_info) > 0 else None
                    if queue_name:
                        self.logger.info(f'* Worker "{worker_host}" connected to queue: "{queue_name}"')
                        if queue_name not in self.available_scriptengine_workers:
                            self.available_scriptengine_workers.append(queue_name)

            script_engine_list = '\n - '.join(self.available_scriptengine_workers)
            self.logger.info(f'*** Connected workers for script engine: \n - {script_engine_list}')

            #  check if we have the workers that we configured in .env
            for engine,flag in self.CAD_SCRIPT_ENGINES.items():
                engine_flag = os.environ.get(flag)
                if engine_flag == '1' and engine not in self.available_scriptengine_workers:
                    self.logger.error(f'ModelRequestHandler::check_celery: Cad engine "{engine}" has no workers available!')
                    return False

            return self.celery_connected

        except Exception as e:
            self.logger.error(e)
            return False

    def test_archiyou_worker(self) -> bool:

        from .celery_tasks import compute_job_archiyou # dynamic import because celery_tasks need to be avoided in no_workers mode

        try:
            result = compute_job_archiyou.apply_async(args=[], kwargs={ 'script' : None })
            result.get()
            return True
        except Exception as e:
            self.logger.error(e)
            return False

    def get_celery_task_method(self, requested_script:CadScriptRequest) -> Any: # TODO: nice typing

        from .celery_tasks import compute_job_archiyou, compute_job_cadquery # dynamic import because celery_tasks need to be avoided in no_workers mode

        TASK_METHODS_BY_ENGINE = {
            'cadquery' : compute_job_cadquery, 
            'archiyou' : compute_job_archiyou,
        }
        DEFAULT_ENGINE = 'cadquery'

        task_method = TASK_METHODS_BY_ENGINE.get(requested_script.cad_engine)
        if task_method is None:
            self.logger.error(f'ModelRequestHandler::get_celery_task_method: Cannot get Celery task method: cad_engine "{requested_script.cad_engine}" is unknown! Defaulted to "cadquery"')
            task_method = TASK_METHODS_BY_ENGINE[DEFAULT_ENGINE]
        
        return task_method

    def script_engine_has_workers(self, requested_script:CadScriptRequest) -> bool:

        return requested_script.cad_engine in self.available_scriptengine_workers
    

    def handle_requested_script_result(self, req:ModelRequestInput, script_result:CadScriptResult, new:bool=False) -> Response|FileResponse:
        # We got a compute result in time to respond directly to the API client
        # Based on settings in the request we return different things
        if script_result:
            if script_result.results.success is True:
            
                if new:
                    self.library.checkin_script_result_in_cache(script_result)
                
                # continue with see what we need to return based on script_result.request
                # the user requested a model or the full result
                if req.script_special_requested_entity is None:
                    # API user requested a full CadScriptResult response
                    if req.output == 'full':
                        # only allow requested format to be outputted
                        script_result = self.library._apply_single_model_format(script_result)
                        return Response(content=script_result.json(), media_type="application/json") # don't parse the content, just output
                    else: # output = model
                        # only a specific format model as output 
                        # we skip loading the result.json and serve the model file data directly
                        return script_result.get_model_file_response(format=req.format)
                        
                # special entities from result
                else:
                    # request files or specific files of this result
                    filenames = list((script_result.results.files or {}).keys())
                    if req.script_special_requested_entity == 'files':
                        return filenames
                    # a file.ext in req.script_special_requested_entity
                    elif re.match('[^\.]+\.[^$]+$', req.script_special_requested_entity): 
                        if req.script_special_requested_entity in filenames: # if indeed that file exists
                            file_data_base64 = script_result.results.files.get(req.script_special_requested_entity)
                            file_data_binary = base64.b64decode(file_data_base64)
                            with tempfile.NamedTemporaryFile(
                                                mode='wb', # for now only binary files!
                                                delete=False, 
                                                suffix=f".{format}") as f:
                                f.write(file_data_binary)
                                return FileResponse(f.name, filename=req.script_special_requested_entity)


            else:
                errors_str = ','.join(script_result.results.errors)
                raise HTTPException(status_code=404, 
                    detail=f"""Error executing the script '{script_result.name}':'{errors_str}'\nPlease notify the OCCI library administrator!""")
        else:
            self.logger.error('ModelRequestHandler::handle_requested_script_result: No script result given!')


    async def handle(self, req:ModelRequestInput) -> RedirectResponse | JSONResponse | FileResponse:
        """
            Handle request coming from API
            
            Normal operation:
                Prepare a CadScriptRequest instance with request in it
                and get from cache or submit to compute workers
            Special entity request 'req.script_special_requested_entity':
                Print out versions or params for introspection

        """
        if req is None or not isinstance(req, ModelRequestInput):
            m = 'ModelRequestHandler::handle(script): No request received'
            self.logger.error(m)
            raise HTTPException(500, detail=m) # raise http exception to give server error

        script = self.library.get_script_request(org=req.script_org, name=req.script_name) # this gets the latest version

        # special entity request that don't need calculation: versions, params, presets
        if req.script_special_requested_entity is not None:
            # properties of general script
            if req.script_special_requested_entity == 'versions':
                return self.library.get_script_versions(script) if script else None
            elif req.script_special_requested_entity == 'params':
                return script.params if script else None
            elif req.script_special_requested_entity == 'presets':
                return script.param_presets if script else None
            # For files or a specific file: see below

        # always make sure we have a version, redirect if needed
        if req.script_version is None:
            # return to default version, forward params
            return RedirectResponse(f'/{script.namespace}/{script.version}{req.get_query_string()}') 

        requested_script = self._req_to_script_request(req)
        requested_script.hash() # set hash based on params

        if req.no_cache is False and self.library.is_cached(requested_script):
            # TODO: check if the requested products are there (like docs)
            self.logger.info(f'**** {requested_script.name}: CACHE HIT FOR REQUEST script "{req.script_name}" - model hash "{requested_script.hash()}" [format="{req.format}" output="{req.output}"] ****')
            cached_script_result = self.library.get_cached_script(requested_script)
            return self.handle_requested_script_result(req, cached_script_result, new=False)
            
        else:
            # no cache - first computing status
            computing_job = self.library.check_script_model_computing_job(script=requested_script, script_instance_hash=requested_script.hash())
            if computing_job is not None:
                # refer back to compute url
                return self.go_to_computing_job_url(requested_script,computing_job.celery_task_id, set_compute_status=False)
            else:
                # no cache: submit to workers
                if self.celery_connected:
                    
                    if self.script_engine_has_workers(requested_script) is False:
                        raise HTTPException(500, detail=f'No workers available for cad script engine "{requested_script.cad_engine}". Try again or report to the administrator!') # raise http exception to give server error

                    task:AsyncResult = self.get_celery_task_method(requested_script).apply_async(args=[], kwargs={ 'script' : requested_script.json() })
                    result_or_timeout = self.start_compute_wait_for_result_or_redirect(req, task)

                    # wait time is over before compute could finish:
                    if result_or_timeout is None:
                        # no result
                        return self.go_to_computing_job_url(requested_script, task.id)
                    else:
                        # result in time! 
                       return self.handle_requested_script_result(req, result_or_timeout, new=True)
                else:
                    # local debug
                    self.logger.warn('ModelRequestHandler::handle(): Compute request without celery connection. You are probably debugging?')
                    return requested_script

    def start_compute_wait_for_result_or_redirect(self, req:ModelRequestInput, task:AsyncResult, wait_time:int=None) -> CadScriptResult: # time in seconds
        """
            Async wait for a given number of seconds T
            if t < T return compute result directly to API client 
            otherwise redirect to computing url
            
            inspired by: https://stackoverflow.com/questions/53967281/what-would-be-promise-race-equivalent-in-python-asynchronous-code
        """

        wait_time = wait_time or self.WAIT_FOR_COMPUTE_RESULT_UNTIL_REDIRECT

        def coro_is_wait(coro):
            return '.wait' in str(coro) # TODO: not really robust, make this better

        # simple async waiting
        async def wait(t):
            await asyncio.sleep(t)
            self.logger.warn(f'ModelRequestHandler::start_compute_wait_for_result_or_redirect: Wait for direct compute result elapsed: {t} seconds!')
            return False

        async def compute_and_handle_result():
            result = await self.result_to_async(task)()
            return result
        
        '''
            IMPORTANT: once a long compute (compute_and_handle_result) is slower than wait it does not continue any more (for some reason)
            Start another async task to monitor the result and handle it
        '''
        async def monitor_for_celery_result(req:ModelRequestInput,task_id) -> CadScriptResult:
            celery_task_result = AsyncResult(task_id)
            while not celery_task_result.ready():
                await asyncio.sleep(1)
            result_dict = celery_task_result.result
            self.handle_requested_script_result(req, CadScriptResult(**result_dict), new=True) # convert to CadScriptResult
            return True

        loop = asyncio.get_running_loop()
        racing_tasks = set()
        racing_tasks.add(loop.create_task(wait(wait_time)))
        racing_tasks.add(loop.create_task(compute_and_handle_result()))

        # see asyncio.wait: https://docs.python.org/3/library/asyncio-task.html#asyncio.wait
        done_first, pending = loop.run_until_complete(asyncio.wait(racing_tasks, return_when=asyncio.FIRST_COMPLETED))
        
        """
            !!! TODO: DEBUG this message:
            RuntimeError: Cannot enter into task <Task pending name='Task-1' coro=<Server.serve() running at /usr/local/lib/python3.10/site-packages/uvicorn/server.py:80> wait_for=<Future finished result=None> cb=[_run_until_complete_cb() at /usr/local/lib/python3.10/asyncio/base_events.py:184, WorkerThread.stop()]> while another task <Task pending name='Task-4' coro=<RequestResponseCycle.run_asgi() running at /usr/local/lib/python3.10/site-packages/uvicorn/protocols/http/h11_impl.py:407> cb=[set.discard()]> is being executed.
            It mostly happens the first request without any noticable consequences
        """

        result = None
        for coro in done_first: # in theory there could be more routines, but probably either wait or compute result
            try:
                # return the first
                result = coro.result()
            except TimeoutError:
                return None
        
        
        for pending_coro in pending:
            # We have to cancel the all remaining coroutines to continue
            # NOTE that the compute task is going through Celery anyways - only we can't follow it anymore
            # We start a new async coroutine to monitor it

            pending_coro.cancel()
            with suppress(asyncio.CancelledError):
                loop.run_until_complete(pending_coro)

            # wait for celery compute task with seperate coroutine
            if not coro_is_wait(pending_coro):
                loop.create_task(monitor_for_celery_result(req, task.id))
        
        if result is not False:
            try: 
                script_result = CadScriptResult(**result) # convert dict result to CadScriptResult instance
            except Exception as e:
                self.logger.info(result.get('results')['tables'])
                self.logger.error(f'ModelRequestHandler::start_compute_wait_for_result_or_redirect(). Mailformed ERROR: "{e}"')
        else:
            script_result = None

        return script_result

    
    def result_to_async(self, task:AsyncResult): 
        """
            Current Celery (v5) does not support asyncio just yet. See: https://github.com/celery/celery/issues/6603
            We use asgiref.sync_to_async to turn AsyncResult.get() into a async method
            asgiref uses threads (see: https://github.com/django/asgiref/blob/main/asgiref/sync.py)
        """
        async def wrapper(*args, **kwargs):
            compute_result:CadScriptResult = await sync_to_async(task.get,thread_sensitive=True)() # includes results. thread_sensitive is needed for fast batch processing
            return compute_result
        return wrapper

    def go_to_computing_job_url(self, script:CadScriptRequest, task_id:str, set_compute_status:bool=True) -> RedirectResponse:
        """
            When compute result takes longer then WAIT_FOR_COMPUTE_RESULT_UNTILL_REDIRECT
            Redirect to compute status url which the user can query untill the compute task is done 
            and then automatically gets redirected
        """
        if set_compute_status:
            self.library.set_script_model_is_computing(script, task_id)

        return RedirectResponse(f'/{script.org}/{script.name}/{script.version}/{script.hash()}/{self.REDIRECTING_COMPUTING_STATE}/{task_id}')


    def _req_to_script_request(self,req:ModelRequestInput) -> CadScriptRequest:

        if not isinstance(req, ModelRequestInput):
            raise HTTPException(500, detail='ModelRequestHandler::_req_to_script_request(req): No request given!') # raise http exception to give server error
        if not isinstance(self.library, CadLibrary):
            raise HTTPException(500, detail='ModelRequestHandler::_req_to_script_request(req): No library loaded. Cannot handle request!') # raise http exception to give server error

        script_request = self.library.get_script_request(org=req.script_org, name=req.script_name, version=req.script_version)

        if not script_request:
            raise HTTPException(500, detail=f'ModelRequestHandler::_req_to_script_request(req): Cannot get script with name "{req.script_name}" and version "{req.script_version}" [optional] from library!')

        script_request.request.format = req.format
        script_request.request.output = req.output # set format in which to return to API (full=json, model return results.models[{format}])
        script_request.request.settings = req.settings
        
        # in req are also the flattened requested param values
        filled_params:Dict[str,Any] = {} 
        
        if script_request.params:
            for name, param in script_request.params.items():
                related_filled_param = getattr(req, name, None)
                if related_filled_param is not None:
                    filled_params[name] = related_filled_param

            script_request.request.params = filled_params

        # NOTE: script can also have no parameters!    
        return script_request

    #### CACHE PRE-CALCULATION ####

    async def compute_script_request(self, script:CadScriptRequest) -> CadScriptResult:

        task:AsyncResult = self.get_celery_task_method(script).apply_async(args=[], kwargs={ 'script' : script.json() })
        result_script_dict = await self.result_to_async(task)()
        result_script = CadScriptResult(**result_script_dict)

        return result_script


    #### UTILS #### 
        
        
    def _setup_logger(self):

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(level=logging.INFO)

        try:
            handler = logging.StreamHandler()
            handler.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)-4s %(message)s')
            handler.setFormatter(formatter)

            if (self.logger.hasHandlers()):  # see: http://tiny.cc/v5w6gz
                self.logger.handlers.clear()

            self.logger.addHandler(handler)

        except Exception as e:
            self.logger.error(e)
