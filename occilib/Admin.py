'''
    Admin.py
        Extends OCCI api with administration functions like:
        - publish/unpublish
        - basic edits for example of description, author, name, params

    Entities:
     - PublishRequest: The request for publication of a given CadScript. Return status and reference to new CadScript and PublicationJob
     - PublishJob: the process of publication including pre-calculation of which the user can ask the status and progress

    Endpoints:
    - /admin/publish - main endpoint to publish a script
    - /admin/publish/{{PublicationJob ID}} - status of PublicationJob
    - /admin/unpublish/{{CadScript ID}} -  
        
'''

import logging
import uuid
from datetime import datetime
from typing import Dict
from enum import Enum
import asyncio


from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from pydantic import BaseModel

import secrets
import string

from .CadScript import CadScript
from .ApiGenerator import ApiGenerator
from .models import ComputeBatchStats

security = HTTPBasic()

#### ADMIN MODELS ####

class PublishRequest(BaseModel):
    pre_calculate:bool=False # request a pre-compute of all models
    script:CadScript=None # the script you want to publish

class PublishJobStatus(str,Enum):
    success = 'success'
    computing = 'computing'
    error = 'error'

class PublishJob(BaseModel):
    id:str = str(uuid.uuid4())
    created_at:datetime = datetime.now()
    updated_at:datetime = datetime.now()
    script:CadScript
    status:PublishJobStatus = None
    stats:ComputeBatchStats = None


#### ADMIN CLASS ####

class Admin:

    #### SETTINGS ####
    SCRIPT_ORG_MIN_CHARS = 4
    SCRIPT_NAME_MIN_CHARS = 4
    SCRIPT_CODE_MIN_CHARS = 10
    SECURITY_ADMIN_USERNAME = 'admin'

    #### END SETTINGS ####

    api:FastAPI = None # reference to FastAPI app instance
    api_generator:ApiGenerator = None
    passphrase:str # strong passphase to protect admin enpoints
    publish_jobs:Dict[str, PublishJob] = {} # keep track of publish jobs and there stats

    def __init__(self, api:FastAPI=None, api_generator:ApiGenerator=None, passphrase:str=None):

        self._setup_logger()

        if not isinstance(api, FastAPI):
            self.logger.error('Please supply a reference to FastAPI app instance!')
        elif not isinstance(api_generator, ApiGenerator):
            self.logger.error('Please supply a reference to OCCI ApiGenerator!')
        else:
            self.api = api
            self.api_generator = api_generator

            self.passphrase = passphrase if passphrase is not None else self._generate_passphrase()
            if passphrase is None:
                self.logger.warn('**** IMPORTANT: PLEASE SUPPLY A STRONG PASSPHRASE. NOW WE GENERATED ONE:\n{self.passpharse}\n[Use this with user "admin" to access the endpoints]')
                
            self._add_admin_endpoints()

        
    def _add_admin_endpoints(self):

        if self.api is None:
            self.logger.error('Cannot add admin endpoints without reference to FastAPI app!')
        else:
            api = self.api
            # /admin/publish
            @api.post('/admin/publish')
            async def publish(req:PublishRequest, credentials: HTTPBasicCredentials = Depends(self._validate_credentials)) -> dict:
                # !!!! BEWARE !!!! Using response types in these route methods (like -> PublishJob ) 
                # seems to force some re-parsing (and somehow skipping upgrading params in CadScript.params ) !!!!
                job = await self._handle_publish_request(req)
                return job.dict()
            
            # /admin/publish/{job_id}
            @api.get('/admin/publish/{job_id}')
            async def get_pub_job(job_id:str, credentials: HTTPBasicCredentials = Depends(self._validate_credentials)) -> dict:
                # !!!! BEWARE !!!! Using response types in these route methods (like -> PublishJob ) 
                # seems to force some re-parsing (and somehow skipping upgrading params in CadScript.params ) !!!!
                return self._get_publish_job(job_id).dict()
            
            # /admin/unpublish
            @api.post('/admin/unpublish/{script_id:int}')
            async def unpublish(script_id:int, credentials: HTTPBasicCredentials = Depends(self._validate_credentials)):
                return script_id
            
    def _validate_credentials(self, credentials: HTTPBasicCredentials = Depends(security)) -> bool:

        r = secrets.compare_digest(credentials.username.encode("utf8"), self.SECURITY_ADMIN_USERNAME.encode('utf8')) \
                and secrets.compare_digest(credentials.password.encode("utf8"), self.passphrase.encode('utf8'))
        if not r:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect usernamd and password",
                headers={"WWW-Authenticate": "Basic"},
            )

        return credentials
    
    async def _handle_publish_request(self, req:PublishRequest) -> PublishJob:
        """ 
            Handles a request to publish a given script
        """

        # Do the needed checks around unique namespaces
        self._check_publish_request(req) # will raise Error

        # Save the script in OCCI library on disk
        if self.api_generator.library.add_script(req.script) is False:
            raise HTTPException(status_code=400, detail='Cannot publish script. It already exists. Try another name or version tag!')
        
        # Add endpoint for this script version
        self.api_generator._generate_version_endpoint(script=req.script)
        # IMPORTANT: this probably does not yet overwrite existing route. See: https://github.com/tiangolo/fastapi/discussions/8489
        # A restart of the server - or API is still required here!
        self.api_generator._generate_default_version_endpoint(script=req.script) 

        # If request (and possible) start pre-calculation of models into cache asynchronously
        if not req.pre_calculate or not req.script.is_cachable():
            # no compute requested (or available)
            return PublishJob(script=req.script, status='success')
        else:
            # lets do a pre-caching compute
            batch_id = str(uuid.uuid4())
        
            def on_done(batch_id) -> bool:
                self.publish_jobs[pub_job.id].status = 'success'

            asyncio.create_task(
                # TODO: only_params as parameter
                self.api_generator.library.compute_script_cache_async(script=req.script, only_params=None, compute_batch_id=batch_id, on_done=on_done)
            ) # don't await this
            # Report back to the API user about the PublishJob
            pub_job = PublishJob(id=batch_id, script=req.script, status='computing')

            self.publish_jobs[pub_job.id] = pub_job

            return pub_job
    
    
    def _get_publish_job(self, id:str) -> PublishJob:
        """
            Get the state of the publish job
            !!!! TODO: We need to centralize job info in Redis if we want to use multiple API instances
        """
        pub_job = self.publish_jobs.get(id)
        if pub_job:
            pub_job.stats = self.api_generator.library._compute_batch_stats.get(id)
            return pub_job
        
        raise HTTPException(status_code=404, detail=f'Cannot find publish job widh id "{id}"!')
        

    def _check_publish_request(self, req:PublishRequest) -> bool: # or None if everything checks out

        if req is None:
            raise HTTPException(status_code=400, detail="Compute task not found or in error state. Please go back to original request url!")
        if req.script is None: 
            raise HTTPException(status_code=400, detail='Please supply a script to be published')
        if req.script.get_namespace() is None:
            raise HTTPException(status_code=400, detail='Make sure your script has a valid namespace. Set "org" and "name" fields!')
        if len(req.script.org) < self.SCRIPT_ORG_MIN_CHARS:
            raise HTTPException(status_code=400, detail=f'The "org" field of your script is too short. Minimum is {self.SCRIPT_ORG_MIN_CHARS}')
        if len(req.script.name) < self.SCRIPT_NAME_MIN_CHARS:
            raise HTTPException(status_code=400, detail=f'The "name" field of your script is too short. Minimum is {self.SCRIPT_NAME_MIN_CHARS}')
        if req.script.code is None or len(req.script.code) < self.SCRIPT_CODE_MIN_CHARS:
            raise HTTPException(status_code=400, detail=f'Your script contains no field "code" or too little code. Is this a real model? Minimum is {self.SCRIPT_CODE_MIN_CHARS}')
        
        return True


    #### CLASS UTILS ####
        
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

    def _generate_passphrase(self, chars=20) -> str:
        
        return ''.join(secrets.choice(string.ascii_letters + string.digits) for i in range(chars))