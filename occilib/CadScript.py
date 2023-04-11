"""

    CadScript.py 

        Script Class containing script data and related methods
        We use pydantic for strict validation

"""

from datetime import datetime
from typing import List, Any, Dict, Tuple, Iterator
from pydantic import BaseModel, validator
import hashlib
import base64
import json
import itertools

from .models import ScriptCadEngine, ModelContentLicense, ModelResult, ModelFormat, ModelQuality, RequestResultFormat, ModelUnits, EndpointStatus, ComputeBatchEndAction
from .Param import ParamConfigBase, ParamConfigNumber, ParamConfigText, ParamConfigBoolean, ParamConfigOptions


class ModelRequest(BaseModel):
    """
        Request to execute CadScript with given params and output form
    """
    created_at:datetime = datetime.now()
    hash:str = None # name+param+values hash id
    params: Dict[str, Any] = {} # simple key (name of param) and value pair
    format: ModelFormat = 'step' # requested output format of the model
    output: RequestResultFormat = None
    quality: ModelQuality = 'high' # TODO
    batch_id: str = None # some id to group requests 
    batch_on_end_action: ComputeBatchEndAction = 'publish'
    meta: dict = {} # TODO
    

    def get_param_query_string(self) -> str:
        '''
            Generate the GET parameters string 
            example: ?format=step&output=full&width=100
        '''
        query_param_values = []
        for param_name, param_value in self.params.items():
            query_param_values.append = f'{param_name}={param_value}'

        query_param_values_string = '&' + '&'.join(query_param_values) if len(query_param_values) > 0 else ''

        return f'?format={self.format}&output={self.output or "model"}{query_param_values_string}'



class CadScript(BaseModel):
    """ 
    A script containing a CAD component with information on inputs and code cad language
    the different steps of CADScript handling: parsing, compute request, compute and results

    """
    id:str = None # unique id for this script {org}/{name}/{version}
    org:str
    name:str # unique name within namespace (always lowercase)
    title:str = None # public title (can have all kinds of characters)
    namespace:str = None # unique endpoint namespace {org}/{name}
    author:str = None
    license:ModelContentLicense = None
    version:str = '1.0'
    url:str = None # url of the endpoint where the script can be found
    description:str = None 
    created_at:datetime = datetime.now()
    updated_at:datetime = datetime.now()
    prev_version:str = None # TODO
    safe:bool = False # if validated as safe code (not implemented yet)
    published:bool = True # if available to the public
    units:ModelUnits = 'mm'
    params:Dict[str, ParamConfigBase | ParamConfigNumber | ParamConfigText | ParamConfigBoolean | ParamConfigOptions ] = {} # list of param definitions - TODO: combine ParamTypes
    param_presets:Dict[str, dict] = {} # TODO: presets of parameters by name and then a { param_name: value } dict
    public_code: bool = False # if public user of the API can see the source code of the CAD script
    code: str  = None# the code of the CAD script
    cad_engine:ScriptCadEngine = 'cadquery' # cadquery, archiyou or openscad (and many more may follow)
    cad_engine_version:str = None # not used currently
    cad_engine_config:dict = None # plug all kind of specific script cad engine config in here
    secret_edit_token:str = None # TODO
    meta:dict = {} # TODO: Remove? Generate tag for FastAPI on the fly

    #### INPUT VALIDATATORS ####
    @validator('name', 'org')
    def lowercase_name_and_org(cls, value):
        return value.lower()

    @validator('namespace', always=True)
    def set_namespace(cls, value, values):
        # always generate namespace from name and org
        return cls.get_namespace(None,**values) # this is somewhat hacky: cls is not an instance!
    
    @validator('params', pre=True)
    def upgrade_params(cls, value, values):
        """
            This upgrades a param before Pydantic parses it as ParamConfigBase
        """
        TYPE_TO_PARAM_CLASS = {
            'number' : ParamConfigNumber,
            'text' : ParamConfigText,
            'boolean' : ParamConfigBoolean,
            'options' : ParamConfigOptions,
        }
        new_params:Dict[str,ParamConfigNumber|ParamConfigText|ParamConfigBoolean|ParamConfigOptions] = {}
        
        for name, param_dict_or_obj in value.items():
            if type(param_dict_or_obj) is dict:
                param_dict = param_dict_or_obj
                # Sometimes we already have the right structure { name: ParamConfigNumber|ParamConfigText|ParamConfigBoolean|etc }
                ParamClass = TYPE_TO_PARAM_CLASS.get(param_dict['type'])
                new_params[name] = ParamClass(**(param_dict | { 'name' : name })) # also add param name coming from dict key
            else:
                new_params[name] = param_dict_or_obj

        return new_params
    
    #### CLASS METHODS ####
    def get_namespace(self, org:str=None, name:str=None, **kwargs) -> str:
        """
            Generate namespace
            The structure is a bit complex because we also take direct args
        """
        org = org or getattr(self, 'org', None)
        name = name or getattr(self, 'name', None)
        if org is not None and len(org) > 0 and name is not None and len(name) > 0:
            return f'{org}/{name}'
        return None

    def hash(self, params: Dict[str, Any]=None) -> str:
        """
            Hash a given dict of value parameters. 
            If not given we check if self is a CadScriptRequest and has request.params and use that
        """
        
        # if params is not given we try to get is from the script.request
        if params is None:
            if not hasattr(self, 'request') or self.request is None:                
                return None

            params = self.request.params

        # NOTE: params can be None if no parameters
        params_str = ''
        if params and len(params.keys()) > 0:
            for name,param_value in params.items():
                params_str += f'{name}={param_value}&'
        
        hash = self._hash(self.name + params_str)
        
        # set hash on request too (if available)
        if hasattr(self, 'request'):
            self.request.hash = hash

        return hash


    def _hash(self, inp:str) -> str:
        # TODO: research this hash function!
        HASH_LENGTH_TRUNCATE = 11
        return base64.urlsafe_b64encode(hashlib.md5(inp.encode()).digest())[:HASH_LENGTH_TRUNCATE].decode("utf-8")

    def is_cachable(self) -> bool:
        """
            Return if the script is cachable by assessing its parameter configuration
        """
        for name,param in self.params.items():
            if param.iterable is False:
                return False
        return True

    
    def all_possible_model_params_dicts(self) -> Dict[str,dict]: # dict[model_hash, dict]
        """
            Get the parameter sets (in {'param_name':value} format) of all possible parametric models
            Also return the model hash in key
            Resulting return data: { 'hash1' : { param_name: value, {..} }, 'hash2' : {...}}
            !!!! IMPORTANT: Can be slow !!!!
        """

        if self.is_cachable() is False:
            return {}

        all_values_per_parameter = []
        for param in self.params.values():
            all_values_per_parameter.append(param.values())

        all_combinations = list(itertools.product(*all_values_per_parameter))
        """ the combinations are generated from the starting value 
            and then iterated from the last list to the first
            
            Example: 
            - param1: [1,2,3,4,5]
            - param2: [10,11,12]
            combinations:
                [[1,10],[1,11],[1,12],[2,10],[2,11],[2,12] etc] 
        """

        all_model_param_sets = {}

        for combination in all_combinations:
            param_values = {}
            for index,value in enumerate(combination):
                param_name = list(self.params.values())[index].name
                param_values[param_name] = value

            # place with hash 
            param_set:Dict[Dict[str,Any]] = {}
            for k,v in param_values.items():
                param_set[k] = v

            param_set_hash = self.hash(param_set)
            all_model_param_sets[param_set_hash] = param_values

        return all_model_param_sets
                    
    
    def iterate_possible_model_params_dicts(self) -> Iterator[Tuple[str, dict]]: # hash, { param1: { value: x }}

        '''
            Iterator over all combinations of param values
        '''

        all_values_per_parameter = []
        for param in self.params.values():
            all_values_per_parameter.append(param.values())

        for combination in itertools.product(*all_values_per_parameter):
            param_values = {}
            for index,value in enumerate(combination):
                param_name = list(self.params.values())[index].name
                param_values[param_name] = value

            param_set:Dict[Dict[str,Any]] = {}
            for k,v in param_values.items():
                param_set[k] = v

            param_set_hash = self.hash(param_set)
            yield param_set_hash, param_values

    def get_num_variants(self) -> int:

        '''
            Get total number of variants for this script:
            All possible values of every parameter (NVP..NVPn)
            num variants = NVP1 * NVP2 * ... NVPN
        '''

        num_combinations = 1
        for param_obj in self.params.values():
            num_combinations *= len(param_obj.values())

        return num_combinations

        

class CadScriptRequest(CadScript):
    """
        CadScript that is used to make a request
    """
    
    status:EndpointStatus = 'success'
    request:ModelRequest = ModelRequest() # just make an empty ModelRequest instance

    def get_param_values_dict(self) -> dict:
        """
             Convert param values to Dict
             request.params is in { name: { value: 'some value' }} format
        """
        if self.request and type(self.request.params) is dict:
            
            param_values:dict = {}
            for k,v in self.request.params.items():
                param_values[k] = v.value

            return param_values


class ModelComputeJob(BaseModel):
    status:EndpointStatus = 'working'
    celery_task_id:str = None
    celery_task_status:str = None
    script:CadScriptRequest = None
    elapsed_time:int = None # in seconds


class CadScriptResult(CadScriptRequest):
    """
        CadScript that has been through compute and has results
    """
    results:ModelResult = ModelResult()
    




