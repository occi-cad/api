"""

    CadLibrary.py

    Main class for getting CadScripts from filesystem and serving them up 
    as parametric models through a REST API to the user. 
    Either the result is served from the cache or executed in real time by workers

    Important notes:

    * Unique names for scripts

"""

import os
import __main__
import logging
from glob import iglob
from pathlib import Path
from shutil import rmtree
import re
from datetime import datetime
import json
import base64
import asyncio
import pathlib
import tempfile
import uuid

from typing import List, Dict, Any
from collections.abc import Callable

from fastapi.responses import Response, FileResponse
from semver.version import Version

from .CadScript import ModelRequest, CadScript, CadScriptRequest, CadScriptResult, ModelComputeJob
from .CadLibrarySearch import CadLibrarySearch
from .Param import ParamConfigNumber, ParamConfigText, ParamConfigOptions, ParamConfigBoolean
from .models import ComputeBatchStats

from dotenv import dotenv_values
DOTENV_CONFIG = dotenv_values()

class CadLibrary:

    DEFAULT_PATH = './scriptlibrary' # relative to script
    FILE_STRUCTURE_TEMPLATE = '{org}/{name}/{version}/{script}' # IMPORTANT: linux directory seperator '/' (not '\')
    CADSCRIPT_FILE_GLOB = ['*.py', '*.js']
    CADSCRIPT_CONFIG_GLOB = ['*.json']
    COMPUTE_FILE_EXT = '.compute'

    api_generator = None # set when needed to generate end points 
    request_handler = None # set when precomputing cache
    searcher:CadLibrarySearch = None 
    source = 'disk' # source of the scripts: disk or file (debug)
    rel_path = DEFAULT_PATH # relative path to directory of CadScripts
    path = None # absolute path to directory of CadScripts
    scripts:List[CadScript] = [] # all scripts

    latest_scripts:Dict[str,CadScript] = {} # only the latest scripts by unique namespace ({org}/{name})
    script_versions:Dict[str,List[CadScript]] = {} # by unique namespace ({org}/{name})
    
    dirs_by_script_name:Dict[str,str] = {}

    _compute_batch_stats:Dict[str,ComputeBatchStats] = {} # by uuid - IMPORTANT: this data needs to be centralized (in Redis for example) when we want multiple API instances
    _background_async_tasks = set() # Needed to keep references to tasks that might be otherwise deleted by carbage collection

    def __init__(self, rel_path:str=DEFAULT_PATH):
        """
            Populate a library with CadScripts either from a directory (default) or a json file (for debugging)
            Paths are relative to root of the api directory
        """
        self._setup_logger()
        
        self.path = Path(rel_path).resolve()
        pathlib.Path(self.path).mkdir(parents=True, exist_ok=True) # make path if not present

        if '.json' in rel_path:
            self._load_scripts_json(rel_path)

        else:
            if self._check_path(rel_path) is None:
                self.logger.error(f'CadLibrary::__init__(): Given path "{rel_path}" is not a valid directory')
            else:
                self._load_scripts_dir(self.path)        
        
        self.order_scripts()
        self._clear_computing_files() # clear all compute files in cache to avoid old stuff blocking new tasks
        self.searcher = CadLibrarySearch(library=self) # initiate search index

        self._print_library_overview()


    def set_api_generator(self, api_generator:Any): # use any here to avoid circular import
        '''
            Sometimes the library needs access to api_generator
        '''
        self.api_generator = api_generator

    def reload(self) -> bool:
        '''
            Reload library from disk. NOTE: it's slow, we probably don't want to use this too ofter!
        '''
        if not self.path:
            self.logger.error('Cannot reload: no library path (self.path) set!')
            return False
        
        self._load_scripts_dir(self.path)
        self.order_scripts()

    def order_scripts(self):
        '''
            We have all scripts in self.scripts. Order them for easy access.
        '''

        self.latest_scripts = {}
        self.script_versions = {}
        
        for script in self.scripts:
            scripts_by_namespace = list(filter(lambda s: s.name == script.name, self.scripts))
            if len(scripts_by_namespace) == 1:
                self.latest_scripts[script.namespace] = script
                self.script_versions[script.namespace] = [script.version]
            else:
                if not self.latest_scripts.get(script.name):
                    scripts_by_namespace_sorted = sorted(scripts_by_namespace, key=lambda s:  Version.parse(s.version, optional_minor_and_patch=True)) 
                    self.latest_scripts[script.namespace] = scripts_by_namespace_sorted[-1] # pick last one ordered by semver
                    self.script_versions[script.namespace] = [s.version for s in scripts_by_namespace_sorted]


    def get_script_request(self, org:str, name:str, version:str=None) -> CadScriptRequest:
        '''
            Get script with given name 
        '''
        
        # check if user might use a float
        if version is not None and type(version) is not str:
            version = str(version)

        script = None
        if version is None:
            script = self.latest_scripts.get(f'{org}/{name}') # namespace
        else:
            l = list(filter(lambda s: (s.org == org and s.name == name and s.version == version), self.scripts))
            if l:
                script = l[0]

        if not script:
            self.logger.error(f'CadLibrary:get_script_request(org, name, version): Could not find script with org "{org}", name "{name}" and version "{version}" [optional] in library!')
            return None
        
        script_request = CadScriptRequest(**dict(script)) # upgrade CadScript instance to CadScriptRequest for direct use by ModelRequestHandler
        script_request.request.created_at = datetime.now() # we need to refresh created_at (the original request ismade when the script is loaded)

        return script_request

    def get_script_versions(self, script:CadScript|CadScriptRequest) -> List[str]:

        return self.script_versions.get(script.namespace)


    def _load_scripts_json(self, rel_path:str) -> List[CadScriptRequest]:
        # rel_path is related to the root of this project (occilib/..)
        json_file_path = os.path.realpath(
            os.path.join(
                os.path.dirname(
                    os.path.dirname(__file__)), rel_path))

        if not os.path.isfile(json_file_path):
            self.logger.error(f'CadLibrary::_load_scripts_json(): No valid JSON file given ("{json_file_path}")!')
            return

        # just parse the json
        try:
            script_config_dicts = json.load(open(json_file_path))
        except Exception as e:
            self.logger.error(f'CadLibrary::_load_scripts_json(): Failed to parse JSON config file "{json_file_path}" for CadScripts: "{e}"')
            return

        for script_config in script_config_dicts:
            self._set_params_keys_to_names(script_config) 
            base_script = CadScript(**script_config)
            # base_script = self._upgrade_params(base_script, script_config) # TODO: remove
            self.scripts.append(base_script)

        self.source = 'file' # set flag so we now the scripts came from a file

        return self.scripts

        
    def _load_scripts_dir(self, path:str = None) -> List[CadScript]:

        library_path = Path(path or self.path)
        self.scripts = [] # reset to be sure

        for script_glob in self._template_to_script_globs(self.FILE_STRUCTURE_TEMPLATE):
            for script_path in library_path.glob(script_glob):
                script_path_from_lib = str(script_path.relative_to(self.path))
                library_script = self._script_path_to_script(script_path_from_lib)
                if library_script:
                    self.scripts.append(library_script)
        
        self.source = 'disk'
        return self.scripts
    


    def _template_to_script_globs(self, template) -> List[str]:
        '''
            Convert script location template to glob
            for example: '{namespace}/{name}/{version}/{script}' ==> [ '**/**/**/*.py', '**/**/**/*.js' ] 
            see: self.FILE_STRUCTURE_TEMPLATE and self.CADSCRIPT_FILE_GLOB
        '''

        base_glob = re.sub('\{[^\}]+\}\/', '**/', template)
        script_globs = []
        for script_glob in self.CADSCRIPT_FILE_GLOB:
            script_globs.append(base_glob.format(script=script_glob))
        
        return script_globs


    def _template_to_regex(self,template:str) -> str:
        '''
            Convert a template string to a regex with named groups
             for example: '{namespace}/{name}/{version}/{script}' 
             ==> '(?P<namespace>[^/]+)/(?P<name>[^/]+)/(?P<version>[^/])/(?P<script>[^$]+)
        '''
        term_regexs = []
        
        for term in template.split('/'): # only linux
            term_name = term.replace('{', '').replace('}', '')
            term_regex = f'(?P<{term_name}>[^\{os.sep}]+)' if term_name != 'script' else '(?P<script>[^$]+)'
            term_regexs.append(term_regex)
            
        return f'\{os.sep}'.join(term_regexs)
        


    def _script_path_to_script(self,script_path:str) -> CadScript: 

        """ Create Script instance based on script_path: {org/author}/{name}/{version}/{filename}
            NOTE: script_path (including filename) is relative to the library path
            TODO: rewrite this from template pattern and grouped regex for easy config!
        """

        parse_script_path_regex = self._template_to_regex(self.FILE_STRUCTURE_TEMPLATE)
        match = re.match(parse_script_path_regex, script_path)

        if not match:
            self.logger.error(f'CadLibrary::_script_path_to_script(): Cannot parse script_path {script_path}. Please make sure you use the file structure "{self.FILE_STRUCTURE_TEMPLATE}" in library root "{self.path}"!')
            return None
        else:
            script_path_values = match.groupdict()

            # IMPORTANT: allow minor versions:
            try:
                parsed_script_version = Version.parse(script_path_values['version'], optional_minor_and_patch=True) 
            except Exception as e:
                self.logger.error(f'CadLibrary::_script_path_to_script(): Script at path "{script_path}" has invalid semversion. Skipped! Please check!')
                return None
            
            if parsed_script_version:

                base_script = self._parse_config(script_path)
                
                base_script.org = script_path_values['org'].lower() # org is always lowercase
                base_script.name = script_path_values['name'].lower() # name is always lowercase
                base_script.namespace = f'{base_script.org}/{base_script.name}'
                base_script.version = script_path_values['version']
                base_script.id = f"{base_script.org}/{base_script.name}/{base_script.version}"
                # In Docker we might directly have environment variables, no .env
                url_root = os.environ.get('API_ROOT_URL') or DOTENV_CONFIG.get('API_ROOT_URL') or None
                base_script.url = f"{url_root}/{base_script.namespace}:{base_script.version}" if url_root else None

                self._set_script_dir(base_script.name, script_path)

                # try getting extra info from path
                base_script.code = self._get_code_from_script_path(script_path)
                base_script.cad_engine = self._get_code_cad_language(script_path)
                base_script.created_at = self._get_script_created_at(script_path)
                base_script.updated_at = self._get_script_updated_at(script_path)

        return base_script


    def _get_script_created_at(self,script_path:str) -> datetime: # NOTE: path from library
        script_abs_path = os.path.realpath(os.path.join(self.path, script_path))
        return datetime.fromtimestamp(os.path.getctime(script_abs_path))

    def _get_script_updated_at(self,script_path:str) -> datetime: # NOTE: path from library
        script_abs_path = os.path.realpath(os.path.join(self.path, script_path))
        return datetime.fromtimestamp(os.path.getmtime(script_abs_path))

    def _parse_config(self,script_path:str) -> CadScript:
        """
            Basic on script_path (relative to library dir) scan directory for a config file (*.json or *.yaml)
            If config found populate an CadScript instance, otherwise return
        """
        script_dir_abs_path = os.path.dirname(os.path.realpath(os.path.join(self.path, script_path)))
        script_dir_name = os.path.split(script_dir_abs_path)[-1]
        script_dir_org = os.path.split(script_dir_abs_path)[-2]
        script_path, script_ext = os.path.splitext(script_path)
        script_name_with_ext = os.path.split(script_path)[-1]
        script_name = script_name_with_ext.split('.')[0]
        
        script_config_file = None
        for config_ext in self.CADSCRIPT_CONFIG_GLOB:
            script_config_file_paths = list(iglob(config_ext, root_dir=script_dir_abs_path))
            if len(script_config_file_paths) > 0:
                script_config_file = script_config_file_paths[0]
                if len(script_config_file_paths) > 1:
                    script_config_file_paths_flat = script_config_file_paths.join(',')
                    self.logger.warn(f'CadLibrary::_parse_config(): There are multiple config files found ({script_config_file_paths_flat}) for script "{script_path}". Took the first!')
                break
        
        if not script_config_file:
            self.logger.warn(f'CadLibrary::_script_path_to_script(): No config found for component "{script_name}. Check its directory: "{script_path}"! Script is consided static now')
            return CadScript(name=script_name) # return default script (only name is required)
        else:
            script_config_file_path = os.path.realpath(os.path.join(script_dir_abs_path, script_config_file))
            script_config_file_name, script_config_file_ext = os.path.splitext(script_config_file)

            script_config = None
            base_script = None

            if script_config_file_ext == '.json':
                try:
                    script_config = json.load(open(script_config_file_path))
                except Exception as e:
                    self.logger.error(f'CadLibrary::_parse_config(): Failed to parse JSON config file "{script_config_file_path}" for CadScript "{script_name}": {e}')

                if script_config:
                    self._set_params_keys_to_names(script_config)
                    # naming priorities: config_file.name, script parent directory name ("script_dir_name") , script file name ("script_name")
                    chosen_script_org = script_config.get('org') or script_dir_org
                    chosen_script_name = script_config.get('name') or script_dir_name or script_name
                    base_script = CadScript(**{ 'name': chosen_script_name, 'org' : chosen_script_org } | script_config) # CadScript needs a name and org
                    #base_script = self._upgrade_params(base_script, script_config) # TODO: remove 
                
            elif script_config_file_ext == '.yaml' or script_config_file_ext == '.yml':
                self.logger.warn('CadLibrary::_parse_config(): YML config files not implemented yet!')
                pass
            
            if base_script:
                return base_script
            else:
                self.logger.warn(f'CadLibrary::_parse_config(): Invalid config for component "{script_name}". Check config file: {script_config_file_path}!')
                return CadScript(name=script_name) # return default script (only name is required)

    def _get_code_cad_language(self, script_path_rel) -> str:
        # TODO: this is not very robust when other script cad engines join
        EXT_TO_CODE_CAD_LANGUAGE = { 
            'py' : 'cadquery',
            'js' : 'archiyou', 
        }
        return EXT_TO_CODE_CAD_LANGUAGE.get(script_path_rel.split('.')[-1])

    def _get_code_from_script_path(self, script_path_rel:str) -> str:
        """
            Get code inside the script
        """
        script_path_abs = os.path.realpath(os.path.join(self.path, script_path_rel))

        if not os.path.isfile(script_path_abs):
            self.logger.error('CadLibrary::_get_code_from_script_path: given path to script is not a file!')
            return None

        with open(script_path_abs, 'r') as f:
            return f.read()

             
    def _set_script_dir(self, script_name:str, script_path):
        """
            Set script name key in dirs_by_script_name 
            for getting to script directories for caching 
        """
        self.dirs_by_script_name[script_name] = os.path.realpath(os.path.join(self.path, os.path.dirname(script_path)))

            
    def _set_params_keys_to_names(self, script_config:dict) -> dict:
        # if script_config['params'] is a dict, set its keys as names to the Params
        if type(script_config['params']) is dict:
            for k,v in script_config['params'].items():
                v['name'] = k

        
    def _upgrade_params(self, base_script:CadScript, script_config:dict) -> CadScript: 
        """
            Pydantic automatically parses the Param as ParamConfigBase
            Upgrade them according to the type field so we get ParamConfigNumber, ParamConfigText etc

        """
        TYPE_TO_PARAM_CLASS = {
            'number' : ParamConfigNumber,
            'text' : ParamConfigText,
            'boolean' : ParamConfigBoolean,
            'options' : ParamConfigOptions,
        }

        new_params:Dict[str,ParamConfigNumber|ParamConfigText|ParamConfigBoolean|ParamConfigOptions] = {}
        for name, param in base_script.params.items():
            ParamClass = TYPE_TO_PARAM_CLASS.get(param.type) # name of type is already validated by Pydantic and models.ParamType enum
            orig_param_data = list(filter( lambda param_conf: param_conf['name'] == param.name, script_config['params'].values()))[0]
            new_params[name] = ParamClass(**orig_param_data)

        base_script.params = new_params

        return base_script

    #### BASIC CACHE OPERATIONS ####

    def is_cached(self, script:CadScriptRequest) -> bool: 
        '''
            Check if a result of a CadScriptRequest is already on disk
            and has the requested special results in the result.json
            We use map per engine that maps settings in CadScriptRequest.request.settings
        '''
        CAD_ENGINE_REQUEST_SETTINGS_TO_RESULTS = {
            'cadquery' : {},
            'archiyou' : {
                # values in request.settings.docs need to be in CadScriptResult.results.files with same value as given in check_value
                # pre_check can change settings value
                'docs' : { 
                            'results': 'docs', 
                            # pre_check makes sure we always have a checked list of docs
                            'pre_check' : lambda requested_script, settings_entry, settings: 
                                            (requested_script.cad_engine_config.get(settings_entry)) or [] if settings is True 
                                            else 
                                                # this checks if given doc actually exists
                                                list(filter(lambda doc_entry: doc_entry in (requested_script.cad_engine_config or {}).get(settings_entry), settings)) 
                                                if type(settings) is list
                                                else [],
                            'check_value' : lambda settings,results: all( item in results_at.keys() for item in [v for v in requested_settings]) if (type(settings) is list and len(settings)) else True }  
            }
        }

        cad_script_result = self.get_cached_script(script)
        if cad_script_result is None:
            return False
        
        # check if requested results are present
        if not script.cad_engine:
            self.logger.error('''CadLibrary::is_cached(): No cad_engine given, so can't check for specially requested results in CadScriptResult!''')
            return True
        
        if script.request.settings is None:
            # No settings given, so we take it the cached result is good enough
            return True
        
        script_request_checks = CAD_ENGINE_REQUEST_SETTINGS_TO_RESULTS.get(script.cad_engine)

        for setting_entry, check in script_request_checks.items():
            requested_settings = script.request.settings.get(setting_entry)
            if check['pre_check']:
                requested_settings = check['pre_check'](script, setting_entry, requested_settings)
                self.logger.info(f'''CadLibrary::is_cached(): Precheck settings for "{setting_entry}" update to: {requested_settings}''')
            if requested_settings is None: 
                self.logger.info(f'''CadLibrary::is_cached(): No specific settings given in "CadScriptRequest.request.settings.{setting_entry}". Returned True''')
                return True # no special settings given
            
            results_at = getattr(cad_script_result.results, (check['results']))
            # run value check that compares settings with results
            try:
                if check['check_value'](requested_settings, results_at) is False:
                    self.logger.info(f'''CadLibrary::is_cached(): Requested settings in "{setting_entry}" don't match with results in "{check['results']}". Returned False''')
                    return False
            except Exception as e:
                self.logger.error(f'CadLibary::is_cached: Error in check function of "{setting_entry}". Please check settings. Returned False. ERROR: {e}')
                return False
                        
        return True

        
    def _get_cached_script_file_path(self, script:CadScriptRequest) -> str:
        """
            Get the result.json in the cache
        """

        cached_script_model_dir = self._get_script_version_cached_model_dir(script)
        cached_script_model_file = f'{cached_script_model_dir}/result.json'
        return cached_script_model_file if Path(cached_script_model_file).is_file() else None


    def get_cached_script(self, script:CadScriptRequest) -> CadScriptResult:
        """
            See if we have a cached version of this CadScriptRequest
        """
        
        cached_script_path = self._get_cached_script_file_path(script) # result.json

        if cached_script_path is None:
            self.logger.error(f'CadLibrary::get_cached_script: Can not get cached script with name "{script.name}"!')
            return None
        else:
            with open(cached_script_path) as f:
                cached_script_dict = json.loads(f.read())
                cached_script = CadScriptResult(**cached_script_dict) # automatically generated url
                # take over the request data 
                cached_script.request = script.request
                self._apply_single_model_format(cached_script)
                return cached_script

    def get_cached_model(self, script:CadScriptRequest) -> Any:

        EXT_OUTPUT_TYPE = {
            'step' : 'text',
            'gltf' : 'binary',
            'stl' : 'binary'
        }

        cached_script_dir = self._get_script_version_cached_model_dir(script)
        cached_model_path = f'{cached_script_dir}/result.{script.request.format}'

        if os.path.exists(cached_model_path) is False:
            self.logger.error(f'CadLibrary::get_cached_model: Cannot get requested model from cache for script "{script.name}"')
            return None
        else:
            output_model_filename = f'{script.name}-{script.hash()}.{script.request.format}'
            return FileResponse(cached_model_path, filename=output_model_filename) # see: https://fastapi.tiangolo.com/advanced/custom-response/#fileresponse - TODO: media_type?


    def _apply_single_model_format(self, script:CadScriptResult) -> CadScriptResult:

        format = script.request.format

        if format is None:
            return script

        needed_model_format = script.results.models.get(format)

        script.results.models = {}

        if needed_model_format:
            script.results.models[format] = needed_model_format
        else:
            self.logger.error(f'CadLibrary::_apply_single_model_format: No model in format {format} found!')

        return script


    def set_script_model_is_computing(self, script:CadScript, task_id:str):
        """
            Set computing status in library cache for this script and parameter hash
            This is needed to avoid computing things twice when requests are shortly after each other
            The task_id is placed as filename in the cache directory
            use 'check_script_model_computing_job' to check
        """

        if not isinstance(script.request, ModelRequest):
            self.logger.error('CadLibrary::set_script_model_is_computing(): Please supply a script instance with a .request!')
            return False

        script_request_dir_path = self._get_script_version_cached_model_dir(script)
        Path(script_request_dir_path).mkdir(parents=True, exist_ok=True) # check and make needed dirs if not exist
        # to avoid all kinds of problems clear the directory before writing the task file
        self._clear_dir(script_request_dir_path)

        with open(f'{script_request_dir_path}/{task_id}{self.COMPUTE_FILE_EXT}', 'w') as fp: # {library_path}/{component}/{component}-cache/{param hash}/{task_id}
            fp.write(script.json()) # write requested script in file for convenience. It is also used to track calculation time

        return True

    def check_script_model_computing_job(self, script:CadScriptRequest, script_instance_hash:str=None) -> ModelComputeJob:
        """
            Check if a specific script model request is computing
            Return ModelComputeJob with among others task_id or None
        """

        # When getting a job we want to supply a hash coming from the job url, otherwise use hash from script itself
        if script_instance_hash is None:
            script_instance_hash = script.hash()

        script_request_dir = f'{self._get_script_version_cache_dir(script)}/{script_instance_hash}'

        if os.path.exists(script_request_dir):
            files = os.listdir(script_request_dir)
            
            if len(files) > 0:
                first_file = files[0]
                path, ext = os.path.splitext(first_file)
                if ext == self.COMPUTE_FILE_EXT: # .compute extension for robustness
                    self.logger.info(f'ModelRequestHandler::check_script_model_computing_job: Found computing file = "{files[0]}"')
                    task_id = files[0].replace(self.COMPUTE_FILE_EXT, '') # name of file is the task_id

                    job = ModelComputeJob(celery_task_id=task_id)

                    try:
                        with open(f'{script_request_dir}/{first_file}', 'r') as f:
                            requested_script_dict = json.loads(f.read())
                            requested_script = CadScriptRequest(**requested_script_dict)
                            job.script = requested_script
                            job.elapsed_time = round((datetime.now() - requested_script.request.created_at).total_seconds() * 1000)
                    except Exception as e:
                        # avoid all kinds of errors for nothing essential
                        self.logger.error(f'CadLibrary::check_script_model_computing_job: ERROR: "{e}"')

                    return job
                else:
                    return None # probably cached files
            
        return None
        
    def remove_script_model_is_computing_job(self, script:CadScriptResult|CadScriptRequest) -> bool:

        script_request_dir = f'{self._get_script_version_cache_dir(script)}{os.sep}{script.hash()}'
        
        if os.path.exists(script_request_dir):
            self.remove_compute_files(dir=script_request_dir)
    
    def _get_script_version_dir(self, script:CadScript) -> str:
        # {library_path}/{org}/{scriptname}/{version}/{scriptname}-cache
        return os.path.join(os.path.realpath(self.path), script.org, script.name, script.version)
    
    def _get_script_version_cache_dir(self, script:CadScript) -> str:
        #  {library_path}/{org}/{scriptname}/{version}/{scriptname}-cache
        script_dir_path = self._get_script_version_dir(script)
        return f'{script_dir_path}{os.sep}{script.name}-cache'

    def _get_script_version_cached_model_dir(self, script:CadScriptRequest) -> str:
            # {library_path}/{org}/{scriptname}/{version}/{scriptname}-cache/{hash}
            hash = script.hash()
            if not hash:
                self.logger.error('_get_script_version_cached_model_dir: CadScript has no request and no hash!')
                return None
            return f'{self._get_script_version_cache_dir(script)}{os.sep}{hash}'
    
    def _clear_dir(self, dir_path) -> bool:
        for path in Path(dir_path).glob("**/*"):
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                rmtree(path)

    def _clear_computing_files(self) -> bool:

        for path in Path(self.path).glob("**/*"):
            if path.is_file() and path.suffix == self.COMPUTE_FILE_EXT:
                path.unlink()

        self.logger.info('CadLibrary::_clear_computing_files: Cleared old compute files!')

        return True

    def checkin_script_result_in_cache(self, script_result:CadScriptResult) -> CadScriptResult: 

        script_result_json = script_result.json()

        # cache if cachable
        result_cache_dir = f'{self._get_script_version_cached_model_dir(script_result)}'

        if script_result.is_cachable():
            # place total JSON response in cache
            Path(result_cache_dir).mkdir(parents=True, exist_ok=True)

            # save results to file: models
            if len(script_result.results.models.keys()) > 0: # only when some models as results
                with open(f'{result_cache_dir}/result.json', 'w') as f:
                    f.write(script_result_json)
                if script_result.results.models.get('step'):
                    with open(f'{result_cache_dir}/result.step', 'w') as f:
                        f.write(script_result.results.models['step'])
                if script_result.results.models.get('stl'):
                    with open(f'{result_cache_dir}/result.stl', 'wb') as f:
                        stl_binary = base64.b64decode(script_result.results.models['stl']) # decode base64
                        f.write(stl_binary)
                if script_result.results.models.get('gltf'):
                    with open(f'{result_cache_dir}/result.gltf', 'wb') as f:
                        gltf_binary = base64.b64decode(script_result.results.models['gltf']) # decode base64
                        f.write(gltf_binary)

                self.logger.info(f'CadLibrary::checkin_script_result_in_cache(): Model variant cached in directory: {result_cache_dir}')
            else:
                self.logger.error('CadLibrary::checkin_script_result_in_cache: Could not get valid result models. Skipped setting in cache!')

            # worker can also return files in result.files = { {{file_name.ext}} : {{base64}} }. Save those to disk
            if type(script_result.results.files) is dict:
                for filename,data in script_result.results.files.items():
                    with open(f'{result_cache_dir}/{filename}', 'wb') as f:
                        data_binary = base64.b64decode(data) # decode base64
                        f.write(data_binary)
                        self.logger.info(f'CadLibrary::checkin_script_result_in_cache: Saved file "{filename}" to disk in dir "{result_cache_dir}"')

        # clean compute files in script dir
        self.remove_compute_files(dir=result_cache_dir)

        # only allow requested format to be outputted
        # script_result = self._apply_single_model_format(script_result)

        # manage batch stats
        if script_result.request.batch_id is not None:
            self._compute_batch_stats[script_result.request.batch_id].done += 1 # increment
            self._compute_batch_stats[script_result.request.batch_id].duration += script_result.results.duration # increment duration in ms

        return script_result

        

    #### CACHE PRE CALCULATION AND ADMIN ####

    async def _submit_and_handle_compute_script_task(self, script:CadScript, param_values:dict, batch_id:str=None) -> CadScriptResult:
        """
            Submit script to compute workers and wait and handle the result asynchronously 
            The result is set in the cache on disk
        """
    
        script_request = self._make_cache_compute_script_request(script, param_values, batch_id)
        # copy over general publish information from cad_engine_config to request.settings
        script_request.request.settings = script_request.request.settings | (script.cad_engine_config or {})
        script_result = await self.request_handler.compute_script_request(script_request)
        
        self.checkin_script_result_in_cache(script_result)

        batch_id = script_result.request.batch_id

        # some stats for readout
        batch_tasks_total = self._compute_batch_stats[batch_id].tasks if batch_id else None
        batch_tasks_done = self._compute_batch_stats[batch_id].done if batch_id else None
        batch_count_str = f'Batch count: {batch_tasks_done}/{batch_tasks_total}' if batch_id is not None else ''
        self.logger.info(f'CadLibrary::_submit_compute_script_task(): Script "{script_result.name}": model "{script_result.request.hash}" submitted and handled. Took: {script_result.results.duration} ms. {batch_count_str}')

        # detect end of batch
        if batch_id and (batch_tasks_done == batch_tasks_total):
            self.logger.info(f'==== END OF BATCH "{batch_id}" TOOK {self._compute_batch_stats[batch_id].duration/1000}s ====')
            # del self._compute_batch_stats[batch_id] # TODO: make something smarter: delete after timeout
            self.handle_end_of_batch(script_result)
            
        return script_result
    
    def handle_end_of_batch(self, script:CadScript|CadScriptRequest|CadScriptResult):
        '''
            End of compute batch: do something special basic on settings in script.request
        '''
        
        self.logger.info(f'Handle end of batch with batch_on_end_action: "{script.request.batch_on_end_action}"')

        # compute batch is related to publication of a script
        if script.request.batch_on_end_action == 'publish':
            r = self.set_script_version_endpoint(script)
            if r: 
                self.logger.info(f'Added endpoint for script "{script.org}/{script.name}/{script.version}" after publish pre-calculation')
                self.reload() # reload scripts to include new script version


    def set_script_version_endpoint(self, script:CadScript|CadScriptRequest|CadScriptResult) -> bool:

        if self.api_generator is None: 
            self.logger.error(f'Cannot create endpoint for script "{script.name}": Library has no reference to api_generator. Use Library.set_api_generator()')
            return False
        
        return self.api_generator._generate_version_endpoint(script)



    def compute_script_cache(self, org:str, name:str) -> str:
        '''
            Given a script org and name compute its cache synchronously 
            If started compute return batch_id
        '''
        
        from .ModelRequestHandler import ModelRequestHandler # keep this from the normal imports
        self.request_handler = ModelRequestHandler(library=self)

        script = self.get_script_request(org, name)
        if script is None:
            self.logger.error(f'CadLibrary::compute_script_cache: Can not get script with org="{org}" and name="{name}"')
            return None
        
        if not script.is_cachable():
            self.logger.error(f'CadLibrary::compute_script_cache: Script is not cachable!')
            return None

        # basic batch information to keep track of progress
        num_variants = script.get_num_variants()

        if num_variants is None:
            return None
        else:
            compute_batch_id = str(uuid.uuid4())

            # IMPORTANT: currently the tasks of a cache batch are saved centrally in the main API instance
            # This might not scale very well with multiple API instances (like is normal with FastAPI/uvicorn in production)
            # The API instances do maintain a link with the task by waiting for it asynchronously 
            # But requests from API user might come at any API instance
            # TODO: use redis to save stats on different compute jobs?
            self._compute_batch_stats[compute_batch_id] = ComputeBatchStats(tasks=num_variants)

            self.logger.info(f'==== START COMPUTE CACHE FOR SCRIPT "{script.name}" with {num_variants} model variants ====')
            
            async_compute_tasks = []            
            for hash,param_values in script.iterate_possible_model_params_dicts():
                # NOTE: hash is omitted
                async_compute_tasks.append(self._submit_and_handle_compute_script_task(script, param_values,compute_batch_id))
        
            loop = asyncio.get_event_loop()
            #tasks = asyncio.gather(*async_compute_tasks)
            tasks = asyncio.wait(async_compute_tasks) # use wait instead of gather because its easier
            loop.run_until_complete(tasks) # results are already handled
            loop.close()

            return compute_batch_id
    
    async def compute_script_cache_async(self, script:CadScript, only_params:List[str], compute_batch_id:str, on_done:Callable[[str],bool]=None) -> str:
        """
            Precalculate all variants 
            or those with selected params (as given in only_params) of script asynchronously
            returns a batch_id immediately for reference later
        """

        from .ModelRequestHandler import ModelRequestHandler # keep this from the normal imports
        self.request_handler = ModelRequestHandler(library=self)
        
        if not script.is_cachable():
            self.logger.error(f'CadLibrary::compute_script_cache: Script is not cachable!')
            return None
        
        # basic batch information to keep track of progress
        num_variants = script.get_num_variants(only_params)

        if num_variants is None:
            return 'no-precompute possible'
        else:
            self._compute_batch_stats[compute_batch_id] = ComputeBatchStats(tasks=num_variants)

            for hash,param_values in script.iterate_possible_model_params_dicts():
                # TODO: should we await the results here instead of creating the tasks concurrently - it might block the event loop
                # This function also copies general publish settings from script.cad_engine_config to script.request
                task = asyncio.create_task(self._submit_and_handle_compute_script_task(script, param_values,compute_batch_id))
                # IMPORTANT: keep references otherwise GC might remove tasks. See: https://docs.python.org/3/library/asyncio-task.html#asyncio.create_task
                self._background_async_tasks.add(task)
                task.add_done_callback(self._background_async_tasks.discard) # auto remove reference after completion
                await task # this is needed to capture every result sequentially

            # after all tasks are done
            # TODO: do we still need to check end of batch in task done handler?
            if on_done and callable(on_done):
                on_done(compute_batch_id)

            return compute_batch_id

    def compute_cache(self):
        """
            Compute and cache synchronously  all results of cachable scripts in this library
            NOTE: Cache management is centralized: We don't allow workers to write to cache!
        """

        from .ModelRequestHandler import ModelRequestHandler # keep this from the normal imports
        self.request_handler = ModelRequestHandler(library=self)
        
        async_compute_tasks = []

        for script in self.scripts:
            if script.is_cachable():
                param_values_sets = script.all_possible_model_params_dicts() # { hash : { param_name: value, .. }}

                self.logger.info(f'==== START COMPUTE CACHE FOR SCRIPT "{script.name}" with {len(param_values_sets.items())} models ====')
                
                for hash,param_values in param_values_sets.items():
                    # NOTE: hash is omitted
                    async_compute_tasks.append(self._submit_and_handle_compute_script_task(script, param_values))
    
        loop = asyncio.get_event_loop()
        tasks = asyncio.gather(*async_compute_tasks)
        loop.run_until_complete(tasks) # results are already handled
        loop.close()
        


    def _make_cache_compute_script_request(self, script:CadScript, param_dict:dict, batch_id:str=None) -> CadScriptRequest:

        script_request = CadScriptRequest(**script.dict())
        script_request.request.params = param_dict # { name_param: value_param, ... }
        script_request.request.hash = script_request.hash()
        script_request.request.batch_id = batch_id

        return script_request


    

    #### SEARCH ####

    def search(self, q:str):

        return self.searcher.search(q)
    
    #### ADMIN ####

    def add_script(self, script:CadScript, overwrite:bool=False) -> bool:

        if overwrite is False and self.script_exists(script):
            self.logger.error(f'Script with org "{script.org}" and name "{script.name}" already exists in library! Use flag overwrite True if needed!')
            return False
        
        self.write_script(script)
        self._add_script_internal(script) # add to internal script data

        return True
        
    def write_script(self, script:CadScript) -> bool:
        """
            Write script onto disk storage of Library
            NOTE: will overwrite existing
        """

        SCRIPT_LANGUAGE_TO_SCRIPT_EXT = {
            'cadquery' : 'py',
            'archiyou' : 'js',
        }

        if script.code is None: 
            self.logger.error(f'Cannot write a script without code!')
            return False

        script_dir = self.script_to_library_path(script)
        Path(script_dir).mkdir(parents=True, exist_ok=True) # make directory if not exists
        ext = SCRIPT_LANGUAGE_TO_SCRIPT_EXT.get(script.cad_engine) or 'py' # default is py
        script_filepath = f'{script_dir}{os.sep}{script.name}.{ext}'
        config_filepath = f'{script_dir}{os.sep}{script.name}.json'
        
        try:
            with open(script_filepath, 'w') as f:
                f.write(script.code) # write script
            with open(config_filepath, 'w') as f:
                d = json.loads(script.json())
                config = {}
                for k,v in d.items(): # cleans null keys from config
                    if v is not None and k != 'code':
                        config[k] = v

                json_pretty_str = json.dumps(config, indent=2) # hack around pydantic lack of pretty print
                f.write(json_pretty_str) # write config 
            
            return True
            
        except Exception as e:
            self.logger.error(f'Could not write script: {e}')
            return False
        
    def _add_script_internal(self, script:CadScript):
        '''
            Add incoming script version to existing internal data structures
        '''
        if len(list(filter(lambda s: s.org == script.org and s.name == script.name and s.version == script.version, self.scripts))) == 0: # make sure it does not exist already
            self.scripts.append(script)
        scripts_by_namespace = list(filter(lambda s: s.name == script.name, self.scripts))
        scripts_by_namespace_sorted = sorted(scripts_by_namespace, key=lambda s:  Version.parse(s.version, optional_minor_and_patch=True)) 
        self.latest_scripts[script.namespace] = scripts_by_namespace_sorted[-1] # pick last one ordered by semver
        self.script_versions[script.namespace] = [s.version for s in scripts_by_namespace_sorted]


    def script_exists(self, script:CadScript) -> bool:
        # TODO: more robust?
        return os.path.isdir(self.script_to_library_path(script)) 
        
    def script_to_library_path(self, script:CadScript) -> str:
        script_path = self.FILE_STRUCTURE_TEMPLATE.format(**script.dict() | { 'script' : '' }) # fill in placeholders (script is '' to get directory instead of script filepath)
        return f'{self.path}{os.sep}{script_path}'

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

    
    def _check_path(self, rel_path:str) -> str:
        # rel_path is related to the root of this project (occilib/..)
        path = os.path.realpath(
            os.path.join(
                os.path.dirname(
                    os.path.dirname(__file__)), rel_path))

        self.path = path if os.path.isdir(path) else None
        return self.path

    def remove_compute_files(self, dir):

        if os.path.exists(dir):
            files = os.listdir(dir)
            for f in files:
                if f.endswith(self.COMPUTE_FILE_EXT):
                    os.remove(os.path.join(dir, f))

    def _print_library_overview(self):

        self.logger.info('**** OCCI COMPONENTS LIBRARY LOADED ****')
        self.logger.info(f'Scripts: {len(self.latest_scripts)}')
        for name, script in self.latest_scripts.items():
            self.logger.info(f'- "{script.namespace}" {self.script_versions[script.namespace]}[{script.cad_engine}] - path: "{self.dirs_by_script_name[script.name]}/", lines of code: {self._get_lines_of_code(script.code)}, params: {len(script.params.keys())}, author:"{script.author}", org:"{script.org}"')
        self.logger.info('********')

    def _get_lines_of_code(self,code:str) -> int:
        
        if type(code) is not str:
            return 0
        return len(code.split('\n'))



            
   

