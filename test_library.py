from occilib.CadLibrary import CadLibrary

#lib = CadLibrary()

lib = CadLibrary()
#lib._load_scripts_dir()
#lib._print_library_overview()
#print (lib.get_script_request('tests/box', version=0.5))
#print (lib.get_script_request('tests/box'))


#print(lib.scripts[2].all_possible_model_params())  
#lib.compute_cache()

#### SEARCH ####
#print(lib.search('box'))

### PRECOMPUTE ###
'''
script = lib.get_script_request(org='tests', name='sphere')
print(script.get_num_variants())
for d in script.iterate_possible_model_params_dicts():
    print(d)
'''

lib.compute_script_cache(org='tests', name='sphere')
