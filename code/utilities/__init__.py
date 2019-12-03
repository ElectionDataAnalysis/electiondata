#!/usr/bin/python3
# under construction

import re

def shorten_and_cap_county(normal):
    ''' takes a county name in normal form, strips "County" from the name, and capitalizes'''
    parts=normal.split(';')
    
    parser = re.compile('^(?P<short_name>[^\n\t]+)\ County')
    return(parser.search(parts[1]).group('short_name').upper())

def add_externalidentifier(dict,id_type):
    '''input is a dictionary whose keys are county names in normal form and values are dictionaries, including identifiertype-identifier pairs, and an identifiertype. Output is same dictionary, with the identifiers (short name, all caps) included, labeled by the given id_type.'''
    for k in dict.keys():
        if dict[k]['Type'] == 'county':
            print(k)
            dict[k]['ExternalIdentifiers'][id_type]=shorten_and_cap_county(k)
    return(dict)
        
def insert_reporting_unit(dict,reporting_unit_list,reporting_unit_type,id_type):
    '''Insert the reporting units in reporting_unit_list into dict, with correct type (e.g., precinct) and recording the name of the reporting unit also as an external identifier, unless the reporting unit is already in the dict, in which case do the right thing. '''
    for r in reporting_unit_list:
        if r not in dict.keys():    # if reporting unit not already in dictionary, add it
            dict[r]={'Type':reporting_unit_type,'ExternalIdentifiers':{id_type:r}}
        elif dict[r]['Type'] != reporting_unit_type:
            t = dict[r]['Type']
            dict[r+' ('+  t   +')'] = dict.pop(r) # rename existing key to include type (e.g., precinct)
            dict[r+' ('+  reporting_unit_type   +')'] = {'Type':reporting_unit_type,'ExternalIdentifiers':{id_type:r}}
            
def extract_precincts(nc_results_file_path):
    rep_unit_list=[]
    with open(nc_results_file_path,'r') as f:
        lines=f.readlines()
    for line in lines[1:]:
        fields = line.split('\t')
        real_precinct=fields[14]
        if real_precinct == 'Y'     # if row designated a "real precinct" in th nc file
            county = fields[0]
            precinct = fields[2]
            rep_key = 'North Carolina;'+county.capitalize()+' County;Precinct '+precinct
            rep_unit_list.append(rep_key)
    return(rep_unit_list)
        
def process(nc_results_file_path,dict_file_path,outfile):
    a = extract_precincts(nc_results_file_path)
    with open(dict_file_path,'r') as f:
        d=eval(f.readline())
    insert_reporting_unit(d,a,'precinct','export1')
    with open(outfile,'w') as f:
        f.write(str(d))
