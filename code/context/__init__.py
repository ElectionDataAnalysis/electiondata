#!/usr/bin/python3
# under construction
# utilities for extracting state context info and inserting it into the files in the context folder
import sys
import re
import psycopg2
from psycopg2 import sql
from datetime import datetime

def build_munger_d(s,m):
    '''UNDER CONSTRUCTION given a state s and a munger m, use the state's context dictionaries to build dictionaries restricted to the given munger'''
    munger_d = {}
    munger_inverse_d = {}
    for t in ['election','party','reportingunit;precinct','office']:
        t_parts = t.split(';')
        context_key = t_parts[0]
        if len(t_parts) > 1:
            type = t_parts[1]
        else:
            type = None
        munger_d[t] = {}
        for k in s.context_dictionary[context_key].keys():
            if 'ExternalIdentifiers' in s.context_dictionary[context_key][k].keys() and   m.name in s.context_dictionary[context_key][k]['ExternalIdentifiers'].keys() and (type == None or s.context_dictionary[context_key][k]['Type'] == type):
                    munger_d[t][k] = s.context_dictionary[context_key][k]['ExternalIdentifiers'][m.name]
        munger_inverse_d[t] = {}
        for k,v in munger_d[t].items():
            if v in munger_inverse_d[t].keys():
                return('Error: munger_d[\''+t+'\'] has duplicate keys with value '+ v)
            munger_inverse_d[v] = k
    return(munger_d,munger_inverse_d)



def raw_to_context(df,m,query_d,munger_d,conn,cur):
    ''' UNDER CONSTRUCTION df is a datafile; m is a munger; d is a dictionary whose keys are names of tables in the common data format '''
    ### *** error: returns counties only, not geoprecinct names
    rs = [str(datetime.now())]
    for k in query_d.keys():
        cur.execute(sql.SQL(m.query_from_raw[k]).format(sql.Identifier(df.state.schema_name), sql.Identifier(df.table_name)))
        items_per_df = cur.fetchall()
        missing = []
        for e in items_per_df:
            if e[0] not in munger_d[k].values():
                missing.append(e[0])
        rs.append('Sample data for '+k+': '+str( items_per_df[0:4]))
        rs.append('For \''+m.name +'\', <b> missing '+k+' list is: </b>'+str(missing)+'. Add any missing '+k+' to the '+k+'.txt file and rerun')
        
    
    return('</p><p>'.join(rs))


### *** defunct?
def old_raw_to_context(df,m,conn,cur):
    ''' df is a datafile; m is a munger. Routine checks context info from a precinct-results data_file against the info for the state, using the given munger as an intermediary. '''

###### flag elections, parties that need to be added (by hand, because contextual knowledge is necessary) to the context folder and the relevant dictionaries
    rs = []     #strings to return for display on web page
    for i in [[ 'elections','election', df.state.elections],['parties','party',df.state.parties],['offices','office',df.state.offices]]:
        cur.execute(sql.SQL(m.query_from_raw[i[1]]).format(sql.Identifier(df.state.schema_name), sql.Identifier(df.table_name)))
        items_per_df = cur.fetchall()
        ## build dict of keys with external identifiers called m.name *** where else is this used?
        munger_d = {}
        for k in i[2].keys():
            if 'ExternalIdentifiers' in i[2][k].keys() and   m.name in i[2][k]['ExternalIdentifiers'].keys():
                munger_d[k] = i[2][k]['ExternalIdentifiers'][m.name]
        missing = []
        for e in items_per_df:
            if e[0] not in munger_d.values():
                missing.append(e[0])
        rs.append('Sample data for '+i[0]+': '+str( items_per_df[0:4]))
        rs.append('For \''+m.name +'\', <b>list of missing '+i[0]+' is: </b>'+str(missing)+'. Add any missing '+i[0]+' to the '+i[0]+'.txt file and rerun')

###### flag reporting-units that need to be added (by hand, because contextual knowledge is necessary) to the context folder and the relevant dictionaries
############### counties, geoprecincts, reporting units of type 'other'

    for i in [['other reporting units','other_reporting_unit','other'],['counties','county', 'county'],['geoprecincts','geoprecinct','precinct']]:
        cur.execute(sql.SQL(m.query_from_raw[i[1]]).format(sql.Identifier(df.state.schema_name), sql.Identifier(df.table_name)))
        items_per_df = cur.fetchall()
    ## build dict of keys with external identifiers called m.name *** where else is this used?
        munger_d = {}
        for k in df.state.reporting_units.keys():
            type = df.state.reporting_units[k]['Type'].split(';')[0]
            if m.name in df.state.reporting_units[k]['ExternalIdentifiers'].keys() and type == i[2]: # for reporting units, need to check Type as well.
                munger_d[k] = df.state.reporting_units[k]['ExternalIdentifiers'][m.name]
        missing = []
        for e in items_per_df:
            if ';'.join(e) not in munger_d.values():

                missing.append(';'.join(e))
        rs.append('For \''+m.name +'\', <b>list of missing '+i[0]+' is: </b>'+str(missing)+'. Add any missing '+i[0]+' to the reporting_units.txt file and rerun')
    return('</p><p>'.join(rs))

### supporting routines
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
        
def dict_insert(dict_file_path,input_d):
    '''Insert the objects in the dictionary (input_d) into the dictionary stored in the file (at dict_file_path), updating each ExternalIdentifiers dict and any new info, throwing error if the dictionaries conflict'''
    with open(dict_file_path,'r') as f:
        file_d = eval(f.read())
    for k in input_d.keys():
        if k in file_d.keys():
            for kk in input_d[k].keys():
                if kk == 'ExternalIdentifiers':  # update external identifiers, checking for conflict
                    for kkk in input_d[k]['ExternalIdentifiers'].keys():
                        if kkk in file_d[k]['ExternalIdentifiers'].keys():
                            if input_d[k]['ExternalIdentifiers'][kkk] != file_d[k]['ExternalIdentifiers'][kkk]:
                                print('Error: ExternalIdentifiers conflict on ' + kkk)
                                sys.exit()
                        else:
                             file_d[k]['ExternalIdentifiers'][kkk] = input_d[k]['ExternalIdentifiers'][kkk]
                else:   # for properties of the item other than External Idenifiers
                    if kk in file_d[k].keys():
                        if input_d[k][kk] != file_d[k][kk]:
                            print('Error: conflict on ' + kk)
                            sys.exit()
                    else:
                        file_d[k][kk]=input_d[k][kk]
        else:
            file_d[k] = input_d[k]    # put input_d info into file_d
    with open(dict_file_path,'w') as f:
            f.write(str(file_d))
    return(file_d)


def insert_reporting_unit(dict,reporting_unit_list,id_type):
    '''Insert the reporting units in reporting_unit_list (list of unit, type pairs) into dict, with correct type (e.g., precinct) and recording the name of the reporting unit also as an external identifier, unless the reporting unit is already in the dict, in which case do the right thing. '''
    for r in reporting_unit_list:
        k = r[0]    # Reporting unit
        t = r[1]    # Type
        if k not in dict.keys():    # if reporting unit not already in dictionary, add it
            dict[k]={'Type':t,'ExternalIdentifiers':{id_type:k}}
        elif dict[k]['Type'] != t: # if reporting type is in the dictionary, but has different 'Type'
            t_dict = dict[k]['Type']
            dict[r+' ('+  t_dict   +')'] = dict.pop(r) # rename existing key to include type (e.g., precinct)
            dict[r+' ('+  reporting_unit_type   +')'] = {'Type':t,'ExternalIdentifiers':{id_type:r}}
            
def extract_precincts(s,df):
    ''' s is a state; df is a datafile with precincts (*** currently must be in the format of the nc_pct_results file; need to read info from metafile) '''
    if s != df.state:   # consistency check: file must belong to state
        print('Datafile ' +df+ ' belongs to state '+df.state.name+', not to '+s.name)
    rep_unit_list=[]
    with open(s.path_to_state_dir+'data/'+df.file_name,'r') as f:
        lines=f.readlines()
    for line in lines[1:]:
        fields = line.strip('\n\r').split('\t')
        real_precinct=fields[14]
        if real_precinct == 'Y':     # if row designated a "real precinct" in th nc file
            county = fields[0]
            precinct = fields[2]
            rep_key = s.name+';'+county.capitalize()+' County;Precinct '+precinct
            rep_unit_list.append([rep_key,'precinct'])  # return key and 'Type'
        elif real_precinct == 'N':
            county = fields[0]
            election = fields[1]
            precinct = fields[2]
            rep_key = s.name+';'+county.capitalize()+' County;'+election+';'+precinct
            rep_unit_list.append([rep_key,'other;'+rep_key])
    return(rep_unit_list)
    

    
def insert_offices(s,d):
    ''' s is a state; d is a dictionary giving the number of districts for standard offices within the state, e.g., {'General Assembly;House of Representatives':120,'General Assembly;Senate':50} for North Carolina. Returns dictionary of offices. '''
    state = s.name
    out_d = {}
    for k in d.keys():
        for i in range(d[k]):
            office_name = state + ';' + k +';District ' + str(i+1)
            out_d[office_name] = {}
    dict_insert(s.path_to_state_dir + 'context/offices.txt',out_d)
    return(out_d)
    



# is this still necessary?
def process(nc_pct_results_file_path,dict_file_path,outfile):
    a = extract_precincts(nc_pct_results_file_path)
    with open(dict_file_path,'r') as f:
        d=eval(f.read())
    insert_reporting_unit(d,a,'nc_export1')
    with open(outfile,'w') as f:
        f.write(str(d))


## temporary code to fix nc_export1 reporting unit ExternalIdentifiers

def fix(fp):        # fp is the path to the reporting_unit.txt file
    with open(fp,'r') as f:
        d= eval(f.read())
    for k in d.keys():
        if d[k]['Type'][:6] == 'other;':
            d[k]['ExternalIdentifiers']['nc_export1']  # remove old
            sections = k.split(';')
            county_key = sections[1]
            nc_export1_county = shorten_and_cap_county(k)
            #p = re.compile('^Precinct (?P<precinct>.+)$')
            #m = p.search(sections[2])
            #precinct = m.group('precinct')
            precinct = sections[3]
            d[k]['ExternalIdentifiers']['nc_export1'] = nc_export1_county+';'+precinct
    with open(fp+'.new','w') as f:
        f.write(str(d))




