#!/usr/bin/python3
# context/__init__.py
# under construction
# utilities for extracting state context info and inserting it into the files in the context folder
import sys
import re
import os

from db_routines import dframe_to_sql
import munge_routines as mr
import db_routines as dbr
import pandas as pd


def fill_externalIdentifier_table(session,cdf_schema,context_schema,mu):
    """
    mu is a munger
    """
    # get table from munger directory with the tab-separated definitions of external identifiers
    fpath=mu.path_to_munger_dir+'ExternalIdentifier.txt'
    print('Pulling munger\'s ExternalIdentifier table from {}'.format(fpath))
    ei_df = pd.read_csv(fpath,sep = '\t')
    ei_df.loc[:,'ExternalIdentifierType'] = mu.name

    ei_df.to_sql('ExternalIdentifier',session.bind,schema=context_schema,if_exists='replace') # TODO better option than replacement?

    cdf = {}
    filtered_ei = {}

    # pull from the cdf the enumeration of ExternalIdentifier types
    cdf['IdentifierType'] = pd.read_sql_table('IdentifierType',session.bind,cdf_schema,index_col=None)

    # pull from the cdf db any tables whose entries have external identifiers
    for t in ei_df['Table'].unique():
        cdf[t] = pd.read_sql_table(t,session.bind,cdf_schema,index_col=None)
        # drop all but Name and Id columns
        cdf[t] = cdf[t][['Name','Id']]

        # manipulate temporary dataframe ei_filt into form for insertion to cdf.ExternalIdentifier
        ei_filt = ei_df[ei_df['Table']==t]   # filter out rows for the given table
        # join Table on name to get ForeignId
        ei_filt = ei_filt.merge(cdf[t],left_on='Name',right_on='Name') # TODO 'Name' on right won't be found in BalLotMeasureSelection or Candidate table
        ei_filt.rename(columns={'Id':'ForeignId','ExternalIdentifierValue':'Value','ExternalIdentifierType':'IdentifierType'},inplace=True)

        # join IdentifierType columns
        ei_filt = mr.enum_col_to_id_othertext(ei_filt,'IdentifierType',cdf['IdentifierType'])

        filtered_ei[t] = ei_filt

    ei_df = pd.concat([filtered_ei[t] for t in ei_df['Table'].unique()])

    # insert appropriate dataframe columns into ExternalIdentifier table in the CDF db
    print('Inserting into ExternalIdentifier table in schema ' + cdf_schema)
    dframe_to_sql(ei_df,session,cdf_schema,'ExternalIdentifier')

    session.flush()
    return ei_df

def fill_composing_reporting_unit_join(session,schema,pickle_dir='../local_data/pickles/'):
    print('Filling ComposingReportingUnitJoin table, i.e., recording nesting relations of ReportingUnits')
    ru_dframe = pd.read_sql_table('ReportingUnit', session.bind, schema,index_col=None)
    ru_dframe['split'] = ru_dframe['Name'].apply(lambda x: x.split(';'))
    ru_dframe['length'] = ru_dframe['split'].apply(len)
    ru_static=ru_dframe.copy()
    cruj_dframe_list = []
    for i in range(ru_dframe['length'].max()-1):
    # check that all components of all Reporting Units are themselves ReportingUnits
        # get name of ith ancestor
        ru_dframe = ru_static.copy()
        ru_dframe['ancestor_'+str(i)] = ru_static['split'].apply(lambda x: ';'.join(x[:-i-1]))
        # get ru Id of ith ancestor
        drop_list = ru_dframe.columns.to_list()
        ru_dframe = ru_dframe.merge(ru_dframe,left_on='Name',right_on='ancestor_'+str(i),suffixes=['_'+str(i),''])
        drop_list.remove('Id')
        cruj_dframe_list.append(ru_dframe[['Id','Id'+'_'+str(i)]].rename(columns={'Id':'ChildReportingUnit_Id','Id'+'_'+str(i):'ParentReportingUnit_Id'}))

    cruj_dframe = pd.concat(cruj_dframe_list)
    cruj_dframe = dframe_to_sql(cruj_dframe, session, schema, 'ComposingReportingUnitJoin')
    session.flush()
    return cruj_dframe

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
            dict[r+' ('+  t   +')'] = {'Type':t,'ExternalIdentifiers':{id_type:r}}
            
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
            out_d[office_name] = {'ElectionDistrict':office_name}
    out_d['North Carolina;US Congress;Senate']={'ElectionDistrict':'North Carolina'}
    dict_insert(s.path_to_state_dir + 'context/Office.txt',out_d)
    return(out_d)
    
if __name__ == '__main__':
    from sqlalchemy.orm import sessionmaker
    import states_and_files as sf
    import db_routines.Create_CDF_db as CDF

    schema='test'
    eng,meta = dbr.sql_alchemy_connect(schema=schema,paramfile='../../local_data/database.ini')
    Session = sessionmaker(bind=eng)
    session = Session()

    data = {'Foo':[1,2,3,2,1],'Selection_Id':[42,43,44,44,46],'Id':[2,3,4,5,6]}
    test_dframe = pd.DataFrame(data=data)
    new = dframe_to_sql(test_dframe, session, schema, 'SelectionElectionContestVoteCountJoin', index_col='Id')

    s = sf.create_state('NC','../../local_data/NC/')
    enumeration_table_list = CDF.enum_table_list(dirpath='../CDF_schema_def_info/')
    print('Done!')
