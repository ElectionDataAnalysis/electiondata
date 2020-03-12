#!usr/bin/python3
import os.path
import sys

import munge_routines

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '.'))

import db_routines as dbr
import munge_routines as mr
from db_routines import Create_CDF_db as CDF
import states_and_files as sf
import analyze as an

from sqlalchemy.orm import sessionmaker
import pandas as pd
import numpy as np

try:
    import cPickle as pickle
except:
    import pickle

def user_picks_munger(dir_path):
    munger_list = [f for f in os.listdir(dir_path) if
                   os.path.isdir(dir_path + f)]
    assert munger_list != [],'No mungers available for in directory {}/{}'.format(s.short_name,election_name)
    default = munger_list[0]
    need_munger = True
    while need_munger:
        print('Available mungers are:')
        for m in munger_list: print(m)
        munger_name = input('Enter short name of munger (default is {})\n'.format(default)) or default
        if munger_name in munger_list:
            need_munger = False
        else:
            print('No such munger; try again.')

    munger_path = '../mungers/' + munger_name + '/'
    print('Creating munger instance from {}'.format(munger_path))
    mu = sf.Munger(munger_path)

    return mu,munger_path

# TODO will need routines to add new munger externalidentifiers to an existing cdf db;
#  will need routines to add new reportingunits to an existing cdf db.
if __name__ == '__main__':
    # initialize state and create database for it (if not already exists)
    default = 'NC'
    abbr = input(
        'Enter short name for your state/district/territory (only alphanumeric and underscore, no spaces, default is ' + default + ')\n'
    ) or default
    print('Creating instance of State for {}'.format(abbr))
    # get absolute path to local_data directory
    current_dir=os.getcwd()
    path_to_src_dir=current_dir.split('/election_anomaly')[0]
    s = sf.State(abbr,'{}/local_data/'.format(path_to_src_dir))
    create_db = input('Make database and schemas for {} (y/n)?\n'.format(abbr))
    if create_db == 'y':
        s.create_db_and_schemas()

    # initialize main session for connecting to db
    eng, meta_generic = dbr.sql_alchemy_connect(db_name=s.short_name)
    Session = sessionmaker(bind=eng)
    session = Session()

    if create_db == 'y':
        # create build tables in cdf schema
        print('Creating common data format tables in schema `cdf` in database {}'.format(s.short_name))

        meta_cdf = CDF.create_common_data_format_tables(session,'cdf',delete_existing=True)
        session.commit()

        # load data from context directory into context schema
        # TODO make it possible to update the context schema
        print('Loading context data from {0}/context directory into `context` schema in database {0}'.format(s.short_name))
        # for file in context folder, create table in context schema.
        context = {}
        for f in os.listdir(s.path_to_state_dir+'/context/'):
            if f[0] == '.': continue
            table_name = f.split('.')[0]

            context[table_name] = pd.read_csv(s.path_to_state_dir+'/context/'+f,sep='\t')
            context[table_name].to_sql(table_name,session.bind,'context',if_exists='fail')

        # %% fill enumeration tables
        print('\tFilling enumeration tables')
        CDF.fill_cdf_enum_tables(session,meta_cdf,'cdf',enumeration_tables)
        print('Loading state context info into CDF schema')
        munge_routines.context_schema_to_cdf(session,s,enumeration_tables)
        session.commit()

    # user picks election
    election_list = [f for f in os.listdir('{}data/'.format(s.path_to_state_dir)) if os.path.isdir('{}data/{}'.format(s.path_to_state_dir,f))]
    assert election_list != [], 'No elections available for {} in directory {}/data'.format(s.short_name,s.path_to_state_dir)
    default = election_list[0]
    need_election = True
    while need_election:
        print('Available elections are:')
        for e in election_list: print(e)
        election_name = input('Enter short name of election (default is {})\n'.format(default)) or default
        if election_name in election_list: need_election = False
        else: print('Election not available; try again.')
    print('Creating Election instance for {}'.format(election_name))
    e = an.Election(session,s,election_name)


    need_to_load_data = input('Load raw data (y/n)?\n')
    if need_to_load_data == 'y':
        # user picks munger
        mu,munger_path = user_picks_munger('{}data/{}/'.format(s.path_to_state_dir,election_name))

        type_map_d = {'string':str,'integer':int}
        type_d = dict(np.array(mu.raw_columns))
        pytype_d = {k: type_map_d.get(v) for k, v in type_d.items()}

        # dfs = pd.read_sql_table('datafile',session.bind,schema='context',index_col='index')
        # note: some fields are empty strings and will be read as nan by read_csv
        dfs=pd.read_csv('{}context/datafile.txt'.format(s.path_to_state_dir),sep='\t').fillna('')
        for datafile in os.listdir(s.path_to_state_dir + 'data/'+election_name+'/'+mu.name+'/'):
            # check datafile is listed in datafiles
            if '{};{}'.format(e.name,datafile) not in dfs['name'].to_list():
                print('WARNING: Datafile {};{} not recognized in the table context.datafile: , will not be processed.'.format(e.name,datafile))
                continue
            df_info = dfs[dfs['name'] == '{};{}'.format(e.name,datafile)].iloc[0]
            encoding=df_info['encoding']
            if not encoding:
                encoding = 'utf-8'  # TODO document default encoding
            if df_info['separator'] == 'tab':
                delimiter = '\t'
            elif df_info['separator'] == 'comma':
                delimiter = ','
            else:
                print('Separator {} not recognized, tab will be assumed'.format(df_info['separator']))
                delimiter = '\t'
            raw_data_dframe = pd.read_csv('{}data/{}/{}/{}'.format(s.path_to_state_dir,election_name,mu.name,datafile),sep=delimiter,dtype=str,encoding=encoding)
            # interpret all as strings (dtype=str above)
            # strip whitespace from each entry
            raw_data_dframe=raw_data_dframe.apply(lambda x:x.str.strip())
            raw_data_dframe.rename(columns=mu.rename_column_dictionary,inplace=True)
            # TODO change column names as necessary mu.rename_column_dictionary

            for c in raw_data_dframe.columns:
                raw_data_dframe[c]=raw_data_dframe[c].fillna('')  # TODO can we do all of the dataframe at once, not col by col?

            print('Loading data into cdf schema from file: {}'.format(datafile))
            mr.raw_dframe_to_cdf(session,raw_data_dframe, mu,e)

    try:    # if mu is not already defined
        mu
    except:
        mu,munger_path = user_picks_munger('{}data/{}/'.format(s.path_to_state_dir,election_name))


    get_top_results = input('Get top-level results (y/n)?\n')
    if get_top_results == 'y':
        top=e.summarize_results(atomic_ru_type=mu.atomic_reporting_unit_type,skip_total_column= not mu.totals_only)
        print (top)

    get_results_by_vctype = input('Get results by vote type (y/n)?\n')
    if get_results_by_vctype == 'y':
        result=e.summarize_results(atomic_ru_type=mu.atomic_reporting_unit_type,mode='by_vote_type',skip_total_column=~mu.totals_only)
        print (result)

    need_to_analyze = input('Analyze (y/n)?\n')
    if need_to_analyze == 'y':
        electionrollup = an.ContestRollup(e,'county',mu.atomic_reporting_unit_type)

        just_one_contest = input('Get anomaly list for just one contest? (y/n)\n')
        while just_one_contest == 'y':
            for x in electionrollup.contest_name_list: print(x)
            contest_name = input('Enter contest name\n')
            try:
                anomaly_list = an.anomaly_list(contest_name,electionrollup.restrict_by_contest_name([contest_name]))
                a_dframe = pd.DataFrame(anomaly_list)
                print(a_dframe)
            except:
                print('Error')
            just_one_contest = input('Get anomaly list for another contest? (y/n)\n')

        all_contests = input('Analyze all contests? (y/n)\n')
        if all_contests == 'y':

            pickle_path = e.pickle_dir+'_anomalies_by_'+electionrollup.roll_up_to_ru_type+'_from_'+electionrollup.atomic_ru_type
            if os.path.isfile(pickle_path):
                print('Anomalies will not be calculated, but will be read from existing file {}:\n\t'.format(pickle_path))
                with open(pickle_path,'rb') as f:
                    anomalies=pickle.load(f)
            else:
                anomalies = an.AnomalyDataFrame(electionrollup)
                with open(pickle_path,'wb') as f:
                    pickle.dump(anomalies,f)
                print('AnomalyDataFrame calculated, stored as pickle at '.format(pickle_path))

            anomalies.worst_bar_for_selected_contests()
            default = 3
            n = input('Draw how many most-anomalous plots (default is {})?\n'.format(default)) or default
            try:
                n = int(n)
                anomalies.draw_most_anomalous(3,'pct')
                anomalies.draw_most_anomalous(3,'raw')

            except:
                print('ERROR (Input was not an integer?); skipping most-anomalous plots')

            draw_all = input('Plot worst bar chart for all contests? (y/n)?\n')
            if draw_all == 'y':
                e.worst_bar_for_each_contest(session,meta_generic)


    eng.dispose()
    print('Done!')
    exit()


