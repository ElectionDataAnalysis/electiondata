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
import user_interface as ui
import states_and_files as sf
import analyze as an

from sqlalchemy.orm import sessionmaker
import pandas as pd
import numpy as np

try:
    import cPickle as pickle
except:
    import pickle

if __name__ == '__main__':
    # print('WARNING: Sorry, lots of bugs at the moment. Don\'t waste your time! -- Stephanie')
    # exit()

    project_root = os.getcwd().split('election_anomaly')[0]
    # state_short_name = 'NC'
    state_short_name = None
    raw_file = os.path.join(project_root,'local_data/FL/data/11062018Election.txt') # TODO note hard-coded
    raw_file_sep = '\t'
    db_paramfile = os.path.join(project_root,'local_data/database.ini')

    s,mu = ui.new_datafile(raw_file,raw_file_sep,db_paramfile,project_root,state_short_name=state_short_name)

    # initialize main session for connecting to db for analysis
    eng, meta_generic = dbr.sql_alchemy_connect(db_name=s.short_name)
    Session = sessionmaker(bind=eng)
    analysis_session = Session()

    e = an.Election(analysis_session,s)


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
            except ValueError:
                print('ERROR (Input was not an integer?); skipping most-anomalous plots')

            draw_all = input('Plot worst bar chart for all contests? (y/n)?\n')
            if draw_all == 'y':
                rollup = e.pull_rollup_from_db_by_types()
                anomalies.worst_bar_for_each_contest(analysis_session,meta_generic)

    eng.dispose()
    print('Done!')
    exit()
