#!usr/bin/python3
import os.path
import sys
from pathlib import Path

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '.'))

import db_routines as dbr
import user_interface as ui
import analyze as an

from sqlalchemy.orm import sessionmaker
import pandas as pd
import tkinter as tk

if __name__ == '__main__':

    # initialize main session for connecting to db for data-loading and analysis
    eng, meta_generic = dbr.sql_alchemy_connect(db_name=input('Database name?\n'))
    Session = sessionmaker(bind=eng)
    analysis_session = Session()

    # initialize root widget for tkinter to allow picking from  system
    tk_root = tk.Tk()

    project_root = Path(__file__).parents[1]

    dfile_d,enum_d,raw_file = ui.find_datafile(project_root,analysis_session)
    if enum_d['_datafile_separator'] == 'tab':
        sep = '\t'
    elif enum_d['_datafile_separator'] == 'comma':
        sep = ','
    else:
        raise Exception(f'separator {enum_d["_datafile_separator"]} not recognized')

    encoding = dfile_d['encoding']
    if encoding == '':
        encoding = 'utf-8'

    s,mu = ui.new_datafile(
        raw_file,sep,analysis_session,project_root,state_short_name=input('State short_name (e.g., \'NC\'?\n'),
        encoding=encoding)
    e = an.Election(analysis_session,s,project_root)

    get_top_results = input('Get top-level results (y/n)?\n')
    if get_top_results == 'y':
        top=e.summarize_results(atomic_ru_type=mu.atomic_reporting_unit_type,skip_total_column= not mu.totals_only)
        print(top)

    get_results_by_vctype = input('Get results by vote type (y/n)?\n')
    if get_results_by_vctype == 'y':
        result=e.summarize_results(atomic_ru_type=mu.atomic_reporting_unit_type,mode='by_vote_type',skip_total_column=~mu.totals_only)
        print(result)

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

            anomalies = an.AnomalyDataFrame(electionrollup)
            print('AnomalyDataFrame calculated.')

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
