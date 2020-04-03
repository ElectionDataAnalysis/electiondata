#!usr/bin/python3
import os.path
import db_routines as dbr
import user_interface as ui
import analyze as an
from sqlalchemy.orm import sessionmaker
import pandas as pd

if __name__ == '__main__':
    print('Caveat emptor: lots of unnecessary redundancy here, beware.')
    # TODO fix redundant asks for project root, paramfile
    project_root = ui.get_project_root()

    # initialize main session for connecting to db for data-loading and analysis
    eng, meta_generic = dbr.sql_alchemy_connect(
        db_name=input('Database name?\n'),paramfile=ui.pick_paramfile(project_root))
    Session = sessionmaker(bind=eng)
    analysis_session = Session()

    state = ui.pick_juris_from_filesystem(analysis_session.bind,project_root,
                                          path_to_jurisdictions=os.path.join(project_root,'jurisdictions'))
    e = an.Election(analysis_session,state,project_root)

    # TODO remove hard-coded county, precinct
    electionrollup = an.ContestRollup(e,'county','precinct')

    just_one_contest = input('Get anomaly list for just one contest? (y/n)\n')
    while just_one_contest == 'y':
        for x in electionrollup.contest_name_list:
            print(x)
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
