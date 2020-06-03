#!usr/bin/python3
import os.path
from sqlalchemy.orm import sessionmaker
import db_routines as dbr
import user_interface as ui
import analyze as an

if __name__ == '__main__':

    interact = input('Run interactively (y/n)?\n')
    if interact == 'y':
        project_root = ui.get_project_root()
        db_paramfile = ui.pick_paramfile()
        db_name = ui.pick_database(project_root, db_paramfile)
        juris_name = None

    else:
        d = ui.config(section='election_anomaly', msg='Pick a parameter file.')
        project_root = d['project_root']
        juris_name = None
        db_paramfile = d['db_paramfile']
        db_name = d['db_name']



    # initialize main session for connecting to db
    eng = dbr.sql_alchemy_connect(
        paramfile=db_paramfile,db_name=db_name)
    Session = sessionmaker(bind=eng)
    analysis_session = Session()
    jurisdiction = ui.pick_juris_from_filesystem(
        project_root,juriss_dir=os.path.join(project_root, 'jurisdictions'),juris_name=juris_name)
    e = an.Election(analysis_session,jurisdiction,project_root)
    # TODO allow db and filesystem directory to have different names


    # TODO remove hard-coding
    if jurisdiction.short_name == 'FL':
        atomic_ru_type = 'county'
    else:
        atomic_ru_type = 'precinct'

    e.summarize_results(db_paramfile=db_paramfile,atomic_ru_type=atomic_ru_type)

    eng.dispose()
    print('Done')
