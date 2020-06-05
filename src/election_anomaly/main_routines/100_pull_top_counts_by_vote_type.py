#!usr/bin/python3
import os.path
from sqlalchemy.orm import sessionmaker
import db_routines as dbr
import user_interface as ui
import analyze_via_pandas as avp

if __name__ == '__main__':
    interact = input('Run interactively (y/n)?\n')
    if interact != 'y':
        d = ui.config(section='election_anomaly',msg='Pick a paramfile.')
        project_root = d['project_root']
        juris_name = d['juris_name']
        db_paramfile = d['db_paramfile']
        db_name = d['db_name']
        top_reporting_unit = d['top_reporting_unit']
        sub_reporting_unit_type = d['sub_reporting_unit_type']
        atomic_ru_type = d['atomic_ru_type']
        rollup_directory = d['rollup_directory']

    else:
        project_root = ui.get_project_root()
        juris_name = None
        db_paramfile = ui.pick_paramfile()
        db_name = ui.pick_database(project_root,db_paramfile)
        top_reporting_unit = input('Top Reporting Unit?\n')
        sub_reporting_unit_type = input(
            'sub-reporting unit type (e.g., \'county\')?\n')  # report will give results by this ru_type
        atomic_ru_type = input('atomic reporting unit type?')
        rollup_directory = ui.pick_directory(description='the directory for election result rollup exports')

    jurisdiction = ui.pick_juris_from_filesystem(
        project_root,juriss_dir=os.path.join(project_root,'jurisdictions'),juris_name=juris_name)

    # initialize main session for connecting to db
    eng = dbr.sql_alchemy_connect(
        paramfile=db_paramfile,db_name=db_name)
    Session = sessionmaker(bind=eng)
    analysis_session = Session()

    rollup = avp.create_rollup(analysis_session,rollup_directory)

    eng.dispose()
    print('Done')
    exit()
