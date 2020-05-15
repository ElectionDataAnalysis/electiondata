#!usr/bin/python3
import os.path
from sqlalchemy.orm import sessionmaker
import db_routines as dbr
import user_interface as ui
import analyze_via_pandas as avp

if __name__ == '__main__':
    project_root = ui.get_project_root()

    # pick db to use
    db_paramfile = ui.pick_paramfile()
    juris_name = None

    jurisdiction = ui.pick_juris_from_filesystem(
        project_root,juriss_dir=os.path.join(project_root,'jurisdictions'),juris_name=juris_name)

    db_name = ui.pick_database(project_root,db_paramfile)

    # initialize main session for connecting to db
    eng, meta_generic = dbr.sql_alchemy_connect(
        paramfile=db_paramfile,db_name=db_name)
    Session = sessionmaker(bind=eng)
    analysis_session = Session()

    target_dir = os.path.join(jurisdiction.path_to_juris_dir,'rollups_from_cdf_db')
    top_reporting_unit_type = 'North Carolina'
    sub_reporting_unit_type = 'county'  # report will give results by this ru_type
    atomic_ru_type = 'precinct'

    rollup = avp.create_rollup(
        analysis_session,top_reporting_unit_type,sub_reporting_unit_type,atomic_ru_type,
        '2018 General',target_dir)

    eng.dispose()
    print('Done')
    exit()
