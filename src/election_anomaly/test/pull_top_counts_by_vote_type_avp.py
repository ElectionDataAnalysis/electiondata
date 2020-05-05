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

    db_name = ui.pick_database(project_root,db_paramfile)

    # initialize main session for connecting to db
    eng, meta_generic = dbr.sql_alchemy_connect(
        paramfile=db_paramfile,db_name=db_name)
    Session = sessionmaker(bind=eng)
    analysis_session = Session()

    jurisdiction = ui.pick_juris_from_filesystem(project_root,
                                                 path_to_jurisdictions=os.path.join(project_root,'jurisdictions'),
                                                 juris_name=juris_name)
    target_dir = os.path.join(project_root,'jurisdictions/NC_5/rollups_from_cdf_db')
    rollup = avp.create_rollup(
        analysis_session,'North Carolina','county','precinct','2018g',target_dir)

    eng.dispose()
    print('Done')
    exit()
