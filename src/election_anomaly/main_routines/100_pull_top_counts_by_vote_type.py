#!usr/bin/python3
import os.path
from sqlalchemy.orm import sessionmaker
import db_routines as dbr
import user_interface as ui
import analyze_via_pandas as avp

if __name__ == '__main__':
    d = ui.get_runtime_parameters(
        ['project_root','juris_name','db_paramfile','db_name','rollup_directory'])
    jurisdiction = ui.pick_juris_from_filesystem(
        d['project_root'],juriss_dir=os.path.join(
            d['project_root'],'jurisdictions'),juris_name=d['juris_name'])

    # initialize main session for connecting to db
    eng = dbr.sql_alchemy_connect(
        paramfile=d['db_paramfile'],db_name=d['db_name'])
    Session = sessionmaker(bind=eng)
    analysis_session = Session()

    rollup = avp.create_rollup(analysis_session,d['rollup_directory'])

    eng.dispose()
    print('Done')
    exit()
