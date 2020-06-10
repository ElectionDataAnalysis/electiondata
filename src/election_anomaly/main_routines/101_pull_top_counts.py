#!usr/bin/python3
import os.path
from sqlalchemy.orm import sessionmaker
from election_anomaly import db_routines as dbr
from election_anomaly import user_interface as ui
from election_anomaly import analyze_via_pandas as avp

if __name__ == '__main__':
    d = ui.get_runtime_parameters(
        ['project_root','juris_name','db_paramfile','db_name','rollup_directory'])

    jurisdiction = ui.pick_juris_from_filesystem(
        d['project_root'],juriss_dir=os.path.join(d['project_root'],'jurisdictions'),juris_name=d['juris_name'])
    target_dir = os.path.join(jurisdiction.path_to_juris_dir,'rollups_from_cdf_db')

    # initialize main session for connecting to db
    eng = dbr.sql_alchemy_connect(
        paramfile=d['db_paramfile'],db_name=d['db_name'])
    Session = sessionmaker(bind=eng)
    analysis_session = Session()

    rollup = avp.create_rollup(analysis_session,d['rollup_directory'],by_vote_type=False)

    eng.dispose()
    print('Done')
    exit()
