#!usr/bin/python3
import os.path
from sqlalchemy.orm import sessionmaker
import db_routines as dbr
import user_interface as ui
import analyze_via_pandas as avp

if __name__ == '__main__':
    project_root = ui.get_project_root()

    # pick db to use
    db_paramfile = ui.pick_paramfile(project_root)
    juris_name = 'NC'
    db_name='NC'

    # initialize main session for connecting to db
    eng, meta_generic = dbr.sql_alchemy_connect(
        paramfile=db_paramfile,db_name=db_name)
    Session = sessionmaker(bind=eng)
    analysis_session = Session()
    a,b,c = avp.contest_type_and_name_by_id(eng)
    eng.dispose()
    print('Done')
