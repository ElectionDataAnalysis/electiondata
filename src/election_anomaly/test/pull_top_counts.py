#!usr/bin/python3
import os.path
from sqlalchemy.orm import sessionmaker
import db_routines as dbr
import user_interface as ui
import analyze as an

if __name__ == '__main__':
    project_root = ui.get_project_root()

    # pick db to use
    db_paramfile = ui.pick_paramfile(project_root)
    state_name = 'NC'
    # state_name = None

    db_name = ui.pick_database(project_root,db_paramfile)

    # initialize main session for connecting to db
    eng, meta_generic = dbr.sql_alchemy_connect(
        paramfile=db_paramfile,db_name=state_name)
    Session = sessionmaker(bind=eng)
    analysis_session = Session()

    state = ui.pick_juris_from_filesystem(analysis_session.bind,project_root,
                                          path_to_jurisdictions=os.path.join(project_root,'jurisdictions'),
                                          jurisdiction_name=state_name)
    e =an.Election(analysis_session,state,project_root)

    e.summarize_results(db_paramfile=db_paramfile)

    eng.dispose()
    print('Done')
