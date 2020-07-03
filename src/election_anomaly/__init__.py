from election_anomaly import db_routines as dbr
from election_anomaly import user_interface as ui
from sqlalchemy.orm import sessionmaker
import os
from pprint import pprint
import sys

class DataLoader():
    def __init__(self):
        # grab parameters
        try:
            self.d, self.parameter_err = ui.get_runtime_parameters(
                ['project_root','juris_name','db_paramfile',
                'db_name','munger_name','results_file'])
        except FileNotFoundError as e:
            print("Parameter file not found. Ensure that it is located" \
                " in the current directory. Exiting.")
            sys.exit()

        if self.parameter_err:
            print("Parameter file missing requirements.")
            print(self.parameter_err)
            print("Exiting")
            sys.exit()

        # pick jurisdiction
        self.juris, self.juris_err = ui.pick_juris_from_filesystem(
            self.d['project_root'],juris_name=self.d['juris_name'],check_files=True)

        # create db if it does not already exist
        error = dbr.establish_connection(paramfile=self.d['db_paramfile'],
            db_name=self.d['db_name'])
        if error:
            dbr.create_new_db(self.d['project_root'], self.d['db_paramfile'], 
                self.d['db_name'])

        # connect to db
        self.engine = dbr.sql_alchemy_connect(paramfile=self.d['db_paramfile'],
            db_name=self.d['db_name'])
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        if self.juris:
            self.juris_load_err = self.juris.load_juris_to_db(self.session,
                self.d['project_root'])
        else:
            self.juris_load_err = None

        # pick munger
        self.munger, self.munger_err = ui.pick_munger(
            project_root=self.d['project_root'],
            mungers_dir=os.path.join(self.d['project_root'],'mungers'),
            session=self.session,munger_name=self.d['munger_name'])

    
    def data_load_errors(self):
        juris_exists = None
        if not self.juris:
            juris_exists = {"juris_created": False}
        
        return self.parameter_err, self.juris_err, juris_exists, \
            self.juris_load_err, self.munger_err


    def fix_errors(self):
        if self.session:
            self.session.close()
        if self.engine:
            self.engine.dispose()

        self.d, self.parameter_err = ui.get_runtime_parameters(
            ['project_root','juris_name','db_paramfile',
            'db_name','munger_name','results_file'])

        # pick jurisdiction
        self.juris, self.juris_err = ui.pick_juris_from_filesystem(
            self.d['project_root'],juris_name=self.d['juris_name'],check_files=True)

        # create db if it does not already exist
        error = dbr.establish_connection(paramfile=self.d['db_paramfile'],
            db_name=self.d['db_name'])
        if error:
            dbr.create_new_db(self.d['project_root'], self.d['db_paramfile'], 
                self.d['db_name'])

        # connect to db
        eng = dbr.sql_alchemy_connect(paramfile=self.d['db_paramfile'],
            db_name=self.d['db_name'])
        Session = sessionmaker(bind=eng)
        self.session = Session()

        self.juris_load_err = self.juris.load_juris_to_db(self.session,
            self.d['project_root'])    