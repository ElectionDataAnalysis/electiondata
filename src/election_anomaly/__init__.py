from election_anomaly import db_routines as dbr
from election_anomaly import user_interface as ui
from sqlalchemy.orm import sessionmaker
import os
from pprint import pprint
import sys
import ntpath
from election_anomaly import analyze_via_pandas as avp

class DataLoader():
    def __new__(self):
        """ Checks if parameter file exists and is correct. If not, does
        not create DataLoader object. """
        try:
            d, parameter_err = ui.get_runtime_parameters(
                ['project_root','juris_name','db_paramfile',
                'db_name','munger_name','results_file','top_reporting_unit'])
        except FileNotFoundError as e:
            print("Parameter file not found. Ensure that it is located" \
                " in the current directory. DataLoader object not created.")
            return None

        if parameter_err:
            print("Parameter file missing requirements.")
            print(parameter_err)
            print("DataLoader object not created.")
            return None

        return super().__new__(self)


    def __init__(self):
        # grab parameters
        self.d, self.parameter_err = ui.get_runtime_parameters(
            ['project_root','juris_name','db_paramfile',
            'db_name','munger_name','results_file','top_reporting_unit'])

        # results_file is the entire path, the _short version is just
        # the filename
        self.d['results_file_short'] = get_filename(self.d['results_file'])

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

    
    def check_errors(self):
        juris_exists = None
        if not self.juris:
            juris_exists = {"juris_created": False}
        
        return self.parameter_err, self.juris_err, juris_exists, \
            self.juris_load_err, self.munger_err


    def reload_requirements(self):
        if self.session:
            self.session.close()
        if self.engine:
            self.engine.dispose()

        self.d, self.parameter_err = ui.get_runtime_parameters(
            ['project_root','juris_name','db_paramfile',
            'db_name','munger_name','results_file','top_reporting_unit'])
        self.d['results_file_short'] = get_filename(self.d['results_file'])

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

    
    def track_results(self, shortname, election):
        filename = self.d['results_file_short']
        top_reporting_unit = self.d['top_reporting_unit']
        known_info_d = {
            'file_name': filename, 
            'short_name': shortname, 
            'ReportingUnit_Id': top_reporting_unit, 
            'Election_Id': election
        }
        db_style_record, error = ui.set_record_info_from_user(self.session, '_datafile', known_info_d=known_info_d)

        if error:
            print(error)
            print("metadata record not created in database")

        else:
            dbr.save_one_to_db(self.session, '_datafile', db_style_record, True)


    def load_results(self):
        results_info = dbr.get_datafile_info(self.session, self.d['results_file_short'])
        ui.new_datafile(self.session, self.munger, self.d['results_file'],
            juris=self.juris, project_root=self.d['project_root'], 
            results_info=results_info)


class Analyzer():
    def __new__(self):
        """ Checks if parameter file exists and is correct. If not, does
        not create DataLoader object. """
        try:
            d, parameter_err = ui.get_runtime_parameters(['db_paramfile', 
                'db_name', 'results_file'])
        except FileNotFoundError as e:
            print("Parameter file not found. Ensure that it is located" \
                " in the current directory. Analyzer object not created.")
            return None

        if parameter_err:
            print("Parameter file missing requirements.")
            print(parameter_err)
            print("Analyzer object not created.")
            return None

        return super().__new__(self)


    def __init__(self):
        self.d, self.parameter_err = ui.get_runtime_parameters(['db_paramfile', 
            'db_name', 'results_file'])
        self.d['results_file_short'] = get_filename(self.d['results_file'])

        eng = dbr.sql_alchemy_connect(paramfile=self.d['db_paramfile'],
            db_name=self.d['db_name'])
        Session = sessionmaker(bind=eng)
        self.session = Session()


    def display_options(self, input):
        results = dbr.get_input_options(self.session, input)
        if results:
            return results
        return None


    def top_counts_by_vote_type(self, rollup_unit, sub_unit):
        d, error = ui.get_runtime_parameters(['rollup_directory'])
        if error:
            print("Parameter file missing requirements.")
            print(error)
            print("Data not created.")
            return
        else:
            rollup_unit_id = dbr.name_to_id(self.session, 'ReportingUnit', rollup_unit)
            sub_unit_id = dbr.name_to_id(self.session, 'ReportingUnitType', sub_unit)
            results_info = dbr.get_datafile_info(self.session, self.d['results_file_short'])
            rollup = avp.create_rollup(self.session, d['rollup_directory'], top_ru_id=rollup_unit_id,
                sub_rutype_id=sub_unit_id, sub_rutype_othertext='', datafile_id_list=results_info[0], 
                election_id=results_info[1])
            return


    def top_counts(self, rollup_unit, sub_unit):
        d, error = ui.get_runtime_parameters(['rollup_directory'])
        if error:
            print("Parameter file missing requirements.")
            print(error)
            print("Data not created.")
            return
        else:
            rollup_unit_id = dbr.name_to_id(self.session, 'ReportingUnit', rollup_unit)
            sub_unit_id = dbr.name_to_id(self.session, 'ReportingUnitType', sub_unit)
            results_info = dbr.get_datafile_info(self.session, self.d['results_file_short'])
            rollup = avp.create_rollup(self.session, d['rollup_directory'], top_ru_id=rollup_unit_id,
                sub_rutype_id=sub_unit_id, sub_rutype_othertext='', datafile_id_list=results_info[0], 
                election_id=results_info[1], by_vote_type=False)
            return


def get_filename(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)
