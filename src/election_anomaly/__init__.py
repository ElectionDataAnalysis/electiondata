from election_anomaly import db_routines as dbr
from election_anomaly import user_interface as ui
from sqlalchemy.orm import sessionmaker
import os
import pandas as pd
from pprint import pprint
import sys
import ntpath
from election_anomaly import analyze_via_pandas as avp

data_loader_parameter_list = [
    'project_root','juris_name','db_paramfile','db_name','munger_name',
    'results_file', 'results_short_name', 'results_download_date', 'results_source', 'results_note',
    'top_reporting_unit','election']

class DataLoader():
    def __new__(self):
        """ Checks if parameter file exists and is correct. If not, does
        not create DataLoader object. """
        try:
            d, parameter_err = ui.get_runtime_parameters(
                data_loader_parameter_list)
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
            data_loader_parameter_list, optional_keys=['aux_data_dir'])

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
            data_loader_parameter_list)
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

    
    def track_results(self):
        filename = self.d['results_file_short']
        top_reporting_unit_id = dbr.name_to_id(self.session,'ReportingUnit',self.d['top_reporting_unit'])
        election_id = dbr.name_to_id(self.session,'Election',self.d['election'])

        data = pd.DataFrame(
            [[self.d['results_short_name'],filename,
              self.d['results_download_date'], self.d['results_source'],
                self.d['results_note'], top_reporting_unit_id, election_id]],
            columns=['short_name', 'file_name',
                     'download_date', 'source',
                     'note', 'ReportingUnit_Id', 'Election_Id'])
        [df,e] = dbr.dframe_to_sql(data,self.session,'_datafile')
        if e:
            return [0,0],e
        else:
            datafile_id = df[(df['short_name']== self.d['results_short_name']) &
                         (df['file_name']==filename) & (df['ReportingUnit_Id']==top_reporting_unit_id) &
                         (df['Election_Id']==election_id)]['Id'].to_list()[0]
            return [datafile_id, election_id], e

    def load_results(self):
        results_info, e = self.track_results()
        if e:
            err = {'database':e}
        else:
            err = ui.new_datafile(self.session, self.munger, self.d['results_file'],self.d['project_root'],
            self.juris, results_info=results_info,aux_data_dir=self.d['aux_data_dir'])
        return err


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
