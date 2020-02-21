#!usr/bin/python3
import re
import sys
import os.path
import clean as cl
import pandas as pd
import db_routines as dbr
class State:
    def create_db_and_schemas(self):
        # create db
        con = dbr.establish_connection()
        cur = con.cursor()
        dbr.create_database(con,cur,self.short_name)
        cur.close()
        con.close()
        # connect to  and create the three schemas
        con = dbr.establish_connection(db_name=self.short_name)
        cur = con.cursor()
        q = "CREATE SCHEMA raw;"
        dbr.query(q,[],[],con,cur)
        q = "CREATE SCHEMA context;"
        dbr.query(q,[],[],con,cur)
        q = "CREATE SCHEMA cdf;"
        dbr.query(q,[],[],con,cur)
        cur.close()
        con.close()
        return

    def __init__(self,short_name,path_to_parent_dir):        # reporting_units,elections,parties,offices):
        """ short_name is the name of the directory containing the state info, including data,
         and is used other places as well.
         path_to_parent_dir is the parent directory of dir_name
        """
        self.short_name = short_name
        if path_to_parent_dir[-1] != '/':     # should end in /
            path_to_parent_dir += '/'
        self.path_to_state_dir= path_to_parent_dir + short_name+'/'
        assert os.path.isdir(self.path_to_state_dir), 'Error: No directory '+ self.path_to_state_dir
        assert os.path.isdir(self.path_to_state_dir+'context/'), 'Error: No directory '+ self.path_to_state_dir+'context/'
        # Check that context directory is missing no files
        context_file_list = ['BallotMeasureSelection.txt','datafile.txt','Election.txt','ExternalIdentifier.txt','metafile.txt','name.txt','Office.txt','remark.txt','ReportingUnit.txt'] # TODO remove schema_name.txt and alter README.txt
        file_missing_list = [f for f in context_file_list if not os.path.isfile(self.path_to_state_dir+'context/'+f)]
        assert file_missing_list == [], 'Error: Missing files in '+ self.path_to_state_dir+'context/'+f +':\n'+ str(file_missing_list)

class Munger:
    def __init__(self,dir_path):
        assert os.path.isdir(dir_path),'Not a directory: ' + dir_path
        assert os.path.isfile(dir_path + 'cdf_tables.txt') and os.path.isfile(
            dir_path + 'count_columns.txt') and os.path.isfile(dir_path + 'raw_columns.txt'),\
            'Directory ' + dir_path + ' must contain files cdf_tables.txt, count_columns.txt and raw_columns.txt'
        if dir_path[-1] != '/': dir_path += '/' # make sure path ends in a slash
        self.path_to_munger_dir=dir_path
        self.name=dir_path.split('/')[-2]    # 'nc_export1'

