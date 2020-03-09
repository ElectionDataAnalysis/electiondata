#!usr/bin/python3
import os.path
import db_routines as dbr
import pandas as pd
import re
import munge_routines as mr


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
        q = "CREATE SCHEMA context;"
        dbr.query(q,[],[],con,cur)
        q = "CREATE SCHEMA cdf;"
        dbr.query(q,[],[],con,cur)
        cur.close()
        con.close()
        return

    def add_to_context_dir(self,element,f):
        """Add the data in the dataframe <f> to the file corresponding to <element> in the <state>'s folder"""
        # TODO
        try:
            elts = pd.read_csv('{}{}.txt'.format(self.path_to_state_dir,element))
        except:
            print('Could not read data from file {}context/{}.txt'.format(self.path_to_state_dir,element))
            return
        for col in elts.columns:
            if not col in f.columns:
                print('WARNING: Column {} not found, will be added with no data.'.format(col))
                print('Be sure all necessary data is added before processing the file')
                f.loc[:,col] = ''
        # pull and order necessary columns from <f>
        f_new =  pd.concat([f[[elts.columns]],elts])
        f_new.to_csv('{}{}.txt'.format(self.path_to_state_dir,element),sep='\t',index=False)

        # TODO insert into cdf.<element>, using mr.load_context_dframe_into_cdf

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
        context_file_list = ['Election.txt','Office.txt','remark.txt','ReportingUnit.txt']
        file_missing_list = [f for f in context_file_list if not os.path.isfile(self.path_to_state_dir+'context/'+f)]
        assert file_missing_list == [], 'Error: Missing files in '+ self.path_to_state_dir+'context/:\n'+ str(file_missing_list)
        # TODO format string above '...'.format()
class Munger:

    def check_new_datafile(self,f,state):
        """f is a results datafile; this routine should add what's necessary to the munger to treat the datafile,
        keeping backwards compatibility and exiting gracefully if datafile needs different munger"""
        print('WARNING: All ReportingUnits in this file will be munged as type \'{}\'. '.format(self.atomic_reporting_unit_type))
        check_ru_type=input('\tIf other behavior is desired, create or use another munger.\n\tProceed with munger {} (y/n)?\n'.format(self.name))
        if check_ru_type != 'y':
            print('Datafile will not be processed.')
            return
        cols=self.raw_columns.name
        assert set(f.columns).issubset(cols), \
            'ERROR: Munger cannot handle the datafile. A column in {} is missing from {} (listed in raw_columns.txt).'.format(f.columns,list(cols))
        # note: we don't look for new offices. Must put desired offices into Office.txt in any case
        # TODO where will user be notified of untreated offices?
        for element in ['ReportingUnit','Party','CandidateContest','Election']:
            print('Examining instances of {}'.format(element))
            # get list of columns of <f> needed to determine the raw_identifier for <element>
            p = '\<([^\>]+)\>'
            col_list = re.findall(p,self.cdf_tables.loc[element,'raw_identifier_formula'])

            # create dataframe of unique instances of <element>
            f_elts = f[col_list].drop_duplicates()

            # munge the given element into a new column of f_elts
            mr.add_munged_column(f_elts,self,element,'raw_identifier_value')

            # get any unmatched lines of f_elts
            unmatched = mu.find_unmatched(f_elts,element)

            # if some elements are unmatched:
            while unmatched.shape[0] > 0:
                # add blank column to be filled in
                unmatched.loc[:,'cdf_internal_name'] = ''
                unmatched_path = '{}unmatched_{}.txt'.format(self.path_to_munger_dir,element)

                # write unmatched lines to file and invite user to edit
                unmatched.to_csv(unmatched_path,sep='\t',index=None)
                print('\tACTION REQUIRED: fill the cdf_internal_name column in the file {}.'.format(unmatched_path,element))
                input('\tEnter any key when work is complete.\n')

                # read user-edited file
                new_f_elts = pd.read_csv(unmatched_path,sep='\t')

                # restrict to matched lines
                new_f_elts=new_f_elts[new_f_elts['cdf_internal_name'].notnull()]

                # add cdf_element column
                new_f_elts.loc[:,'cdf_element'] = element

                # add to raw_identifiers
                self.add_to_raw_identifiers(new_f_elts)

                if element != 'CandidateContest':
                    # add to state's context directory
                    # TODO what form should new_f_elts have?
                    f.rename(columns={'cdf_internal_name':'Name'},inplace=True)
                    state.add_to_context_dir(element,new_f_elts)

                # add to the cdf.<element> table
                # TODO create source_df in right format
                mr.load_context_dframe_into_cdf(session,source_df,element)

                if element == 'ReportingUnit':
                   # insert as necessary into ComposingReportingUnitJoin table
                    new_f_elts['Name']=new_f_elts['cdf_internal_name']
                    dbr.cruj_insert(state,new_f_elts)

                # see if anything remains to be matched
                unmatched = mu.find_unmatched(new_f_elts,element)
        # TODO
        return

    def add_to_raw_identifiers(self,f):
        """Adds rows in <f> to the raw_identifiers.txt file and to the attribute <self>.raw_identifiers"""
        for col in mu.raw_identifiers.columns:
            assert col in f.columns, 'Column {} is not found in the dataframe'.format(col)
        # restrict to columns needed, and in the right order
        f = f[mu.raw_identifiers.columns]
        # add rows to <self>.raw_identifiers
        self.raw_identifiers = pd.concat([self.raw_identifiers,f]).drop_duplicates()
        # update the external munger file
        self.raw_identifiers.to_csv('{}raw_identifiers.txt'.format(self.path_to_munger_dir),sep='\t',index=False)

    def find_unmatched(self,f_elts,element):
        """find any instances of <element> in <f_elts> but not interpretable by <self>"""

        # identify instances that are not matched in the munger's raw_identifier table
        # limit to just the given element
        ri = self.raw_identifiers[self.raw_identifiers.cdf_element==element]

        # TODO prevent index column from getting into f_elts at next line
        f_elts = f_elts.merge(ri,on='raw_identifier_value',how='left',suffixes=['','_ri'])
        unmatched = f_elts[f_elts.cdf_internal_name.isnull()]
        unmatched = unmatched.drop(['cdf_internal_name'],axis=1)
        unmatched=unmatched.sort_values('raw_identifier_value')
        # unmatched.rename(columns={'{}_raw'.format(element):'raw_identifier_value'},inplace=True)
        return unmatched

    def __init__(self,dir_path,cdf_schema_def_dir='CDF_schema_def_info/'):
        assert os.path.isdir(dir_path),'Not a directory: {}'.format(dir_path)
        for f in ['cdf_tables.txt','atomic_reporting_unit_type.txt','count_columns.txt']""
            assert os.path.isfile('{}{}'.format(dir_path,f)),'Directory {} does not contain file {}'.format(dir_path,f)
        self.name=dir_path.split('/')[-2]    # 'nc_general'

        if dir_path[-1] != '/':
            dir_path += '/' # make sure path ends in a slash
        self.path_to_munger_dir=dir_path

        # read raw_identifiers file into a table
        self.raw_identifiers=pd.read_csv('{}raw_identifiers.txt'.format(dir_path),sep='\t') # note no natural index column

        # define dictionary to change any column names that match internal CDF names
        col_d = {t:'{}_{}'.format(t,self.name)
                 for t in os.listdir('{}Tables'.format(cdf_schema_def_dir))}
        self.rename_column_dictionary=col_d

        # read raw columns from file (renaming if necessary)
        self.raw_columns = pd.read_csv('{}raw_columns.txt'.format(dir_path),sep='\t').replace({'name':col_d})

        # read cdf tables and rename in ExternalIdentifiers col if necessary
        cdft = pd.read_csv('{}cdf_tables.txt'.format(dir_path),sep='\t',index_col='cdf_element')  # note index
        for k in col_d.keys():
            cdft['raw_identifier_formula'] = cdft['raw_identifier_formula'].str.replace('\<{}\>'.format(k),'<{}>'.format(col_d[k]))
        self.cdf_tables = cdft

        # determine how to treat ballot measures (ballot_measure_style)
        bms=pd.read_csv('{}ballot_measure_style.txt'.format(dir_path),sep='\t')
        assert 'short_name' in bms.columns, 'ballot_measure_style.txt does not have required column \'short_name\''
        assert 'truth' in bms.columns, 'ballot_measure_style.txt does not have required column \'truth\''
        self.ballot_measure_style=bms[bms['truth']]['short_name'].iloc[0]    # TODO error handling. This takes first line  marked "True"

        # determine whether the file has columns for counts by vote types, or just totals
        count_columns=pd.read_csv('{}count_columns.txt'.format(dir_path),sep='\t').replace({'RawName':col_d})
        self.count_columns=count_columns
        if list(count_columns['CountItemType'].unique()) == ['total']:
            self.totals_only=True
        else:
            self.totals_only=False

        if self.ballot_measure_style == 'yes_and_no_are_candidates':
            with open('{}ballot_measure_selections.txt'.format(dir_path),'r') as f:
                selection_list=f.readlines()
            self.ballot_measure_selection_list = [x.strip() for x in selection_list]


            bms_str=cdft.loc['BallotMeasureSelection','raw_identifier_formula']
            # note: bms_str will start and end with <>
            self.ballot_measure_selection_col = bms_str[1:-1]
        else:
            self.ballot_measure_selection_list=None # TODO is that necessary?

        with open('{}atomic_reporting_unit_type.txt'.format(dir_path),'r') as f:
            self.atomic_reporting_unit_type = f.readline()

if __name__ == '__main__':
    # get absolute path to local_data directory
    current_dir=os.getcwd()
    path_to_src_dir=current_dir.split('/election_anomaly/')[0]
    s = State('NC','{}/local_data/'.format(path_to_src_dir))
    mu = Munger('../../mungers/nc_primary/',cdf_schema_def_dir='../CDF_schema_def_info/')
    f = pd.read_csv('../../local_data/NC/data/2020p_asof_20200305/nc_primary/results_pct_20200303.txt',sep='\t')
    mu.check_new_datafile(f,s)

    print('Done (states_and_files)!')