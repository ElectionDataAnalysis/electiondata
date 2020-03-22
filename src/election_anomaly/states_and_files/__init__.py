#!usr/bin/python3
import os.path
import db_routines as dbr
import pandas as pd
import re
import munge_routines as mr
from sqlalchemy.orm import sessionmaker
import user_interface as ui


class State:
    def create_db(self):
        # create db
        con = dbr.establish_connection()
        cur = con.cursor()
        dbr.create_database(con,cur,self.short_name)
        cur.close()
        con.close()
        return

    def prepare_candidatecontests(self,session):
        """create/update corresponding CandidateContest records for general and primary election contests
        (and insert in cdf db if they don't already exist)"""
        # TODO need to run this only when/if Offices change

        office_file = os.path.join(
            self.path_to_state_dir,'context/Office.txt')
        co_kwargs = {'sep':'\t'}
        context_office = pd.read_csv(office_file,**co_kwargs)
        ui.resolve_nulls(context_office,office_file,kwargs=co_kwargs)

        cdf_office = pd.read_sql_table('Office',session.bind,index_col=None)
        cdf_ru = pd.read_sql_table('ReportingUnit',session.bind,index_col=None)

        # create dataframe of general election candidatecontests (cc) with right data
        cc = cdf_office.merge(
            context_office,left_on='Name',right_on='Name',suffixes=['','_context']
        ).merge(
            cdf_ru,left_on='ElectionDistrict',right_on='Name',suffixes=['_office','_ru']
        )
        # restrict to columns we need and set order
        cc = cc[['Name_office','VotesAllowed','NumberElected','NumberRunoff','Id_office','Id_ru','IsPartisan']]
        # ensure names of columns are correct (note we name contest after office)
        cc.rename(columns={'Id_ru':'ElectionDistrict_Id','Id_office':'Office_Id','Name_office':'Name'},inplace=True)
        # insert values for 'PrimaryParty_Id' column
        cc.loc[:,'PrimaryParty_Id'] = None
        # save cc with just general elections (as basis for defining party primary contests); cc_all will accumulate all party primaries too
        cc_all = cc.copy()

        cdf_p = pd.read_sql_table('Party',session.bind,None,index_col=None)
        for party_id in cdf_p['Id'].to_list():
            cc_primary = cc[cc['IsPartisan']].copy()  # non-partisan contests don't have party primaries, so omit them.
            cc_primary.loc[:,'PrimaryParty_Id'] = party_id
            cc_primary.loc[:,'Name'] =  cc_primary['Name'] + ' Primary;' + cdf_p[cdf_p['Id'] == party_id].iloc[0]['Name']
            # TODO can we use f'' here?
            cc_all = pd.concat([cc_all,cc_primary])

        mr.dframe_to_sql(cc_all,session,None,'CandidateContest')
        return cc_all

    def check_election_districts(self):
        """Looks in context file to check that every ElectionDistrict in Office.txt is listed in ReportingUnit.txt"""
        ed = pd.read_csv(os.path.join(self.path_to_state_dir,'context/Office.txt'),sep='\t',header=0).loc[:,'ElectionDistrict'].to_list()
        ru = list(pd.read_csv(os.path.join(self.path_to_state_dir,'context/ReportingUnit.txt'),sep='\t').loc[:,'Name'])
        missing = [x for x in ed if x not in ru]
        if len(missing) == 0:
            all_ok = True
            print('Congratulations! Every ElectionDistrict is a ReportingUnit!\n')
        else:
            all_ok = False
            print('Every ElectionDistrict must be a ReportingUnit. This is not optional!!')
            ui.show_sample(missing,'ElectionDistricts','are not yet ReportingUnits',
                           outfile='electiondistricts_missing_from_reportingunits.txt',dir=self.path_to_state_dir)
            input('Please make corrections to Office.txt or additions to ReportingUnit.txt to resolve the problem.\n'
                  'Then his return to continue.'
                  f'(Directory is {os.path.join(self.path_to_state_dir,"context")}')
            self.check_election_districts()
        return all_ok

    def add_to_context_dir(self,element,df):
        """Add the data in the dataframe <f> to the file corresponding
        to <element> in the <state>'s context folder.
        <f> must have all columns matching the columns in context/<element>.
        OK for <f> to have extra columns"""
        # TODO
        try:
            elts = pd.read_csv('{}context/{}.txt'.format(self.path_to_state_dir,element),sep='\t')
        except FileNotFoundError:
            print('File {}context/{}.txt does not exist'.format(self.path_to_state_dir,element))
            return
        else:
            for col in elts.columns:
                if col not in df.columns:
                    print('WARNING: Column {} not found, will be added with no data.'.format(col))
                    print('Be sure all necessary data is added before processing the file')
                    df.loc[:,col] = ''
            # pull and order necessary columns from <df>
            df_new = pd.concat([df[elts.columns],elts])
            df_new.to_csv('{}context/{}.txt'.format(self.path_to_state_dir,element),sep='\t',index=False)

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
        self.path_to_state_dir = path_to_parent_dir + short_name+'/'
        assert os.path.isdir(self.path_to_state_dir), 'Error: No directory ' + self.path_to_state_dir
        assert os.path.isdir(self.path_to_state_dir+'context/'), \
            'Error: No directory ' + self.path_to_state_dir+'context/'
        # Check that context directory is missing no essential files
        context_file_list = ['Office.txt','remark.txt','ReportingUnit.txt']
        file_missing_list = [ff for ff in context_file_list
                             if not os.path.isfile(f'{self.path_to_state_dir}context/{ff}')]
        assert file_missing_list == [], \
            f'Error: Missing files in {os.path.join(self.path_to_state_dir,"context")}:\n{file_missing_list}'


class Munger:
    def finalize_element(self,element,results,state,sess,project_root):
        """Guides user to make any necessary or desired changes in context/<element>.txt
        and makes corresponding changes to db"""
        finalized = False
        while not finalized:
            self.prepare_context_and_db(element,results,state,sess,project_path=project_root)
            if element == 'ReportingUnit':
                eds_ok = False
                while not eds_ok:
                    eds_ok = state.check_election_districts()
                    if not eds_ok:
                        # recheck Office and ReportingUnit
                        self.prepare_context_and_db(
                            'Office',results,state,sess,project_path=project_root)
                        self.prepare_context_and_db(
                            'ReportingUnit',results,state,sess,project_path=project_root)
            fin = input(f'Is the file context/{element}.txt finalized to your satisfaction (y/n)?\n')
            if fin == 'y':
                finalized = True
            else:
                input('Make your changes, then hit return to continue.')
        return

    def check_ballot_measure_selections(self):
        if len(self.ballot_measure_selection_list) == 0:
            print(f'There are no Ballot Measure Selections for the munger {self.name}.\n'
                  f'No ballot measure contests will be processed by this munger.')
        else:
            print(f'Ballot Measure Selections for the munger {self.name} are:\n'
              f'{", ".join(self.ballot_measure_selection_list)}')
        needs_warning = False
        correct = input('Does this list include every ballot measure selecton name in your datafile (y/n)?\n')
        while correct != 'y':
            needs_warning = True
            add_or_remove = input('Enter \'a\' to add and \'r\' to remove Ballot Measure Selections.\n')
            if add_or_remove == 'a':
                new = input(f'Enter a missing selection\n')
                if new != '':
                    self.ballot_measure_selection_list.append(new)
            elif add_or_remove == 'r' :
                idx, val = ui.pick_one(pd.DataFrame([[x] for x in self.ballot_measure_selection_list],columns=['Selection']),'Selection',item='Selection')
                if idx is not None:
                    self.ballot_measure_selection_list.remove(val)
            else:
                print('Answer not valid. Please enter \'a\' or \'r\'.')
            print(f'Ballot Measure Selections for the munger {self.name} are:\n'
                  f'{", ".join(self.ballot_measure_selection_list)}')
            correct = input('Is this consistent with your datafile (y/n)?\n')
        if needs_warning:
            print(f'To make this change permanent, edit the BallotMeasureSelection lines '
                  f'in {self.name}/raw_identifiers.txt')
        return

    def check_atomic_ru_type(self):
        print(f'This munger classifies each line of the datafile as type \'{self.atomic_reporting_unit_type}\'.')
        check_ru_type = input(
            f'\tIs it OK to treat every line of the datafile as \'{self.atomic_reporting_unit_type}\'(y/n)?\n')
        if check_ru_type != 'y':
            print('Datafile will not be processed.')
            raise Exception('Munger would assign wrong ReportingUnitType to datafile. Use a different munger.')
        else:
            return

    def check_candidatecontest(self,results,state,sess,project_path='.'):
        # TODO
        """report raw contests in <results> missing from raw_identifiers.txt
        and report cdf_internal names of contests in <results>-join-raw_identifiers
        that are missing from db, guiding user to fix if desired
        """
        ri = os.path.join(self.path_to_munger_dir,'raw_identifiers.txt')

        ri_df = pd.read_csv(ri,sep='\t')
        raw_from_results = set(mr.add_munged_column(
            results,self,'CandidateContest','CandidateContest_external')['CandidateContest_external'])
        raw_from_ri = set(ri_df[ri_df.cdf_element == 'CandidateContest'].loc[:,'raw_identifier_value'])
        missing_from_ri = [x for x in raw_from_results if x not in raw_from_ri]

        while missing_from_ri:
            print(f'Contests in the raw results with no corresponding CandidateContest line '
                  f'in {self.name}/raw_identifiers.txt will be ignored,\n'
                  f'and no results for these contests will be loaded into the database.')
            ui.show_sample(
                missing_from_ri,'contests in the raw results',
                f'have no corresponding CandidateContest line in {self.name}/raw_identifiers.txt',
                outfile='contests_missing_from_munger.txt')
            add_contests = input(f'Would you like to add any CandidateContests to {self.name}/raw_identifiers.txt (y/n)?\n')
            if add_contests == 'y':
                input(f'Add any desired contests to {self.name}/raw_identifiers.txt and hit return to continue')
                ri_df = pd.read_csv(ri,sep='\t')
                raw_from_ri = set(ri_df[ri_df.cdf_element == 'CandidateContest'].loc[:,'raw_identifier_value'])
                missing_from_ri = [x for x in raw_from_results if x not in raw_from_ri]
            else:
                missing_from_ri = []

        cdf_from_ri = set(ri_df[ri_df.cdf_element == 'CandidateContest'].loc[:,'cdf_internal_name'])
        cdf_df = pd.read_sql_table('CandidateContest',sess.bind)
        cdf_from_db = set(cdf_df['Name'])
        missing_from_db = {x for x in cdf_from_ri if x not in cdf_from_db}
        while missing_from_db:
            print(f'Some munged candidate contests in the datafile are missing from the database.\n')
            ui.show_sample(missing_from_db,'munged candidate contests','are missing from the database',
                           outfile='missing_ccs.txt')
            input(f'Add the office corresponding to each missing contest to {state.short_name}/context/Office.txt.\n'
                  f'This may require some research about the office. When ready, hit enter to continue.')
            self.prepare_context_and_db('Office',results,state,sess,project_path=project_path)
            cdf_df = pd.read_sql_table('CandidateContest',sess.bind)

            cdf_from_db = set(cdf_df['Name'])
            missing_from_db = {x for x in cdf_from_ri if x not in cdf_from_db}
        return

    def prepare_context_and_db(self,element,results,state,sess,project_path='.'):
        """Loads info from context/<element>.txt into db; checks results file <element>s against munger;
        then checks munger against db. Throughout, guides user to make corrections in context/<element>.txt;
        finally loads final context/<element>.txt into db. Note that this will only add records to db, never remove. """
        # TODO why do ReportingUnits get checked twice somehow?
        print(f'Updating database with info from {state.short_name}/context/{element}.txt.\n')
        no_dupes = False
        while no_dupes == False:
            source_file = os.path.join(state.path_to_state_dir,f'context/{element}.txt')
            source_df = pd.read_csv(source_file,sep='\t')

            dupes,source_df = ui.find_dupes(source_df)
            if not dupes.empty:
                input(f'WARNING: {state.short_name}/context/{element}.txt has duplicates.\n'
                      f'Edit the file to remove the duplication, then hit return to continue')
            else:
                no_dupes = True

        mr.load_context_dframe_into_cdf(sess,state,source_df,element,
                                        os.path.join(project_path,'election_anomaly/CDF_schema_def_info'))

        mr.add_munged_column(results,self,element,f'{element}_external')
        results_elements = results[f'{element}_external'].unique()

        mu_elements = self.raw_identifiers[self.raw_identifiers.cdf_element==element]
        elements_mixed = pd.DataFrame(
            results_elements,columns=[f'{element}_external']).merge(
            mu_elements,how='left',left_on=f'{element}_external',right_on='raw_identifier_value'
            ).fillna('')


        # are there elements in results file that cannot be munged?
        not_identified = elements_mixed[elements_mixed.raw_identifier_value.isnull()].loc[:,
                         f'{element}_external'].to_list()
        # and are not in unmunged_{element}s.txt?
        try:
            with open(os.path.join(self.path_to_munger_dir,f'unmunged_{element}s_in_datafile.txt'),'r') as f:
                unmunged_elements_in_datafile = [x.strip() for x in f.readlines()]
                not_munged = [x for x in not_identified if x not in unmunged_elements_in_datafile]
        except FileNotFoundError:
            not_munged = not_identified

        if len(not_munged) > 0:
            print(f'Some {element}s in the results file cannot be interpreted by the munger {self.name}.')
            print(f'Note: {element}s listed in unmunged_{element}s.txt are interpreted as \'to be ignored\'.')
            outfile = f'unmunged_{element}s.txt'
            ui.show_sample(not_munged,f'{element}s in datafile','cannot be munged',outfile=outfile,dir=self.path_to_munger_dir)
            add_to_munger = input('Would you like to add some/all of these to the munger (y/n)?\n')
            if add_to_munger == 'y':
                input(f'For each {element} you want to add to the munger:\n'
                    f'\tCut the corresponding line in {self.name}/{outfile} '
                    f'\tAdd a corresponding line the file {self.name}/raw_identifiers.txt, \n'
                    f'\tincluding creating a name to be used internally in the Common Data Format database Name field.\n'
                    f'\tThen edit the file {state.short_name}/context/{element}.txt, adding a line for each new {element}.\n\n'
                    f'\tMake sure the internal cdf name is exactly the same in both files.\n'
                    f'\tYou may need to do some contextual research to fill all the fields in {element}.txt\n\n'
                    f'Then hit return to continue.\n')

        # add all elements from context/ to db
        source_df = pd.read_csv(f'{state.path_to_state_dir}context/{element}.txt',sep='\t')
        mr.load_context_dframe_into_cdf(sess,
                                        state,source_df,element,
                                        CDF_schema_def_dir=os.path.join(project_path,
                                                                        'election_anomaly/CDF_schema_def_info'))

        db_element_df = pd.read_sql_table(element,sess.bind)
        db_elements = list(db_element_df['Name'].unique())

        # are there elements recognized by munger but not in db?
        munged_elements = elements_mixed[elements_mixed.raw_identifier_value.notnull()].loc[:,'cdf_internal_name'].to_list()
        if '' in munged_elements:
            munged_elements = munged_elements.remove('')  # remove any empty strings (e.g., from lines with no Party)
        if munged_elements:
            bad_set = {x for x in munged_elements if x not in db_elements}
        else:
            bad_set = {}

        if len(bad_set) > 0:
            print(f'Election results for munged {element}s missing from database will not be processed.')
            outfile = f'{element}s_munged_but_not_in_db.txt'
            ui.show_sample(bad_set,f'munged elements','are in raw_identifiers.txt but not in the database',
                           outfile=outfile,dir=self.path_to_munger_dir)
            add_to_db = input('Would you like to add some/all of these to the database (y/n)?\n')
            if add_to_db == 'y':
                input(f'For each {element} you want to add to the database:\n'
                        f'\tCut the corresponding line in {self.name}/{outfile}.\n'
                        f'\tAdd a line the file {state.short_name}/context/{element}.txt.\n'
                        f'\tCopy the internal cdf name from {self.name}/raw_identifiers.txt'
                        f' and paste it into {state.short_name}/context/{element}.txt.\n'
                        f'\tYou may need to do some contextual research to fill the other fields in {element}.txt\n\n'
                        f'Then hit return to continue.\n')
        source_df = pd.read_csv(f'{state.path_to_state_dir}context/{element}.txt',sep='\t')
        mr.load_context_dframe_into_cdf(sess,
                                        state,source_df,element,
                                        CDF_schema_def_dir=os.path.join(project_path,
                                                                        'election_anomaly/CDF_schema_def_info'))
        return

    def raw_cols_match(self,df):
        cols = self.raw_columns.name
        if set(cols).issubset(df.columns):
            return True
        else:
            missing = [x for x in set(cols) if x not in df.columns]
            print(f'Missing columns are {missing}')
            return False

    def check_new_results_dataset(self,results,state,sess,contest_type,project_root='.'):
        """<results> is a results dataframe of a single <contest_type>;
        this routine should add what's necessary to the munger to treat the dataframe,
        keeping backwards compatibility and exiting gracefully if dataframe needs different munger."""

        assert self.raw_cols_match(results), \
            f"""A column in {results.columns} is missing from raw_columns.txt."""

        if contest_type == 'Candidate':
            # check Party, Office, ReportingUnit in context & db, updating if necessary (prereq to checking CandidateContests)
            for element in ['Party','Office','ReportingUnit']:
                self.finalize_element(element,results,state,sess,project_root)
            # After Party and Office are finalized, prepare CandidateContest and check against munger
            state.prepare_candidatecontests(sess)
            self.check_candidatecontest(results,state,sess,project_path=project_root)

        if contest_type == 'BallotMeasure':
            self.finalize_element('ReportingUnit',results,state,sess,project_root)
            # TODO feature: prevent finalizing RUs twice if both contest_types are treated
            # check that munger processes ballot measure contests appropriately.
            print(f'This munger assumes that {self.ballot_measure_style_description}.')
            check_bms = input(f'\tIs this appropriate for the datafile (y/n)?\n')
            if check_bms != 'y':
                raise Exception('Datafile will not be processed. Use a different munger and try again.')
        return

    def add_to_raw_identifiers(self,df):
        """Adds rows in <df> to the raw_identifiers.txt file and to the attribute <self>.raw_identifiers"""
        for col in mu.raw_identifiers.columns:
            assert col in df.columns, 'Column {} is not found in the dataframe'.format(col)
        # restrict to columns needed, and in the right order
        df = df[mu.raw_identifiers.columns]
        # add rows to <self>.raw_identifiers
        self.raw_identifiers = pd.concat([self.raw_identifiers,df]).drop_duplicates()
        # update the external munger file
        self.raw_identifiers.to_csv(f'{self.path_to_munger_dir}raw_identifiers.txt',sep='\t',index=False)
        return

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
        """<dir_path> is the directory for the munger."""
        assert os.path.isdir(dir_path),f'{dir_path} is not a directory'
        for ff in ['cdf_tables.txt','atomic_reporting_unit_type.txt','count_columns.txt']:
            if not os.path.isfile(os.path.join(dir_path,ff)):
                input(f'Directory {dir_path} does not contain file {ff}. Please create it and hit return to continue')
        self.name=dir_path.split('/')[-1]    # e.g., 'nc_general'

        self.path_to_munger_dir=dir_path

        # read raw_identifiers file into a table
        # note no natural index column
        self.raw_identifiers=pd.read_csv(os.path.join(dir_path,'raw_identifiers.txt'),sep='\t')

        # define dictionary to change any column names that match internal CDF names
        col_d = {t:f'{t}_{self.name}'
                 for t in os.listdir(os.path.join(cdf_schema_def_dir,'Tables'))}
        self.rename_column_dictionary=col_d

        # read raw columns from file (renaming if necessary)
        self.raw_columns = pd.read_csv(os.path.join(dir_path,'raw_columns.txt'),sep='\t').replace({'name':col_d})

        # read cdf tables and rename in ExternalIdentifiers col if necessary
        cdf_table_file = os.path.join(dir_path,'cdf_tables.txt')
        cdft = ui.confirm_or_correct_cdf_table_file(cdf_table_file,self.raw_columns.name.to_list()).set_index('cdf_element')
        # change names for raw columns whose names match cdf elements
        for k in col_d.keys():
            cdft['raw_identifier_formula'] = cdft['raw_identifier_formula'].str.replace(
                '\<{}\>'.format(k),'<{}>'.format(col_d[k]))
        self.cdf_tables = cdft

        # determine how to treat ballot measures (ballot_measure_style) and its description
        bms_file = os.path.join(dir_path,'ballot_measure_style.txt')
        bmso_file=os.path.join(os.path.abspath(os.path.join(dir_path,os.pardir)),
                                       'ballot_measure_style_options.txt')
        self.ballot_measure_style,self.ballot_measure_style_description = ui.confirm_or_correct_ballot_measure_style(
            bmso_file,bms_file)

        # determine whether the file has columns for counts by vote types, or just totals

        count_columns_file = os.path.join(dir_path,'count_columns.txt')
        count_columns_df=pd.read_csv(count_columns_file,sep='\t').replace({'RawName':col_d})
        self.count_columns = ui.check_count_columns(
            count_columns_df,count_columns_file,self.path_to_munger_dir,cdf_schema_def_dir)
        if list(self.count_columns['CountItemType'].unique()) == ['total']:
            self.totals_only=True
        else:
            self.totals_only=False

        if self.ballot_measure_style == 'yes_and_no_are_candidates':
            ri_df = pd.read_csv(os.path.join(dir_path,'raw_identifiers.txt'),sep='\t')
            self.ballot_measure_selection_list = ri_df[ri_df.cdf_element=='BallotMeasureSelection'].loc[:,'raw_identifier_value'].to_list()

            bms_str=cdft.loc['BallotMeasureSelection','raw_identifier_formula']
            # note: bms_str will start and end with <>
            self.ballot_measure_selection_col = bms_str[1:-1]
        else:
            self.ballot_measure_selection_list=None  # TODO is that necessary?

        with open(os.path.join(dir_path,'atomic_reporting_unit_type.txt'),'r') as ff:
            self.atomic_reporting_unit_type = ff.readline()


if __name__ == '__main__':
    # get absolute path to jurisdictions directory
    current_dir=os.getcwd()
    path_to_src_dir=current_dir.split('/election_anomaly/')[0]

    s = State('NC_test2',f'{path_to_src_dir}/jurisdictions/')
    mu = Munger('../../mungers/not_for_prime_time_NC/',cdf_schema_def_dir='../CDF_schema_def_info/')
    f = pd.read_csv('../../jurisdictions/NC/data/2018g/nc_general',sep='\t')

    # initialize main session for connecting to db
    eng, meta_generic = dbr.sql_alchemy_connect(db_name=s.short_name)
    Session = sessionmaker(bind=eng)
    session = Session()

    mu.check_new_results_dataset(f,s,session,project_root=path_to_src_dir)

    print('Done (states_and_files)!')
