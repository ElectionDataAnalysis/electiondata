#!usr/bin/python3
import os.path
import db_routines as dbr
import pandas as pd
import warnings   # TODO use warnings module to handle warnings in all files
import munge_routines as mr
import user_interface as ui
import re
import numpy as np
from pathlib import Path
import csv


class Jurisdiction:
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
            self.path_to_juris_dir,'context/Office.txt')
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
        # save cc with just general elections (as basis for defining party primary contests);
        # cc_all will accumulate all party primaries too
        cc_all = cc.copy()

        cdf_p = pd.read_sql_table('Party',session.bind,None,index_col=None)
        for party_id in cdf_p['Id'].to_list():
            cc_primary = cc[cc['IsPartisan']].copy()  # non-partisan contests don't have party primaries, so omit them.
            cc_primary.loc[:,'PrimaryParty_Id'] = party_id
            cc_primary.loc[:,'Name'] = cc_primary['Name'] + ' Primary;' + cdf_p[cdf_p['Id'] == party_id].iloc[0]['Name']
            # TODO can we use f'' here?
            cc_all = pd.concat([cc_all,cc_primary])

        dbr.dframe_to_sql(cc_all,session,'CandidateContest')
        return cc_all

    def check_dependencies(self,element):
        """Looks in context file to check that every ElectionDistrict in <element>.txt is listed in <target>.txt,
        """
        d = context_dependency_dictionary()
        context_dir = os.path.join(self.path_to_juris_dir,"context")
        f_path = os.path.join(context_dir,f'{element}.txt')
        if os.path.isfile(f_path):
            element_df = pd.read_csv(f_path,sep='\t',index_col=None)
        else:
            ensure_context(self.path_to_juris_dir,None)

        # Find all dependent columns
        dependent = [c for c in element_df if c in d.keys()]
        changed_elements = set()
        report = [f'In context/{element}.txt:']
        for c in dependent:
            target = d[c]
            ed = pd.read_csv(os.path.join(context_dir,f'{element}.txt'),sep='\t',header=0).loc[:,c].to_list()
            ru = list(pd.read_csv(os.path.join(context_dir,f'{target}.txt'),sep='\t').loc[:,'Name'])
            missing = [x for x in ed if x not in ru and not np.isnan(x)]
            if len(missing) == 0:
                report.append(f'Every {c} is a {target}.')
            else:
                changed_elements.add(element)
                changed_elements.add(target)
                print(f'Every {c} must be a {target}. This is not optional!!')
                ui.show_sample(missing,f'{c}s',f'are not yet {target}s')
                input(f'Please make corrections to {element}.txt or additions to {target}.txt to resolve the problem.\n'
                      'Then his return to continue.')
                changed_elements.update(self.check_dependencies(target))
        if dependent:
            print ('\n\t'.join(report))
        if changed_elements:
            print(f'(Directory is {context_dir}')
        return changed_elements

    def add_to_context_dir(self,element,df):
        """Add the data in the dataframe <df> to the file corresponding
        to <element> in the Jurisdiction's context folder.
        <df> must have all columns matching the columns in context/<element>.
        OK for <df> to have extra columns"""
        # TODO
        try:
            elts = pd.read_csv(f'{self.path_to_juris_dir}context/{element}.txt',sep='\t')
        except FileNotFoundError:
            print(f'File {self.path_to_juris_dir}context/{element}.txt does not exist')
            return
        else:
            for col in elts.columns:
                if col not in df.columns:
                    warnings.warn(f'WARNING: Column {col} not found, will be added with no data.')
                    print('Be sure all necessary data is added before processing the file')
                    df.loc[:,col] = ''
            # pull and order necessary columns from <df>
            df_new = pd.concat([df[elts.columns],elts])
            df_new.to_csv(f'{self.path_to_juris_dir}context/{element}.txt',sep='\t',index=False)

            # TODO insert into cdf.<element>, using mr.load_context_dframe_into_cdf
            #  in separate function, or in wrapper

            return

    def extract_context(self,results,munger,element):
        # TODO
        """Return a dataframe of context info for <element> extracted
        from raw results file <results> via <munger>, suitable for
        applying add_to_context_dir()"""
        # TODO help user add appropriate lines to munger/raw_identifiers.txt and update munger
        return

    def __init__(self,short_name,path_to_parent_dir,project_root=None,check_context=False):        # reporting_units,elections,parties,offices):
        """ short_name is the name of the directory containing the jurisdiction info, including data,
         and is used other places as well.
         path_to_parent_dir is the parent directory of dir_name
        """
        if not project_root:
            project_root = ui.get_project_root()

        self.short_name = ui.pick_or_create_directory(path_to_parent_dir,short_name)
        self.path_to_juris_dir = os.path.join(path_to_parent_dir, self.short_name)

        if check_context:
            # Ensure that context directory exists and is missing no essential files
            check_jurisdiction_directory(self.path_to_juris_dir)
            ensure_context(self.path_to_juris_dir,project_root)


class Munger:
    def finalize_element(self,element,results,jurisdiction,sess,project_root):
        """Guides user to make any necessary or desired changes in context/<element>.txt
        and makes corresponding changes to db"""
        finalized = False
        while not finalized:
            self.prepare_context_and_db(element,results,jurisdiction,sess,project_path=project_root)

            # check dependencies
            all_ok = False
            while not all_ok:
                changed_elements = jurisdiction.check_dependencies(element)
                if changed_elements:
                    # recheck items from change list
                    for e in changed_elements:
                        self.prepare_context_and_db(e,results,jurisdiction,sess,project_path=project_root)
                else:
                    all_ok=True

            fin = input(f'Is the file {element}.txt finalized to your satisfaction (y/n)?\n')
            if fin == 'y':
                finalized = True
            else:
                input('Make your changes, then hit return to continue.')
        return

    def check_ballot_measure_selections(self):
        if self.ballot_measure_style == 'yes_and_no_are_candidates':
            if len(self.ballot_measure_selection_list) == 0:
                print(f'There are no Ballot Measure Selections for the munger {self.name}.\n'
                      f'No ballot measure contests will be processed by this munger.')
            else:
                print(f'Ballot Measure Selections for the munger {self.name} are:\n'
                  f'{", ".join(self.ballot_measure_selection_list)}')
            if self.ballot_measure_count_column_selections:
                warnings.warn(
                    f'WARNING: When {self.ballot_measure_style_description},\n'
                    f'there should be no content in the munger\'s ballot_measure_count_column_selections attributes.\n'
                    f'Check for unnecessary rows in {self.name}/ballot_measure_count_column_selections.txt.')
            needs_warning = False
            correct = input('Does this list include every ballot measure selection name in your datafile (y/n)?\n')
            while correct != 'y':
                needs_warning = True
                add_or_remove = input('Enter \'a\' to add and \'r\' to remove Ballot Measure Selections.\n')
                if add_or_remove == 'a':
                    new = input(f'Enter a missing selection\n')
                    if new != '':
                        self.ballot_measure_selection_list.append(new)
                elif add_or_remove == 'r':
                    idx, val = ui.pick_one(
                        pd.DataFrame([[x] for x in self.ballot_measure_selection_list],
                                     columns=['Selection']),'Selection',item='Selection')
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
        elif self.ballot_measure_style == 'yes_and_no_are_candidates':
            if self.ballot_measure_selection_list:
                warnings.warn(
                    f'WARNING: there should be no ballot selections in the ballot_measure_selection_list attribute.\n'
                    f'when {self.ballot_measure_style_description}.\n'
                    f'Check for unnecessary rows in {self.name}/raw_identifiers.txt.')
            # TODO check that every field in ballot_measure_count_column_selections is in count_columns
            for f in self.ballot_measure_count_column_selections.fieldname.to_list():
                if f not in self.count_columns.RawName:
                    input(f'The column {f} in {self.name}/ballot_measure_count_column_selections.txt\n'
                          f'is not listed in the {self.name}/count_columns attribute.\n'
                          f'Please fix this by editing one or both files. Hit return when done')
        # TODO allow corrections at run time.
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

    def check_candidatecontest(self,results,jurisdiction,sess,project_path='.'):
        # TODO
        """report raw contests in <results> missing from raw_identifiers.txt
        and report cdf_internal names of contests in <results>-join-raw_identifiers
        that are missing from db, guiding user to fix if desired
        """
        ri = os.path.join(self.path_to_munger_dir,'raw_identifiers.txt')

        ri_df = pd.read_csv(ri,sep='\t')
        raw_from_results = set(mr.add_munged_column_NEW(
            results,self,'CandidateContest',inplace=False)[f'CandidateContest_raw'])
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
            add_contests = input(
                f'Would you like to add any CandidateContests to {self.name}/raw_identifiers.txt (y/n)?\n')
            if add_contests == 'y':
                input(f'Add any desired contests to {self.name}/raw_identifiers.txt and hit return to continue')
                ri_df = pd.read_csv(ri,sep='\t',keep_default_na=False)
                # If keep_default_na is False, and na_values are not specified, no strings will be parsed as NaN.
                # TODO use keep_default_na in other places too?
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
            input(
                f'Add the office corresponding to each missing contest to '
                f'{jurisdiction.short_name}/context/Office.txt.\n'
                f'This may require some research about the office. When ready, hit enter to continue.')
            self.prepare_context_and_db('Office',results,jurisdiction,sess,project_path=project_path)
            cdf_df = pd.read_sql_table('CandidateContest',sess.bind)

            cdf_from_db = set(cdf_df['Name'])
            missing_from_db = {x for x in cdf_from_ri if x not in cdf_from_db}
        return

    def prepare_context_and_db(self,element,raw,jurisdiction,sess,project_path='.',enumeration=False):
        """Loads info from context/<element>.txt into db; checks <element>s from file <raw> against munger;
        then checks munger against db. Throughout, guides user to make corrections in context/<element>.txt;
        finally loads final context/<element>.txt into db. Note that this will only add records to db, never remove. """

        # get results and change column names to distinguish raw from munged
        results = raw.copy()
        d = {x:f'{x}_{self.field_rename_suffix}' for x in self.field_list}
        results.rename(columns=d,inplace=True)
        mr.add_munged_column_NEW(results,self,element)
        results_elements = results[f'{element}_raw'].unique()

        mu_elements = self.raw_identifiers[self.raw_identifiers.cdf_element==element]
        elements_mixed = pd.DataFrame(
            results_elements,columns=[f'{element}_self.field_rename_suffix']).merge(
            mu_elements,how='left',left_on=f'{element}_self.field_rename_suffix',right_on='raw_identifier_value'
            ).fillna('')

        # are there elements in results file that cannot be munged?
        not_identified = elements_mixed[elements_mixed.raw_identifier_value==''].loc[:,
                         f'{element}_self.field_rename_suffix'].to_list()
        # and are not in unmunged_{element}s.txt?
        try:
            with open(os.path.join(self.path_to_munger_dir,f'unmunged_{element}s_in_datafile.txt'),'r') as f:
                unmunged_elements_in_datafile = [x.strip() for x in f.readlines()]
                # TODO blank values will not be included in <not_munged> - how does this flow through
            not_munged = [x for x in not_identified if x not in unmunged_elements_in_datafile and x != '']
        except FileNotFoundError:
            not_munged = [x for x in not_identified if x !='']

        if len(not_munged) > 0:
            print(f'Some {element}s in the results file cannot be interpreted by the munger {self.name}.')
            if element[-7:] == 'Contest' or element[-9:] == 'Selection':
                contest_msg = \
                    '\nDon\'t worry if Ballot Measure items are listed as unmunged Candidate items, or vice versa.'
            else:
                contest_msg = ''
            print(f'Note: {element}s listed in unmunged_{element}s.txt are interpreted as \'to be ignored\'.{contest_msg}')
            outfile = f'unmunged_{element}s.txt'
            # TODO when showing unmunged candidates, show only unmunged candidates from munged contests
            #  but first check for unmunged contests!
            ui.show_sample(
                not_munged,f'{element}s in datafile','cannot be munged',outfile=outfile,dir=self.path_to_munger_dir)
            add_to_munger = input('Would you like to add some/all of these to the munger (y/n)?\n')
            if add_to_munger == 'y':
                input(f'For each {element} you want to add to the munger:\n'
                    f'\tCut the corresponding line in {self.name}/{outfile} '
                    f'\tAdd a corresponding line the file {self.name}/raw_identifiers.txt, \n'
                    f'\tcreating a name to be used internally in the Common Data Format database Name field.\n'
                    f'\tThen edit the file {jurisdiction.short_name}/context/{element}.txt, '
                    f'adding a line for each new {element}.\n\n'
                    f'\tMake sure the internal cdf name is exactly the same in both files.\n'
                    f'\tYou may need to do some contextual research to fill all the fields in {element}.txt\n\n'
                    f'Then hit return to continue.\n')

        # get source file for <element> from enumerations or context folders
        if enumeration:
            source_file = os.path.join(project_path,'election_anomaly/CDF_schema_def_info/enumerations',f'{element}.txt')
            print(f'Updating database with info from /CDF_schema_def_info/enumerations/{element}.txt.\n')
            source_df = pd.read_csv(source_file,sep='\t')
        else:
            source_file = os.path.join(jurisdiction.path_to_juris_dir,'context',f'{element}.txt')
            print(f'Updating database with info from {jurisdiction.short_name}/context/{element}.txt.\n')
            source_df = pd.read_csv(source_file,sep='\t')

            # check that context folder contents are consistent
            if not os.path.isfile(source_file):
                ensure_context(jurisdiction.path_to_juris_dir,project_path)

            source_df = dedupe(
               source_df,source_file,warning=f'{jurisdiction.short_name}/context/{element}.txt has duplicates.')

            mr.load_context_dframe_into_cdf(sess,project_path,jurisdiction,source_df,element,
                                        os.path.join(project_path,'election_anomaly/CDF_schema_def_info'))

        # add all elements from context/ to db
        source_df = pd.read_csv(source_file,sep='\t')
        mr.load_context_dframe_into_cdf(sess,project_path,
                                        jurisdiction,source_df,element,
                                        cdf_schema_def_dir=os.path.join(project_path,
                                                                        'election_anomaly/CDF_schema_def_info'))

        db_element_df = pd.read_sql_table(element,sess.bind)
        # TODO: some elements need more than just name to match. E.g., Candidate may have Party_Id
        name_field = mr.get_name_field(element)
        db_elements = list(db_element_df[name_field].unique())

        # are there elements recognized by munger but not in db?
        munged_elements = elements_mixed[
                              elements_mixed.raw_identifier_value.notnull()].loc[:,'cdf_internal_name'].to_list()
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
                        f'\tAdd a line the file {jurisdiction.short_name}/context/{element}.txt.\n'
                        f'\tCopy the internal cdf name from {self.name}/raw_identifiers.txt'
                        f' and paste it into {jurisdiction.short_name}/context/{element}.txt.\n'
                        f'\tYou may need to do some contextual research to fill the other fields in {element}.txt\n\n'
                        f'Then hit return to continue.\n')
        else:
            print(f'Congrats! Each munged {element} is in the database!')
        source_df = pd.read_csv(source_file,sep='\t')
        mr.load_context_dframe_into_cdf(sess,project_path,
                                        jurisdiction,source_df,element,
                                        cdf_schema_def_dir=os.path.join(project_path,
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

    def check_new_results_dataset(self,results,jurisdiction,sess,contest_type,project_root='.'):
        """<results> is a results dataframe of a single <contest_type>;
        this routine should add what's necessary to the munger to treat the dataframe,
        keeping backwards compatibility and exiting gracefully if dataframe needs different munger."""

        assert self.raw_cols_match(results), \
            f"""A column in {results.columns} is missing from raw_columns.txt."""

        if contest_type == 'Candidate':
            # check Party, Office, ReportingUnit in context & db, updating if necessary (prereq to checking
            # CandidateContests)
            for element in ['Party','Office','ReportingUnit']:
                self.finalize_element(element,results,jurisdiction,sess,project_root)
            # After Party and Office are finalized, prepare CandidateContest and check against munger
            jurisdiction.prepare_candidatecontests(sess)
            self.check_candidatecontest(results,jurisdiction,sess,project_path=project_root)

        if contest_type == 'BallotMeasure':
            self.finalize_element('ReportingUnit',results,jurisdiction,sess,project_root)
            # TODO feature: prevent finalizing RUs twice if both contest_types are treated
            # check that munger processes ballot measure contests appropriately.
            print(f'This munger assumes that {self.ballot_measure_style_description}.')
            check_bms = input(f'\tIs this appropriate for the datafile (y/n)?\n')
            if check_bms != 'y':
                raise Exception('Datafile will not be processed. Use a different munger and try again.')
        return

    def check_new_results_dataset_NEW(self,results,jurisdiction,sess,project_root='.'):
        """<results> is a results dataframe of a single <contest_type>;
        this routine should add what's necessary to the munger to treat the dataframe,
        keeping backwards compatibility and exiting gracefully if dataframe needs different munger."""

        # check Party, Office, ReportingUnit in context & db, updating if necessary (prereq to checking
        # CandidateContests)
        for element in ['Party','Office','ReportingUnit','Candidate','BallotMeasureContest']:
            self.finalize_element(element,results,jurisdiction,sess,project_root)
        # After Party and Office are finalized, prepare CandidateContest and check against munger
        jurisdiction.prepare_candidatecontests(sess)
        self.check_candidatecontest(results,jurisdiction,sess,project_path=project_root)

        return

    def check_against_self(self):
        """check that munger is internally consistent; offer user chance to correct munger"""
        # TODO write this function
        checked = False
        while not checked:
            checked = True
            problems = []

            # every cdf_element in raw_identifiers.txt is in cdf_elements.cdf_element
            missing = [x for x in self.raw_identifiers.cdf_element.unique() if x not in self.cdf_elements.index]
            if missing:
                m_str = ','.join(missing)
                problems.append(
                    f'''At least one cdf_element in raw_identifiers.txt is missing from cdf_elements.txt: {m_str}''')

            # every source is either row, column or other
            bad_source = [x for x in self.cdf_elements.source if x not in ['row','column','other']]
            if bad_source:
                b_str = ','.join(bad_source)
                problems.append(f'''At least one source in cdf_elements.txt is not recognized: {b_str} ''')

            # formulas have good syntax
            bad_formula = [x for x in self.cdf_elements.raw_identifier_formula.unique() if not mr.good_syntax(x)]
            if bad_formula:
                f_str = ','.join(bad_formula)
                problems.append(f'''At least one formula in cdf_elements.txt has bad syntax: {f_str} ''')

            # for each column-source record in cdf_element, contents of bracket are numbers in the header_rows
            p_not_just_digits = re.compile(r'<.*\D.*>')
            p_catch_digits = re.compile(r'<(\d+)>')
            bad_column_formula = set()
            for i,r in self.cdf_elements[self.cdf_elements.source == 'column'].iterrows():
                if p_not_just_digits.search(r['raw_identifier_formula']):
                    bad_column_formula.add(r['raw_identifier_formula'])
                else:
                    integer_list = [int(x) for x in p_catch_digits.findall(r['raw_identifier_formula'])]
                    bad_integer_list = [x for x in integer_list if (x > self.header_row_count-1 or x < 0)]
                    if bad_integer_list:
                        bad_column_formula.add(r['raw_identifier_formula'])
            if bad_column_formula:
                cf_str = ','.join(bad_column_formula)
                problems.append(f'''At least one column-source formula in cdf_elements.txt has bad syntax: {cf_str} ''')

            # TODO if field in formula matches an element self.cdf_element.index,
            #  check that rename is not also a column
            if problems:
                checked = False
                problem_str = '\n\t'.join(problems)
                print(f'Problems found:\n{problem_str} ')
                input(f'Correct the problems by editing the files in the directory {self.path_to_munger_dir}\n'
                      f'Then hit enter to continue.')
                [self.cdf_elements,self.atomic_reporting_unit_type,self.header_row_count,
                 self.field_name_row,] = read_munger_info_from_files(self.path_to_munger_dir)
        return

    def check_against_db(self,sess):
        """check that munger is consistent with db; offer user chance to correct munger"""
        checked = False
        while not checked:
            checked = True
            problems = []
            # set of cdf_elements in cdf_elements.txt is same as set pulled from db
            [db_elements, db_enumerations, db_joins, db_others] = dbr.get_cdf_db_table_names(sess.bind)
            db_elements.add('CountItemType')
            db_elements.add('BallotMeasureSelection')
            db_elements.remove('CandidateSelection')
            db_elements.remove('ExternalIdentifier')
            db_elements.remove('VoteCount')
            db_elements.remove('Office')
            # TODO why these? make this programmatic

            m_elements = self.cdf_elements.index
            db_only = [x for x in db_elements if x not in m_elements]
            m_only = [x for x in m_elements if x not in db_elements]

            if db_only:
                db_str = ','.join(db_only)
                problems.append(f'Some cdf elements in the database are not listed in the munger: {db_str}')
            if m_only:
                m_str = ','.join(m_only)
                problems.append(f'Some cdf elements in the munger are not in the database: {m_str}')

            if problems:
                checked = False
                problem_str = '\n\t'.join(problems)
                print(f'Problems found:\n{problem_str} ')
                input(f'Correct the problems by editing the files in the directory {self.path_to_munger_dir}\n'
                      f'Then hit enter to continue.')
                [self.cdf_elements,self.atomic_reporting_unit_type,self.header_row_count,
                 self.field_name_row] = read_munger_info_from_files(self.path_to_munger_dir)
        return

    def check_against_datafile(self,raw,cols_to_munge,count_columns):
        """check that munger is compatible with datafile <raw>;
        offer user chance to correct munger"""
        checked = False
        while not checked:
            checked = True
            problems = []
            # check for unmunged rows and report

            if problems:
                checked = False
                problem_str = '\n\t'.join(problems)
                print(f'Problems found:\n{problem_str} ')
                input(f'Correct the problems by editing the files in the directory {self.path_to_munger_dir}\n'
                      f'Then hit enter to continue.')
                [self.cdf_elements,self.atomic_reporting_unit_type,self.header_row_count,
                 self.field_name_row] = read_munger_info_from_files(self.path_to_munger_dir)
        # TODO write this function
        return

    def add_to_raw_identifiers(self,df):
        """Adds rows in <df> to the raw_identifiers.txt file and to the attribute <self>.raw_identifiers"""
        for col in self.raw_identifiers.columns:
            assert col in df.columns, 'Column {} is not found in the dataframe'.format(col)
        # restrict to columns needed, and in the right order
        df = df[self.raw_identifiers.columns]
        # add rows to <self>.raw_identifiers
        self.raw_identifiers = pd.concat([self.raw_identifiers,df]).drop_duplicates()
        # update the external munger file
        self.raw_identifiers.to_csv(f'{self.path_to_munger_dir}raw_identifiers.txt',sep='\t',index=False)
        return

    def __init__(self,dir_path):
        """<dir_path> is the directory for the munger."""
        while not os.path.isdir(dir_path):
            input(f'{dir_path} is not a directory. Please create it and hit return to continue.')
        for ff in ['cdf_elements.txt','format.txt']:
            while not os.path.isfile(os.path.join(dir_path,ff)):
                input(f'Directory \n\t{dir_path}\ndoes not contain file {ff}.\n'
                      f'Please create the file and hit return to continue')

        self.name= os.path.basename(dir_path)  # e.g., 'nc_general'
        self.path_to_munger_dir=dir_path

        [self.cdf_elements,self.atomic_reporting_unit_type,self.header_row_count,
         self.field_name_row] = read_munger_info_from_files(self.path_to_munger_dir)

        self.field_rename_suffix = '___' # NB: must not match any suffix of a cdf element name;

        # used repeatedly, so calculated once for convenience
        # TODO find places this is calculated; replace with self.field_list
        self.field_list = set()
        for t,r in self.cdf_elements.iterrows():
            self.field_list=self.field_list.union(r['fields'])

# TODO before processing context files into db, alert user to any duplicate names.
#  Enforce name change? Or just suggest?


def read_munger_info_from_files(dir_path):
    # read cdf_element info and add column for list of fields used in formulas
    cdf_elements = pd.read_csv(os.path.join(dir_path,'cdf_elements.txt'),sep='\t',index_col='name').fillna('')
    cdf_elements.loc[:,'fields'] = None
    for i,r in cdf_elements.iterrows():
        text_field_list,last_text = mr.text_fragments_and_fields(cdf_elements.loc[i,'raw_identifier_formula'])
        cdf_elements.loc[i,'fields'] = [f for t,f in text_field_list]

    # read formatting info
    format_info = pd.read_csv(os.path.join(dir_path,'format.txt'),sep='\t',index_col='item')
    # TODO check that format.txt file is correct
    atomic_reporting_unit_type = format_info.loc['atomic_reporting_unit_type','value']
    field_name_row = int(format_info.loc['field_name_row','value'])
    header_row_count = int(format_info.loc['header_row_count','value'])
        # TODO maybe file separator and encoding should be in format.txt?

    # TODO if cdf_elements.txt uses any cdf_element names as fields in any raw_identifiers formula,
    #   will need to rename some columns of the raw file before processing.
    return [cdf_elements, atomic_reporting_unit_type,header_row_count,field_name_row]


def check_jurisdiction_directory(juris_path):
    # create jurisdiction directory
    try:
        os.mkdir(juris_path)
    except FileExistsError:
        print(f'Directory {juris_path} already exists, will be preserved')
    else:
        print(f'Directory {juris_path} created')

    # create subdirectories
    subdir_list = ['context','data','output']
    for sd in subdir_list:
        sd_path = os.path.join(juris_path,sd)
        try:
            os.mkdir(sd_path)
        except FileExistsError:
            print(f'Directory {sd_path} already exists, will be preserved')
        else:
            print(f'Directory {sd_path} created')
    return


def ensure_context(juris_path,project_root=None):
    if not project_root:
        project_root = ui.get_project_root()
    # ensure directory exists
    check_jurisdiction_directory(juris_path)

    # ensure contents are correct
    templates = os.path.join(project_root,'templates/context_templates')
    enums = os.path.join(project_root,'election_anomaly/CDF_schema_def_info/enumerations')
    template_list = os.listdir(templates)
    context_file_list = ['ReportingUnit','Election','BallotMeasureContest','Office','Party','CandidateContest','Candidate','ExternalIdentifier','remark']

    # TODO check for problematic null entries?

    for element in context_file_list:
        el_path = os.path.join(juris_path,'context',f'{element}.txt')
        # remark
        if element == 'remark':

            open(el_path,'a').close()  # creates file if it doesn't exist already
            with open(el_path,'r') as f:
                remark = f.read()
            print(f'Current contents of remark.txt is:\n{remark}\n')
            input(
                f'In the file context/remark.txt, add or correct anything that user should know about the jurisdiction.\n'
                f'Then hit return to continue.')
        elif element == 'ExternalIdentifier':
            pass # TODO
        else:
            ui.fill_context_file(
                os.path.join(juris_path,'context'),templates,element)
    return


def dedupe(df,f_path,warning='There are duplicates'):
    dupes = True
    while dupes:
        dupes,df = ui.find_dupes(df)
        if dupes.empty:
            dupes = False
        else:
            input(f'WARNING: {warning}\n'
                  f'Edit the file to remove the duplication, then hit return to continue')
            df = pd.read_csv(f_path,sep='\t')
    return df


def context_dependency_dictionary():
    """Certain fields in context files refer to other context files.
    E.g., ElectionDistricts are ReportingUnits"""
    d = {'ElectionDistrict':'ReportingUnit','Office':'Office','PrimaryParty':'Party','Party':'Party',
         'Election':'Election'}
    return d

if __name__ == '__main__':
    print('Done (states_and_files)!')
