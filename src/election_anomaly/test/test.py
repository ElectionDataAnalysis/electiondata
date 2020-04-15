#!usr/bin/python3
import analyze_via_pandas as avp
import os
import user_interface as ui
import pandas as pd

if __name__ == '__main__':

    idx, option = ui.pick_one(pd.DataFrame([[0,'FL'],[1,'NC']],columns=['num','state']).set_index('num'),'state',item='state',required=True)
    if option == 'FL':
        election = '2018general'
        top_ru = 'Florida'
        sub_ru_type = 'county'
        count_type = 'total'
        count_status = 'unknown'
        rollup_dir = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/jurisdictions/FL/rollups_from_cdf_db/FROMDB_FL'
        output_dir = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/jurisdictions/FL/tmp'
        # contest = 'Florida;US Congress Senate'
        # contest = 'Florida;Attorney General'
        contest = 'Florida;Governor'

        comparison_contests = ['Florida;Commissioner of Agriculture','Florida;US Congress Senate',
                               'Florida;Chief Financial Officer','Florida;Governor','Florida;Attorney General']
        contest_group_types = None

    elif option == 'NC':
        election = '2018 General Election'
        top_ru = 'North Carolina'
        sub_ru_type = 'county'
        count_type = 'mixed'
        count_status = 'unknown'

        rollup_dir = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/jurisdictions/NC/rollups_from_cdf_db/FROMDB_NC'
        output_dir = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/jurisdictions/NC/tmp'
        contest = 'North Carolina;US Congress House of Representatives District 9'
        comparison_contests = ['state-house','congressional']
        contest_group_types = ['congressional','state-house']

    rollup = avp.rollup_df(os.path.join(rollup_dir,election,top_ru,f'by_{sub_ru_type}',f'TYPE{count_type}_STATUS{count_status}.txt'))

    contest_type = {x:'Candidate' for x in comparison_contests} # TODO include BM contests too

    single = avp.process_single_contest(rollup,contest,os.path.join(output_dir,election,top_ru,f'by_{sub_ru_type}'))

    dropoff = avp.dropoff_from_rollup(
        election,top_ru,sub_ru_type,count_type,count_status,rollup_dir,output_dir,contest,comparison_contests,contest_type,contest_group_types=contest_group_types)


    print('Done')
