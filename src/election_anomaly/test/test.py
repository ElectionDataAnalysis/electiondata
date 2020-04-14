#!usr/bin/python3
import analyze_via_pandas as avp
import os
import user_interface as ui
import pandas as pd

if __name__ == '__main__':

    idx, option = ui.pick_one(pd.DataFrame([[0,'FL'],[1,'NC']],columns=['num','state']).set_index('num'),'state',required=True)
    if option == 'FL':
        election = '2018general'
        top_ru = 'Florida'
        sub_ru_type = 'county'
        count_type = 'total'
        count_status = 'unknown'
        rollup_dir = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/jurisdictions/FL/rollups_from_cdf_db/FROMDB_FL'
        output_dir = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/jurisdictions/FL/tmp'
        contest = 'Florida;US Congress Senate'

    elif option == 'NC':
        election = '2018 General Election'
        top_ru = 'North Carolina'
        sub_ru_type = 'county'
        count_type = 'mixed'
        count_status = 'unknown'

        rollup_dir = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/jurisdictions/NC/rollups_from_cdf_db/FROMDB_NC'
        output_dir = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/jurisdictions/NC/tmp'
        contest = 'North Carolina;US Congressional Representative District 9'


    rollup = avp.rollup_df(os.path.join(rollup_dir,election,top_ru,f'by_{sub_ru_type}',f'TYPE{count_type}_STATUS{count_status}.txt'))

    cc = avp.single_contest_selection_columns(rollup,contest,count_type,output_dir)
    col_list = cc.columns
    if count_type != 'mixed':  # if single count type
        bb = avp.diff_from_avg(cc,col_list).round(2)
    else:
        count_types = cc['CountType'].unique()
        for ct in count_types:
            dd = cc[cc['CountType']==ct]  # only rows of that CountItemType
            bb = avp.diff_from_avg(dd,col_list)
            pass
            # TODO


    contest_list = ['Florida;Commissioner of Agriculture','Florida;US Congress Senate','Florida;Chief Financial Officer','Florida;Governor','Florida;Attorney General','congressional']
    contests = ['Florida;Commissioner of Agriculture','Florida;US Congress Senate','Florida;Chief Financial Officer','Florida;Governor','Florida;Attorney General']
    contest_type = {x:'Candidate' for x in contests}

    dropoff = avp.dropoff_from_rollup(
        election,top_ru,sub_ru_type,count_type,count_status,rollup_dir,output_dir,contests,contest_type,contest_group_types=None)

    a = avp.by_contest_columns(election,top_ru,sub_ru_type,count_type,count_status,rollup_dir,
                               contest_group_types=['congressional','state-house','state-senate'])

    b = avp.diff_from_avg(a,contest_list)

    print('Done')
