#!usr/bin/python3
import analyze_via_pandas as avp

if __name__ == '__main__':
    election = '2018general'
    top_ru = 'Florida'
    sub_ru_type = 'county'
    count_type = 'total'
    count_status = 'unknown'
    rollup_dir = '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/jurisdictions/FL/rollups_from_cdf_db/FROMDB_FL'


    contest_list = ['Florida;Commissioner of Agriculture','Florida;US Congress Senate','Florida;Chief Financial Officer','Florida;Governor','Florida;Attorney General','congressional']
    contests = ['Florida;Commissioner of Agriculture','Florida;US Congress Senate','Florida;Chief Financial Officer','Florida;Governor','Florida;Attorney General']
    contest_type = {x:'Candidate' for x in contests}

    dropoff = avp.dropoff_from_rollup(
        election,top_ru,sub_ru_type,count_type,count_status,rollup_dir,contests,contest_type,contest_group_types=None)

    # a = avp.by_contest_columns('2018 General Election','North Carolina','county','mixed','unknown', '/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/jurisdictions/NC/rollups_from_cdf_db/FROMDB_NC',contest_group_types=['congressional','state-house','state-senate'])
    a = avp.by_contest_columns('2018general','Florida','county','total','unknown','/Users/Steph-Airbook/Documents/CampaignScientific/NSF2019/State_Data/results_analysis/src/jurisdictions/FL/rollups_from_cdf_db/FROMDB_FL',contest_group_types=['congressional','state-house','state-senate'])




    b =  avp.diff_from_avg(a,contest_list)

    print('Done')
