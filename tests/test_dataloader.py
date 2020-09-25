from election_data_analysis import database as db
from election_data_analysis import user_interface as ui
from election_data_analysis import Analyzer
import pandas as pd
import os
from psycopg2 import sql
import pytest

def get_analyzer(p_path: str = None):
    one_up = os.path.dirname(os.getcwd())
    if p_path:
        param_file = p_path
    else:
        param_file = os.path.join(one_up, "src", "run_time.ini")
    a = Analyzer(param_file)
    return a


def aggregate_results(election, jurisdiction, contest_type, by_vote_type):
    # using the analyzer gives us access to DB session
    empty_df_with_good_cols = pd.DataFrame(columns=['contest','count'])
    a = get_analyzer()
    election_id = db.name_to_id(a.session, "Election", election)
    if not election_id:
        return empty_df_with_good_cols
    connection = a.session.bind.raw_connection()
    cursor = connection.cursor()

    datafile_list, e = db.data_file_list(cursor, election_id, by="Id")
    if e:
        print(e)
        return empty_df_with_good_cols
    if len(datafile_list) == 0:
        print(f"No datafiles found for Election_Id {election_id}")
        return empty_df_with_good_cols

    df, err_str = db.export_rollup_from_db(
        cursor,
        jurisdiction,
        "county",
        contest_type,
        datafile_list,
        by="Id",
        exclude_total=True,
        by_vote_type=True,
    )
    if df.empty:
        # TODO better logic? This is like throwing spaghetti at the wall
        # try without excluding total
        df, err_str = db.export_rollup_from_db(
            cursor,
            jurisdiction,
            "county",
            contest_type,
            datafile_list,
            by="Id",
            exclude_total=False,
            by_vote_type=True,
        )
    if err_str:
        return empty_df_with_good_cols
    return df


def data_exists(election, jurisdiction, p_path=None):
    a = get_analyzer(p_path=p_path)
    election_id = db.name_to_id(a.session, "Election", election)
    jurisdiction_id = db.name_to_id(a.session, "ReportingUnit", jurisdiction)
    con = a.session.bind.raw_connection()
    cur = con.cursor()
    q = sql.SQL('SELECT "Id" FROM _datafile WHERE "Election_Id" = %s AND "ReportingUnit_Id" = %s')
    cur.execute(q,(election_id,jurisdiction_id))

    answer = cur.fetchall()
    if len(answer) > 0:
        return True
    else:
        return False


def check_totals_match_vote_types(election, jurisdiction):
    df_candidate = aggregate_results(election, jurisdiction, "Candidate", False)
    df_ballot = aggregate_results(election, jurisdiction, "BallotMeasure", False)
    df_by_ttl = pd.concat([df_candidate, df_ballot])

    df_candidate = aggregate_results(election, jurisdiction, "Candidate", True)
    df_ballot = aggregate_results(election, jurisdiction, "BallotMeasure", True)
    df_by_type = pd.concat([df_candidate, df_ballot])
    return df_by_ttl["count"].sum() == df_by_type["count"].sum()


# A couple random contests
def check_contest_totals(election, jurisdiction, contest):
    df_candidate = aggregate_results(election, jurisdiction, "Candidate", False)
    df_ballot = aggregate_results(election, jurisdiction, "BallotMeasure", False)
    df = pd.concat([df_candidate, df_ballot])
    df = df[df["contest"] == contest]
    return df["count"].sum()


def check_count_type_totals(election, jurisdiction, contest, count_item_type):
    df_candidate = aggregate_results(election, jurisdiction, "Candidate", False)
    df_ballot = aggregate_results(election, jurisdiction, "BallotMeasure", False)
    df = pd.concat([df_candidate, df_ballot])
    df = df[df["contest"] == contest]
    df = df[df["count_item_type"] == count_item_type]
    return df["count"].sum()


# #### Tests start below #### #
# For each state, run at least 6 tests:
# 1. Presidential
# 2. One statewide chosen at random
# 3. One senate
# 4. One rep
# 5. If vote type is available, slice one of the above by vote type
# 6. If vote type is available, check that totals match vote type sums


### North Carolina Data Loading Tests ###
#constants
ok = {"nc": data_exists('2018 General','North Carolina')}

print(ok)

@pytest.mark.skipif(not ok["nc"], reason="No NC data")
def test_nc_presidential():
    # No presidential contests in 2018
    assert True == True


@pytest.mark.skipif(not ok["nc"], reason="No NC data")
def test_nc_statewide_totals():
    assert (
        check_contest_totals(
            "2018 General",
            "North Carolina",
            "US House NC District 3",
        )
        == 187901
    )


# @pytest.mark.skipif(not ok["nc"],"No NC data")
def test_nc_senate_totals():
    assert (
        check_contest_totals(
            "2018 General",
            "North Carolina",
            "NC Senate District 15",
        )
        == 83175
    )


@pytest.mark.skipif(not ok["nc"], reason="No NC data")
def test_nc_house_totals():
    assert (
        check_contest_totals(
            "2018 General",
            "North Carolina",
            "NC House District 1",
        )
        == 27775
    )


@pytest.mark.skipif(not ok["nc"], reason="No NC data")
def test_nc_contest_by_vote_type():
    assert (
        check_count_type_totals(
            "2018 General",
            "North Carolina",
            "US House NC District 4",
            "absentee-mail",
        )
        == 10778
    )


@pytest.mark.skipif(not ok["nc"], reason="No NC data")
def test_nc_totals_match_vote_type():
    assert check_totals_match_vote_types("2018 General", "North Carolina") == True

### Florida Data Loading Tests ###
def test_fl_presidential():
    assert (
        check_contest_totals(
            "2016 General",
            "Florida",
            "US President (FL)",
        )
        == 9420039
    )


def test_fl_statewide_totals():
    assert (
        check_contest_totals(
            "2016 General",
            "Florida",
            "US Senate FL",
        )
        == 9301820
    )


def test_fl_senate_totals():
    assert (
        check_contest_totals(
            "2016 General",
            "Florida",
            "FL Senate District 3",
        )
        == 236480
    )


def test_fl_house_totals():
    assert (
        check_contest_totals(
            "2016 General",
            "Florida",
            "US House FL District 10",
        )
        == 305989
    )


def test_fl_contest_by_vote_type():
    # Vote type not available
    assert True == True


def test_fl_totals_match_vote_type():
    # Vote type not available
    assert True == True



### Pennsylvania Data Loading Tests ###
def test_pa_presidential():
    assert (
        check_contest_totals(
            "2016 General",
            "Pennsylvania",
            "US President (PA)",
        )
        == 6115402
    )


def test_pa_statewide_totals():
    assert (
        check_contest_totals(
            "2016 General",
            "Pennsylvania",
            "PA Auditor General",
        )
        == 5916931
    )


def test_pa_senate_totals():
    assert (
        check_contest_totals(
            "2016 General",
            "Pennsylvania",
            "PA Senate District 41",
        )
        == 112283
    )


def test_pa_house_totals():
    assert (
        check_contest_totals(
            "2016 General",
            "Pennsylvania",
            "PA House District 21",
        )
        == 26453
    )


def test_pa_contest_by_vote_type():
    # Vote type not available
    assert True == True


def test_pa_totals_match_vote_type():
    # Vote type not available
    assert True == True



### Georgia Data Loading Tests ###
def test_ga_presidential():
    #no presidential contests in 2018
    assert True == True


def test_ga_statewide_totals():
    assert (
        check_contest_totals(
            "2018 General",
            "Georgia",
            "GA Governor",
        )
        == 3939328
    )


def test_ga_senate_totals():
    assert (
        check_contest_totals(
            "2018 General",
            "Georgia",
            "GA Senate District 5",
        )
        == 34429
    )


def test_ga_house_totals():
    assert (
        check_contest_totals(
            "2018 General",
            "Georgia",
            "US House GA District 2",
        )
        == 229171
    )


def test_ga_contest_by_vote_type():
    assert (
        check_count_type_totals(
            "2018 General",
            "Georgia",
            "GA Senate District 5",
            "absentee-mail",
        )
        == 2335
    )


def test_ga_totals_match_vote_type():
    assert check_totals_match_vote_types("2018 General", "Georgia") == True



### South Carolina Data Loading Tests ###
def test_sc_presidential():
    #only 2020 democratic presidental primary results loaded
    assert True == True


def test_sc_statewide_totals():
    #only 2020 democratic presidental primary results loaded
    assert True == True

def test_sc_senate_totals():
    #only 2020 democratic presidental primary results loaded
    assert True == True

def test_sc_house_totals():
    #only 2020 democratic presidental primary results loaded
    assert True == True


def test_sc_contest_by_vote_type():
    #only 2020 democratic presidental primary results loaded
    assert True == True


def test_sc_totals_match_vote_type():
    #only 2020 democratic presidental primary results loaded
    assert True == True


### Indiana Data Loading Tests ###
def test_in_presidential():
    assert (
        check_contest_totals(
            "2016 General",
            "Indiana",
            "US President (IN)",
        )
        == 2728138
    )


def test_in_statewide_totals():
    assert (
        check_contest_totals(
            "2016 General",
            "Indiana",
            "IN Attorney General",
        )
        == 2635832
    )


def test_in_senate_totals():
    assert (
        check_contest_totals(
            "2016 General",
            "Indiana",
            "IN Senate District 7",
        )
        == 50622
    )


def test_in_house_totals():
    assert (
        check_contest_totals(
            "2016 General",
            "Indiana",
            "IN House District 13",
        )
        == 26712
    )


def test_in_contest_by_vote_type():
    # Vote type not available
    assert True == True


def test_in_totals_match_vote_type():
    # Vote type not available
    assert True == True


### Arkansas Data Loading Tests ###
def test_ar_presidential():
    #no presidential contests in 2018
    assert True == True


def test_ar_statewide_totals():
    assert (
        check_contest_totals(
            "2018 General",
            "Arkansas",
            "AR Governor",
        )
        == 891509
    )


def test_ar_senate_totals():
    assert (
        check_contest_totals(
            "2018 General",
            "Arkansas",
            "AR Senate District 5",
        )
        == 27047
    )


def test_ar_house_totals():
    assert (
        check_contest_totals(
            "2018 General",
            "Arkansas",
            "AR House District 19",
        )
        == 7927
    )


def test_ar_contest_by_vote_type():
    assert (
        check_count_type_totals(
            "2018 General",
            "Arkansas",
            "AR Senate District 5",
            "absentee",
        )
        == 453
    )


def test_ar_totals_match_vote_type():
    assert check_totals_match_vote_types("2018 General", "Arkansas") == True


### Michigan Data Loading Tests ###
def test_ar_presidential():
    #no presidential contests in 2018
    assert True == True


def test_mi_statewide_totals():
    assert (
        check_contest_totals(
            "2018 General",
            "Michigan",
            "MI Governor",
        )
        == 4250585
    )


def test_mi_senate_totals():
    assert (
        check_contest_totals(
            "2018 General",
            "Michigan",
            "MI Senate District 37",
        )
        == 124414
    )

def test_mi_house_totals():
    assert (
        check_contest_totals(
            "2018 General",
            "Michigan",
            "MI House District 8",
        )
        == 28017
    )


def test_mi_contest_by_vote_type():
    # Vote type not available
    assert True == True


def test_mi_totals_match_vote_type():
    # Vote type not available
    assert True == True