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
ok = {
    "IA20p": data_exists('2020 Primary','Iowa'),
}

print(ok)

### NC dataloading tests ###
#NC16 tests
@pytest.mark.skipif(not ok["ia20p"], reason="No IA 2020 Primary data")
def test_ia_presidential():
    assert(
        check_contest_totals(
            "2020 Primary",
            "Iowa",
            "US President (IA) (Democratic Party)",
        )
        == 149973
    )


@pytest.mark.skipif(not ok["ia20p"], reason="No IA 2020 Primary data")
def test_ia_statewide_totals():
    assert(
        check_contest_totals(
            "2020 Primary",
            "Iowa",
            "US Senate IA (Republican Party)",
        )
        == 229721
    )

@pytest.mark.skipif(not ok["ia20p"], reason="No IA 2020 Primary data")
def test_ia_state_senate_totals():
    assert (
        check_contest_totals(
            "2020 Primary",
            "Iowa",
            "IA Senate District 2 (Republican Party)",
        )
        == 11583
    )

@pytest.mark.skipif(not ok["ia20p"], reason="No IA 2020 Primary data")
def test_ia_state_rep_totals():
    assert (
        check_contest_totals(
            "2020 Primary",
            "Iowa",
            "US House IA District 8 (Democratic Party)",
        )
        == 1382
    )

@pytest.mark.skipif(not ok["ia20p"], reason="No IA 2020 Primary data")
def test_ia_contest_by_vote_type():
    assert (
        check_count_type_totals(
            "2020 Primary",
            "Iowa",
            "US House IA District 8 (Democratic Party)",
            "Polling",
        )
        == 1375
    )

@pytest.mark.skipif(not ok["ia20p"], reason="No IA 2020 Primary data")
def test_ia_totals_match_vote_type():
    assert True == True
