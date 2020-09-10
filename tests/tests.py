from election_data_analysis import database as db
from election_data_analysis import Analyzer
import pandas as pd
import os


def aggregate_results(election, jurisdiction, contest_type, by_vote_type):
    # using the analyzer gives us access to DB session
    one_up = os.path.dirname(os.getcwd())
    param_file = os.path.join(one_up, "src", "run_time.ini")
    a = Analyzer(param_file)
    election_id = db.name_to_id(a.session, "Election", election)
    jurisdiction_id = db.name_to_id(a.session, "ReportingUnit", election)

    connection = a.session.bind.raw_connection()
    cursor = connection.cursor()

    datafile_list, e = db.data_file_list(cursor, election_id, by="Id")
    if e:
        return e
    by = "Id"
    if len(datafile_list) == 0:
        return f"No datafiles found for Election_Id {election_id}"

    df, err = db.export_rollup_from_db(
        cursor,
        jurisdiction,
        "county",
        contest_type,
        datafile_list,
        by="Id",
        exclude_total=True,
        by_vote_type=True,
    )
    return df


def check_totals(election, jurisdiction):
    df_candidate = aggregate_results(election, jurisdiction, "Candidate", False)
    df_ballot = aggregate_results(election, jurisdiction, "BallotMeasure", False)
    df = pd.concat([df_candidate, df_ballot])
    return df["count"].sum()


def check_totals_match_vote_types(election, jurisdiction):
    df_candidate = aggregate_results(election, jurisdiction, "Candidate", False)
    df_ballot = aggregate_results(election, jurisdiction, "BallotMeasure", False)
    df_by_ttl = pd.concat([df_candidate, df_ballot])

    df_candidate = aggregate_results(election, jurisdiction, "Candidate", True)
    df_ballot = aggregate_results(election, jurisdiction, "BallotMeasure", True)
    df_by_type = pd.concat([df_candidate, df_ballot])
    return df_by_ttl["count"].sum() == df_by_type["count"].sum()


def check_contest_totals(election, jurisdiction, contest):
    df_candidate = aggregate_results(election, jurisdiction, "Candidate", False)
    df_ballot = aggregate_results(election, jurisdiction, "BallotMeasure", False)
    df = pd.concat([df_candidate, df_ballot])
    df = df[df["contest"] == contest]
    return df["count"].sum()


def check_count_type_totals(election, jurisdiction, count_item_type):
    df_candidate = aggregate_results(election, jurisdiction, "Candidate", False)
    df_ballot = aggregate_results(election, jurisdiction, "BallotMeasure", False)
    df = pd.concat([df_candidate, df_ballot])
    df = df[df["count_item_type"] == count_item_type]
    return df["count"].sum()


""" North Carolina Data Loading Tests """


def test_nc_totals():
    assert check_totals("2018 General", "North Carolina") == 14756973


def test_nc_totals_match_vote_type():
    assert check_totals_match_vote_types("2018 General", "North Carolina") == True


def test_nc_contest_totals():
    assert (
        check_contest_totals(
            "2018 General",
            "North Carolina",
            "North Carolina;General Assembly House of Representatives District 1",
        )
        == 27775
    )


def test_nc_count_type_totals():
    assert (
        check_count_type_totals("2018 General", "North Carolina", "absentee-mail")
        == 380688
    )


def test_nc_totals_fail():
    assert check_totals("2018 General", "North Carolina") == 1
