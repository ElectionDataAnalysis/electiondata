import electiondata as e
from electiondata import constants

# Instructions:
#   Copy this template and rename, including your jurisdiction and the timestamp of the results file
#       following this model: test_North-Carolina_2020_General_20201113_1645.py
#           * start with test_ so pytest will find the file
#           * get the underscores and hyphens right, so the system will find the file
#           * timestamp is YYYYMMDD_xxxx, where xxxx is the military-time Pacific Standard Time
#           * (timestamp is crucial for files collected during recounts and evolving canvass counts)
#   Change the constants to values from your file
#   "triple-quote" out any tests for contest types your state doesn't have in 2020 (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `elections/tests`

# # # constants - CHANGE THESE!! - use internal db names
election = "2021 Democratic Primary"
jurisdiction = "Virginia"
top_contest = "VA Governor"
total_top_contest = 307367 + 58213 + 98052 + 13694 + 17606  # total of all votes for US President
district_contest_1 = "VA House District 66"
total_district_contest_1 = 2916 + 1842
single_vote_type = "total"  # pick any one with corresponding data in your file, but use internal db name
top_contest_vote_type =  307367 + 58213 + 98052 + 13694 + 17606 # total votes for US President of that vote type
county_or_other = "county"  # Change this only if results are subdivided by something other than counties
#  e.g., 'parish' in LA, 'state-house' in Alaska, 'ward' in Philadelphia
single_county = "Virginia;Fairfax County"  # pick any one from your file, but use internal db name
top_contest_votes_county =  53783 + 8792 + 16220 + 2712 + 2168 # total votes for US President in that county

abbr = constants.abbr[jurisdiction]

def test_data_exists(dbname):
    assert e.data_exists(election, jurisdiction, dbname=dbname)


def test_top_contest(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            top_contest,
            sub_unit_type=county_or_other,
            dbname=dbname,
        )
        == total_top_contest
    )


def test_district_1(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            district_contest_1,
            sub_unit_type=county_or_other,
            dbname=dbname,
        )
        == total_district_contest_1
    )


def test_standard_vote_types(dbname):
    assert e.check_count_types_standard(election, jurisdiction, dbname=dbname)


def test_vote_type_counts_consistent(dbname):
    assert e.check_totals_match_vote_types(election, jurisdiction, dbname=dbname)


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )


def test_count_type_subtotal(dbname):
    assert (
            e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            dbname=dbname,
            sub_unit_type=county_or_other,
            vote_type=single_vote_type,
        )
            == top_contest_vote_type
    )


def test_county_subtotal(dbname):
    assert (
            e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            dbname=dbname,
            county=single_county,
            sub_unit_type=county_or_other,
        )
            == top_contest_votes_county
    )
