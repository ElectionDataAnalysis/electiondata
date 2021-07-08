import electiondata as e

# Instructions:
#   Copy this template and rename, including your jurisdiction and the timestamp of the results file
#       following this model: test_North-Carolina_2020_General_20201113_1645.py
#           * start with test_ so pytest will find the file
#           * get the underscores and hyphens right, so the system will find the file
#           * timestamp is YYYYMMDD_xxxx, where xxxx is the military-time Pacific Standard Time
#           * (timestamp is crucial for files collected during recounts and evolving canvass counts)
#   Change the constants to values from your file
#   "triple-quote" out any tests for contest types your state doesn't have in 2020
#   (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `elections/tests`

# # # constants - CHANGE THESE!! - use internal db names
election = "2020 Primary"
jurisdiction = "Arizona"
abbr = "AZ"
statewide_contest = f"US President ({abbr}) (Democratic Party)"
total_statewide_votes = 536509  # total of all votes for the statewide contest
cd = f"US House {abbr} District 3 (Republican Party)"  # US House congressional contest
total_cd_votes = 29260  # total votes in that US House contest in the chosen cd
hd = f"{abbr} House District 8 (Democratic Party)"  # state house contest
total_shd_votes = 13727  # total votes in that State House contest
sd = f"{abbr} Senate District 10 (Republican Party)"  # state senate contest
total_ssd_votes = 19891  # total votes in that State Senate contest
single_vote_type = "total"  # pick any one with corresponding data in your file, but use internal db name
statewide_votes_vote_type = (
    536509
)  # total votes for the statewide contest of that vote type
county_or_other = "county"  # Change this only if results are subdivided by something other than counties
#  e.g., 'parish' in LA, 'state-house' in Alaska, 'ward' in Philadelphia
single_county = "Arizona;Santa Cruz County"  # pick any one from your file
statewide_votes_county = 3848  # total votes for the statewide contest in that county


def test_data_exists(dbname):
    assert e.data_exists(election, jurisdiction, dbname=dbname)

# NB: Because one primary result file has vote types but the other does not, can't test both at once.
# see github issue #358
"""
def test_statewide(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            statewide_contest,
            sub_unit_type=county_or_other,
            dbname=dbname,
        )
        == total_statewide_votes
    )
"""

def test_congressional_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            cd,
            sub_unit_type=county_or_other,
            dbname=dbname,
        )
        == total_cd_votes
    )


def test_state_senate_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            sd,
            sub_unit_type=county_or_other,
            dbname=dbname,
        )
        == total_ssd_votes
    )


def test_state_house_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            hd,
            sub_unit_type=county_or_other,
            dbname=dbname,
        )
        == total_shd_votes
    )


def test_standard_vote_types(dbname):
    assert e.check_count_types_standard(election, jurisdiction, dbname=dbname)


# NB: Because one primary result file has vote types but the other does not, can't test both at once.
# see github issue #358


"""def test_vote_type_counts_consistent(dbname):
    assert e.check_totals_match_vote_types(election, jurisdiction, dbname=dbname)

"""
def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )

# NB: Because one primary result file has vote types but the other does not, can't test both at once.
# see github issue #358

"""def test_count_type_subtotal(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            statewide_contest,
            sub_unit_type=county_or_other,
            dbname=dbname,
            vote_type=single_vote_type,
        )
        == statewide_votes_vote_type
    )

def test_county_subtotal(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            statewide_contest,
            dbname=dbname,
            county=single_county,
            sub_unit_type=county_or_other,
        )
        == statewide_votes_county
    )
"""