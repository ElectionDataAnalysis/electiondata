
import electiondata as e

# Instructions:
#   Change the constants to values from your file
#   Delete any tests for contest types your state doesn't have in 2020 (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `elections/tests`

# # # constants - CHANGE THESE!! - use internal db names
# # # NB: uocava votes are reported statewide, so system does *not* roll them up by county
# # # therefore the test numbers do not include uocava.
election = "2020 General"
jurisdiction = "Maine"
jurisdiction_type = "state"
abbr = "ME"
total_pres_votes = 819461  # total of all votes for President
cd = 1  # congressional district
total_cd_votes = 436027  # total votes in the chosen cd
shd = 2  # state house district  ***NOT AVAILABLE***
total_shd_votes = 6470
ssd = 1  # state senate district
total_ssd_votes = 18996
single_vote_type = "not-uocava"  # pick any one from your file ***NOT AVAILABLE***
pres_votes_vote_type = 828305 - 5771  # actually 828305 - 5771, but showing as 'other' right now
single_county = "Maine;Androscoggin County"  # pick any one from your file
pres_votes_county = 58707  # total votes for pres of that county


def test_data_exists(dbname):
    assert e.data_exists(election, jurisdiction, dbname=dbname)


def test_presidential(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            dbname=dbname,
            sub_unit_type=jurisdiction_type,
        )
        == total_pres_votes
    )


def test_congressional_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US House {abbr} District {cd}",
            sub_unit_type=jurisdiction_type,
            dbname=dbname,
        )
        == total_cd_votes
    )


"""
def test_state_senate_totals(dbname):
    assert (e.contest_total(
        election,
        jurisdiction,
        f"{abbr} Senate District {ssd}",
        dbname=dbname,
        )
        == total_ssd_votes
    )


def test_state_house_totals(dbname):
    assert (e.contest_total(
        election,
        jurisdiction,
        f"{abbr} House District {shd}",
        dbname=dbname,
        )
        == total_shd_votes
    )


def test_standard_vote_types(dbname):
    assert e.check_count_types_standard(election, jurisdiction, dbname=dbname)
"""


def test_vote_type_counts_consistent(dbname):
    assert e.check_totals_match_vote_types(election, jurisdiction, dbname=dbname)


def test_count_type_subtotal(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            dbname=dbname,
            vote_type=single_vote_type,
            sub_unit_type=jurisdiction_type,
        )
        == pres_votes_vote_type
    )


def test_county_subtotal(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            dbname=dbname,
            county=single_county,
        )
        == pres_votes_county
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
