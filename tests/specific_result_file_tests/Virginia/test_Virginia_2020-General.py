import electiondata as e

# Instructions:
#   Change the constants to values from your file
#   Delete any tests for contest types your state doesn't have in 2020 (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `elections/tests`

# source for totals:
# https://results.elections.virginia.gov/vaelections/2020%20November%20General/Site/Congress.html
# # # constants - CHANGE THESE!! - use internal db names
election = "2020 General"
jurisdiction = "Virginia"
abbr = "VA"
total_pres_votes = 2413568 + 1962430 + 64761 + 19765  # total of all votes for President
total_ussen_votes = 2466500 + 1934199 + 4388
cd = 3  # congressional district
total_cd_votes = 233326 + 107299 + 736  # total votes in the chosen cd
shd = 29  # state house district
total_shd_votes = 16365 + 28787 + 64
ssd = 15  # state senate district
total_ssd_votes = -1
single_vote_type = "total"  # pick any one from your file
pres_votes_vote_type = 2413568 + 1962430 + 64761 + 19765
county_or_other = "locality"
single_county = "Virginia;Bath County"  # pick any one from your file
pres_votes_county = 646 + 1834 + 16 + 5  # total votes for pres in that county


def test_data_exists(dbname):
    assert e.data_exists(election, jurisdiction, dbname=dbname)


def test_presidential(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            sub_unit_type=county_or_other,
            dbname=dbname,
        )
        == total_pres_votes
    )


def test_us_senate(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US Senate {abbr}",
            sub_unit_type=county_or_other,
            dbname=dbname,
        )
        == total_ussen_votes
    )


def test_congressional_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US House {abbr} District {cd}",
            sub_unit_type=county_or_other,
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
"""


def test_standard_vote_types(dbname):
    assert e.check_count_types_standard(election, jurisdiction, dbname=dbname)


def test_vote_type_counts_consistent(dbname):
    assert e.check_totals_match_vote_types(election, jurisdiction, dbname=dbname)


def test_count_type_subtotal(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            sub_unit_type=county_or_other,
            dbname=dbname,
            vote_type=single_vote_type,
        )
        == pres_votes_vote_type
    )


def test_county_subtotal(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            sub_unit_type=county_or_other,
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
