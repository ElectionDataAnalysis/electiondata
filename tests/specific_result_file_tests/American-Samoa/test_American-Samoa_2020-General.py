import electiondata as e

# Instructions:
#   Change the constants to values from your file
#   Delete any tests for contest types your state doesn't have in 2020 (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `elections/tests`

# # # constants - CHANGE THESE!! - use internal db names
election = "2020 General"
jurisdiction = "American Samoa"
jurisdiction_type = "territory"
abbr = "AS"
total_pres_votes = -1  # total of all votes for President
total_gov_votes = 11861  # total of all votes for Governor
cd = 1  # congressional district
total_cd_votes = 11749  # total votes in the chosen cd
shd = 1  # state house district
total_shd_votes = 1151
ssd = 14  # state senate district
total_ssd_votes = -1
single_vote_type = "total"  # pick any one from your file
con_votes_vote_type = 11749
# pick any one from your file
# Change this only if results are subdivided by something other than counties
county_or_other = "district"
single_county = "American Samoa;Eastern District"
con_votes_county = 5700  # total votes for congress of that county


def test_data_exists(dbname):
    assert e.data_exists(election, jurisdiction, dbname=dbname)


"""
def test_presidential(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            dbname=dbname,
        )
        == total_pres_votes
    )
"""


def test_governor_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"{abbr} Governor",
            dbname=dbname,
            sub_unit_type=jurisdiction_type,
        )
        == total_gov_votes
    )


def test_congressional_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US House {abbr} Delegate",
            dbname=dbname,
            sub_unit_type=jurisdiction_type,
        )
        == total_cd_votes
    )


"""
def test_state_senate_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"{abbr} Senate District {ssd}",
            dbname=dbname,
        )
        == total_ssd_votes
    )
"""


def test_state_house_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"{abbr} House District {shd}",
            sub_unit_type="territory",
            dbname=dbname,
        )
        == total_shd_votes
    )


def test_standard_vote_types(dbname):
    assert e.check_count_types_standard(election, jurisdiction, dbname=dbname)


def test_vote_type_counts_consistent(dbname):
    assert e.check_totals_match_vote_types(election, jurisdiction, dbname=dbname)


def test_count_type_subtotal(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US House {abbr} Delegate",
            dbname=dbname,
            vote_type=single_vote_type,
            sub_unit_type=jurisdiction_type,
        )
        == con_votes_vote_type
    )


def test_county_subtotal(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US House {abbr} Delegate",
            dbname=dbname,
            reporting_unit=single_county,
            sub_unit_type=county_or_other,
        )
        == con_votes_county
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
