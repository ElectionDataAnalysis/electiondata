import election_data_analysis as e

# Instructions:
#   Change the constants to values from your file
#   Delete any tests for contest types your state doesn't have in 2020 (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `election_data_analysis/tests`

# # # constants - CHANGE THESE!! - use internal db names
election = "2020 Primary"
jurisdiction = "Texas"
abbr = "TX"

party1 = "Democratic Party"
party2 = "Republican Party"
pres_total_party1 = 2094428  # total of all votes for President
cd = 3  # congressional district
total_cd_votes_party2 = 53938  # total votes in the chosen cd
shd = 101  # state house district
total_shd_votes_party1 = 10924
ssd = 13  # state senate district
total_ssd_votes_party2 = 8247
single_vote_type = "total"  # pick any one from your file
pres_votes_vote_type_party2 = 2017167
single_county = "Texas;Montgomery County"  # pick any one from your file
pres_votes_county_party1 = 25612  # total votes for pres of that county


def test_data_exists(dbname):
    assert e.data_exists(election, jurisdiction, dbname=dbname)


def test_vote_type_counts_consistent(dbname):
    assert e.check_totals_match_vote_types(election, jurisdiction, dbname=dbname)


def test_standard_vote_types(dbname):
    assert e.check_count_types_standard(election, jurisdiction, dbname=dbname)


def test_presidential_vote_type(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr}) ({party2})",
            vote_type=single_vote_type,
            dbname=dbname,
        )
        == pres_votes_vote_type_party2
    )


def test_county_subtotal(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr}) ({party1})",
            dbname=dbname,
            county=single_county,
        )
        == pres_votes_county_party1
    )


def test_presidential(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr}) ({party1})",
            dbname=dbname,
        )
        == pres_total_party1
    )


def test_state_house_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US House {abbr} District {cd} ({party2})",
            dbname=dbname,
        )
        == total_cd_votes_party2
    )


def test_state_senate_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"{abbr} Senate District {ssd} ({party2})",
            dbname=dbname,
        )
        == total_ssd_votes_party2
    )


def test_state_rep_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"{abbr} House District {shd} ({party1})",
            dbname=dbname,
        )
        == total_shd_votes_party1
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
