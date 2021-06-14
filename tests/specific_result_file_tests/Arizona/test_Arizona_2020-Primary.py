import election_data_analysis as e

# Instructions:
#   Change the constants to values from your file
#   Delete any tests for contest types your state doesn't have in 2020 (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `election_data_analysis/tests`

# # # constants - CHANGE THESE!! - use internal db names
election = "2020 Primary"
jurisdiction = "Arizona"
abbr = "AZ"

party1 = "Democratic Party"
party2 = "Republican Party"
pres_total_party1 = 536509  # total of all votes for President in that party
cd = 3  # congressional district
total_cd_votes_party2 = (
    29260  # total votes for Congressional rep in the chosen cd and party
)
# NB: file shows 44 votes total for a write-in candidate, but all vote-type counts are 0,
#  so our program does not find them.
shd = 6  # state house district
total_shd_votes_party1 = 24035
ssd = 10  # state senate district
total_ssd_votes_party2 = 19891
single_vote_type = "total"  # pick any one from your file
pres_votes_vote_type_party2 = -1
single_county = "Arizona;Santa Cruz County"  # pick any one from your file
county_or_other = "county"  # Change this only if results are subdivided by something other than counties
#  e.g., 'parish' in LA, 'state-house' in Alaska, 'ward' in Philadelphia
pres_votes_county_party1 = (
    3848  # total votes for US President in that county for party1
)


def test_data_exists(dbname):
    assert e.data_exists(election, jurisdiction, dbname=dbname)


def test_vote_type_counts_consistent(dbname):
    assert e.check_totals_match_vote_types(election, jurisdiction, dbname=dbname)


def test_standard_vote_types(dbname):
    assert e.check_count_types_standard(election, jurisdiction, dbname=dbname)


"""
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

"""


def test_county_subtotal(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr}) ({party1})",
            dbname=dbname,
            county=single_county,
            sub_unit_type=county_or_other,
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


def test_us_house_totals(dbname):
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
