import election_data_analysis as e

# Instructions:
#   Change the constants to values from your file
#   Delete any tests for contest types your state doesn't have in 2020 (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `election_data_analysis/tests`

# # # constants - CHANGE THESE!! - use internal db names
election = "2020 General"
jurisdiction = "Minnesota"
abbr = "MN"
total_pres_votes = (
    1484065 + 1717077 + 5611 + 10033 + 7940 + 5651 + 1210 + 643 + 34976 + 9965
)  # total of all votes for President
cd = 7  # congressional district
total_cd_votes = 17710 + 6499 + 194066 + 144840 + 362  # total votes in the chosen cd
shd = "47A"  # state house district
total_shd_votes = 19267 + 9308 + 31
ssd = 13  # state senate district
total_ssd_votes = 32623 + 14306 + 55
single_vote_type = "early"  # pick any one from your file
pres_votes_vote_type = 0
single_county = "Minnesota;Aitkin County"  # pick any one from your file
pres_votes_county = 10025  # total votes for pres of that county


def test_data_exists(dbname):
    assert e.data_exists(election, jurisdiction, dbname=dbname)


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


def test_congressional_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US House {abbr} District {cd}",
            dbname=dbname,
        )
        == total_cd_votes
    )


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


def test_state_house_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"{abbr} House District {shd}",
            dbname=dbname,
        )
        == total_shd_votes
    )


def test_standard_vote_types(dbname):
    assert e.check_count_types_standard(election, jurisdiction, dbname=dbname)


def test_vote_type_counts_consistent(dbname):
    assert e.check_totals_match_vote_types(election, jurisdiction, dbname=dbname)


"""
def test_count_type_subtotal(dbname):
    assert (e.contest_total(
        election,
        jurisdiction,
        f"US President ({abbr})",
        dbname=dbname,
        vote_type=single_vote_type,
        )
        == pres_votes_vote_type
    )

"""


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
