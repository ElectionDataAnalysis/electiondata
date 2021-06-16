import election_data_analysis as e

# Instructions:
#   Fill in the constants with values from your file
#   Delete any tests for contest types your state doesn't have in 2020 (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `election_data_analysis/tests`

## constants - use internal db names
election = "2020 General"
jurisdiction = "Pennsylvania"
abbr = "PA"
single_vote_type = "absentee"  # pick any one from your file
single_county = "Pennsylvania;Philadelphia County"  # pick any one from your file


def data_exists(dbname):
    assert e.data_exists(election, jurisdiction, dbname=dbname)


def test_presidential(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            dbname=dbname,
        )
        == 6422156
    )


def test_statewide_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"{abbr} Auditor General",
            dbname=dbname,
        )
        == 6274473
    )


def test_congressional_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US House {abbr} District 16",
            dbname=dbname,
        )
        == 315704
    )


def test_state_senate_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"{abbr} Senate District 35",
            dbname=dbname,
        )
        == 122414
    )


def test_state_house_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"{abbr} House District 116",
            dbname=dbname,
        )
        == 25615
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
        e.count_type_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            single_vote_type,
            dbname=dbname,
        )
        == 2253712
    )


def test_one_county_vote_type(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            dbname=dbname,
            county=single_county,
            vote_type=single_vote_type,
        )
        == 263568
    )
