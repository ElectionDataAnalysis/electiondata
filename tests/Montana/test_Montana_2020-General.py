import election_data_analysis as e

# Instructions:
#   Change the constants to values from your file
#   Delete any tests for contest types your state doesn't have in 2020 (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `election_data_analysis/tests`

## constants - CHANGE THESE!! - use internal db names
election = "2020 General"
jurisdiction = "Montana"
abbr = "MT"
single_vote_type = (
    "total"  # pick any one from your file. only 'total' avaialable for MT
)
single_county = "Montana;Deer Lodge County"  # pick any one from your file
county_type = (
    "county"  # unless major subdivision is something else, e.g. 'parish' for Louisiana
)


def test_data_exists(dbname):
    assert e.data_exists(election, jurisdiction, dbname=dbname)


def test_presidential(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            dbname=dbname,
            sub_unit_type=county_type,
        )
        == 15147 + 341740 + 243753
    )


def test_statewide_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US Senate {abbr}",
            dbname=dbname,
            sub_unit_type=county_type,
        )
        == 271226 + 331359
    )


def test_congressional_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US House {abbr} District 1",
            dbname=dbname,
            sub_unit_type=county_type,
        )
        == 337327 + 261183
    )


def test_state_senate_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"{abbr} Senate District 2",
            dbname=dbname,
            sub_unit_type=county_type,
        )
        == 8740 + 4057
    )


def test_state_house_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"{abbr} House District 13",
            dbname=dbname,
            sub_unit_type=county_type,
        )
        == 4796 + 1452 + 497
    )


def test_standard_vote_types(dbname):
    assert e.check_count_types_standard(election, jurisdiction, dbname=dbname)


def test_vote_type_counts_consistent(dbname):
    assert e.check_totals_match_vote_types(election, jurisdiction, dbname=dbname)


def test_count_type_subtotal(dbname):
    assert (
        e.count_type_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            single_vote_type,
            dbname=dbname,
        )
        == 15147 + 341740 + 243753
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
        == 142 + 2184 + 2562
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
