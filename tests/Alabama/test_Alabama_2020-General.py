import election_data_analysis as e

election = "2020 General"
jurisdiction = "Alabama"
# Instructions:
#   Add in the Jurisdiction and abbreviation
#   Delete any tests for contest types your state doesn't have in 2020 (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `election_data_analysis/tests`

jurisdiction = "Alabama"
abbr = "AL"


def test_data_exists(dbname):
    assert e.data_exists("2020 General", f"{jurisdiction}", dbname=dbname)


def test_presidential(dbname):
    assert (
        e.contest_total(
            "2020 General",
            f"{jurisdiction}",
            f"US President ({abbr})",
            dbname=dbname,
        )
        == 2284601
    )


def test_senate_totals(dbname):
    assert (
        e.contest_total(
            "2020 General",
            f"{jurisdiction}",
            f"US Senate {abbr}",
            dbname=dbname,
        )
        == 2277854
    )


def test_congressional_totals(dbname):
    assert (
        e.contest_total(
            "2020 General",
            f"{jurisdiction}",
            f"US House {abbr} District 1",
            dbname=dbname,
        )
        == 314491
    )


# No state legislature contests, since state legislature elections for Alabama
# occure during midterm years, e.g., 2010, 2014, 2018, ...


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
