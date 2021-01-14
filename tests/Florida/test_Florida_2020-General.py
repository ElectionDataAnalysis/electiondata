import election_data_analysis as e

election = "2020 General"
jurisdiction = "Florida"
# FL20g test
# Instructions:
#   Delete any tests for contest types your state doesn't have in 2020 (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `election_data_analysis/tests`


def test_data_exists(dbname):
    assert e.data_exists("2020 General", "Florida", dbname=dbname)


def test_presidential(dbname):
    assert (
        e.contest_total(
            "2020 General",
            "Florida",
            "US President (FL)",
            dbname=dbname,
        )
        == 11040997
    )


def test_congressional_totals(dbname):
    assert (
        e.contest_total(
            "2020 General",
            "Florida",
            "US House FL District 24",
            dbname=dbname,
        )
        == 289356
    )


def test_state_senate_totals(dbname):
    assert (
        e.contest_total(
            "2020 General",
            "Florida",
            "FL Senate District 3",
            dbname=dbname,
        )
        == 257487
    )


def test_state_house_totals(dbname):
    assert (
        e.contest_total(
            "2020 General",
            "Florida",
            "FL House District 92",
            dbname=dbname,
        )
        == 60913
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
