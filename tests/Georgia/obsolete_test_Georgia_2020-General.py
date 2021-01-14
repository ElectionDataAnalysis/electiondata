import election_data_analysis as e

# GA20g test
# Instructions:
#   Delete any tests for contest types your state doesn't have in 2020 (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `election_data_analysis/tests`


def test_data_exists(dbname):
    assert e.data_exists("2020 General", "Georgia", dbname=dbname)


def test_presidential(dbname):
    assert (
        e.contest_total(
            "2020 General",
            "Georgia",
            "US President (GA)",
            dbname=dbname,
        )
        == 2411889 + 2356314 + 58903
    )


def test_senate_totals(dbname):
    assert (
        e.contest_total(
            "2020 General",
            "Georgia",
            "US Senate GA",
            dbname=dbname,
        )
        == 2411043 + 2264694 + 110150
    )


def test_congressional_totals(dbname):
    assert (
        e.contest_total(
            "2020 General",
            "Georgia",
            "US House GA District 1",
            dbname=dbname,
        )
        == 181426 + 123208
    )


def test_state_senate_totals(dbname):
    assert (
        e.contest_total(
            "2020 General",
            "Georgia",
            "GA Senate District 2",
            dbname=dbname,
        )
        == 54102
    )


def test_state_house_totals(dbname):
    assert (
        e.contest_total(
            "2020 General",
            "Georgia",
            "GA House District 2",
            dbname=dbname,
        )
        == 21977
    )
