import election_data_analysis as e

#DE20g test
# Instructions:
#   Delete any tests for contest types your state doesn't have in 2020 (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `election_data_analysis/tests`

def data_exists(dbname):
    assert e.data_exists("2020 General","Delaware",dbname=dbname)

def test_presidential(dbname):
    assert(e.contest_total(
        "2020 General",
        "Delaware",
        "US President (DE)",
        dbname=dbname,
        )
        == -1
    )

def test_senate_totals(dbname):
    assert (e.contest_total(
        "2020 General",
        "Delaware",
        "US Senate DE",
        dbname=dbname,
        )
        == -1
    )

def test_congressional_totals(dbname):
    assert (e.contest_total(
        "2020 General",
        "Delaware",
        "US House DE District 1",
        dbname=dbname,
        )
        == -1
    )

def test_state_senate_totals(dbname):
    assert (e.contest_total(
        "2020 General",
        "Delaware",
        "DE Senate District 1",
        dbname=dbname,
        )
        == -1
    )

def test_state_house_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "Delaware",
        "DE House District 1",
        dbname=dbname,
        )
        == -1
    )

