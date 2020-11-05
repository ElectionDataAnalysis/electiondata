import election_data_analysis as e

# Instructions:
#   Add in the Jurisdiction and abbreviation
#   Delete any tests for contest types your state doesn't have in 2020 (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `election_data_analysis/tests`

jurisdiction = 'New Hampshire'
abbr = 'NH'

def data_exists(dbname):
    assert e.data_exists("2020 General",f"{jurisdiction}",dbname=dbname)

def test_presidential(dbname):
    assert(e.contest_total(
        "2020 General",
        f"{jurisdiction}",
        f"US President ({abbr})",
        dbname=dbname,
        )
        == 803810
    )

def test_senate_totals(dbname):
    assert (e.contest_total(
        "2020 General",
        f"{jurisdiction}",
        f"US Senate {abbr}",
        dbname=dbname,
        )
        == 777986
    )

def test_congressional_totals(dbname):
    assert (e.contest_total(
        "2020 General",
        f"{jurisdiction}",
        f"US House {abbr} District 1",
        dbname=dbname,
        )
        == -1
    )

def test_statewide_totals(dbname):
    assert (e.contest_total(
        "2020 General",
        f"{jurisdiction}",
        f"{abbr} Governor",
        dbname=dbname,
        )
        == 798716
    )

def test_state_house_totals(dbname):
    assert (e.contest_total(
        "2020 General",
        f"{jurisdiction}",
        f"{abbr} House District 1",
        dbname=dbname,
        )
        == -1
    )
