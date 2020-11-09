import election_data_analysis as e

# Instructions:
#   Add in the Jurisdiction and abbreviation
#   Delete any tests for contest types your state doesn't have in 2020 (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `election_data_analysis/tests`

jurisdiction = 'Nebraska'
abbr = 'NE'

def test_data_exists(dbname):
    assert e.data_exists("2020 General",f"{jurisdiction}",dbname=dbname)

def test_presidential(dbname):
    assert(e.contest_total(
        "2020 General",
        f"{jurisdiction}",
        f"US President ({abbr})",
        dbname=dbname,
        )
        == 922817
    )

def test_senate_totals(dbname):
    assert (e.contest_total(
        "2020 General",
        f"{jurisdiction}",
        f"US Senate {abbr}",
        dbname=dbname,
        )
        == 839222
    )

def test_congressional_totals(dbname):
    assert (e.contest_total(
        "2020 General",
        f"{jurisdiction}",
        f"US House {abbr} District 1",
        dbname=dbname,
        )
        == 310182
    )

def test_congressional2_totals(dbname):
    assert (e.contest_total(
        "2020 General",
        f"{jurisdiction}",
        f"US House {abbr} District 2",
        dbname=dbname,
        )
        == 318502
    )

def test_congressional3_totals(dbname):
    assert (e.contest_total(
        "2020 General",
        f"{jurisdiction}",
        f"US House {abbr} District 3",
        dbname=dbname,
        )
        == 284130
    )
