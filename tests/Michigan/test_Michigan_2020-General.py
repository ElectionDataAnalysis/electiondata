import election_data_analysis as e

# Instructions:
#   Add in the Jurisdiction and abbreviation
#   Delete any tests for contest types your state doesn't have in 2020 (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `election_data_analysis/tests`

jurisdiction = 'Michigan'
abbr = 'MI'

def test_data_exists(dbname):
    assert e.data_exists("2020 General",f"{jurisdiction}",dbname=dbname)

def test_presidential(dbname):
    assert(e.contest_total(
        "2020 General",
        f"{jurisdiction}",
        f"US President ({abbr})",
        dbname=dbname,
        )
        == 3842256 #not 100% sure that i was using pivot tables right to get this number. I think this is the total number of votes cast for president 
    )

def test_senate_totals(dbname):
    assert (e.contest_total(
        "2020 General",
        f"{jurisdiction}",
        f"US Senate {abbr}",
        dbname=dbname,
        )
        == 3799664 #similarly, this should total votes for senators
    )

def test_congressional_totals(dbname):
    assert (e.contest_total(
        "2020 General",
        f"{jurisdiction}",
        f"US House {abbr} District 1",
        dbname=dbname,
        )
        == 398809
    )
"""
#not sure what state senate is called in michigan, or even if they are up for election right now. state house might be things like: District Representative in State Legislature, in which case the second test is right 
def test_state_senate_totals(dbname):
    assert (e.contest_total(
        "2020 General",
        f"{jurisdiction}",
        f"{abbr} Senate District 1",
        dbname=dbname,
        )
        == -1
    )

def test_state_house_totals(dbname):
    assert (e.contest_total(
        "2020 General",
        f"{jurisdiction}",
        f"{abbr} House District 101",
        dbname=dbname,
        )
        == 4754 + 7348+7730+8928+5283+9231+5977+11068
    )
"""