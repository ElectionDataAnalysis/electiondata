import election_data_analysis as e
#IA20 tests

# Iowa has Presidential caucuses, not primaries

def test_statewide_totals(dbname):
    assert(not e.data_exists("2020 Primary","Iowa",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Iowa",
            "US Senate IA (Republican Party)",
        dbname=dbname,
    )
        == 229721
    )

def test_state_senate_totals(dbname):
    assert (not e.data_exists("2020 Primary","Iowa",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Iowa",
            "IA Senate District 2 (Republican Party)",
        dbname=dbname,
    )
        == 11583
    )

def test_state_rep_totals(dbname):
    assert (not e.data_exists("2020 Primary","Iowa",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Iowa",
            "IA House District 8 (Democratic Party)",
            dbname=dbname,
    )
        == 1382
    )

# no vote totals