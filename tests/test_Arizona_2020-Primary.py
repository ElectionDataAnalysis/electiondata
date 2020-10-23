import election_data_analysis as e
#AZ20 tests

def test_presidential(dbname):
    # TODO get this contest
    #Contest Not available
    assert True == True

# TODO get AZ munger to read write-in votes; then will have to add 451 for Bo 'Heir Archy' Garcia
def test_statewide_totals(dbname):
    assert( e.contest_total(
            "2020 Primary",
            "Arizona",
            "US Senate AZ (Democratic Party)",
        dbname=dbname,
    )
        == 665620
    )

def test_state_senate_totals(dbname):
    assert ( e.contest_total(
            "2020 Primary",
            "Arizona",
            "AZ Senate District 10 (Republican Party)",
        dbname=dbname,
    )
        == 19891
    )

def test_state_rep_totals(dbname):
    assert ( e.contest_total(
            "2020 Primary",
            "Arizona",
            "AZ House District 6 (Democratic Party)",
            dbname=dbname,
        )
        == 24035
    )


def test_congressional_totals(dbname):
    assert ( e.contest_total(
            "2020 Primary",
            "Arizona",
            "US House AZ District 6 (Democratic Party)",
            dbname=dbname,
        )
        == 3651 + 29218 + 4592 + 42538
    )

