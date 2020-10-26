import election_data_analysis as e

def test_data_exists(dbname):
    assert e.data_exists("2020 Primary","Illinois",dbname=dbname)

def test_presidential_totals(dbname):
    assert (e.contest_total(
            "2020 Primary",
            "Illinois",
            "US President (IL) (Democratic Party)",
            dbname=dbname,
        )
            == 2216933
    )

def test_statewide_totals(dbname):
    assert (e.contest_total(
            "2020 Primary",
            "Illinois",
            "IL Senate IL (Democratic Party)",
            dbname=dbname,
        )
            == 1941286
    )


def test_senate_totals(dbname):
    assert (e.contest_total(
            "2020 Primary",
            "Illinois",
            "IL Senate District 11 (Democratic Party)",
            dbname=dbname,
        )
            == 22716
    )


def test_house_totals(dbname):
    assert (e.contest_total(
            "2020 Primary",
            "Illinois",
            "IL House District 60 (Democratic Party)",
            dbname=dbname,
        )
            == 8888
    )
