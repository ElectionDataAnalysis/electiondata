import election_data_analysis as e

def data_exists(dbname):
    assert e.data_exists("2020 Primary", "California", dbname=dbname)

def test_ca_presidential_20(dbname):
    assert ( e.contest_total(
            "2020 Primary",
            "California",
            "US President (CA) (Democratic Party)",
            dbname=dbname,
        )
            == 11568728
    )

def test_ca_senate_totals_20(dbname):
    assert ( e.contest_total(
            "2020 Primary",
            "California",
            "CA Senate District 13 (Libertarian Party)",
            dbname=dbname,
        )
            == 11820
    )


def test_ca_rep_20(dbname):
    assert ( e.contest_total(
            "2020 Primary",
            "California",
            "CA House District 60 (Republican Party)",
            dbname=dbname,
        )
            == 77936
    )

def test_ca_congress_20(dbname):
    assert ( e.contest_total(
            "2020 Primary",
            "California",
            "US House CA District 6 (Democratic Party)",
            dbname=dbname,
        )
            == 265322
    )
