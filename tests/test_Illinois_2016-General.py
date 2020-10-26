import election_data_analysis as e


def test_presidential(dbname):
    assert (e.contest_total(
            "2016 General",
            "Illinois",
            "US President (IL)",
            dbname=dbname,
        )
            == 5536424
    )


def test_statewide_totals(dbname):
    assert (e.contest_total(
            "2016 General",
            "Illinois",
            "IL Comptroller",
            dbname=dbname,
        )
            == 5412543
    )


def test_senate_totals(dbname):
    assert (e.contest_total(
            "2016 General",
            "Illinois",
            "IL Senate District 14",
            dbname=dbname,
        )
            == 79949
    )


def test_house_totals(dbname):
    assert (e.contest_total(
            "2016 General",
            "Illinois",
            "IL House District 13",
            dbname=dbname,
        )
            == 40831
    )
