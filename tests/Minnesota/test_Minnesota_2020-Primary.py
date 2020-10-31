import election_data_analysis as e


def test_data_exists(dbname):
    assert e.data_exists("2020 Primary", "Minnesota", dbname=dbname)


def test_mn_presidential_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Minnesota",
            "US President (MN) (Democratic-Farmer-Labor Party)",
            dbname=dbname,
        )
        == 744198
    )


def test_mn_statewide_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Minnesota",
            "US Senate MN (Republican Party)",
            dbname=dbname,
        )
        == 244888
    )


def test_mn_senate_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Minnesota",
            "MN Senate District 22 (Democratic-Farmer-Labor Party)",
            dbname=dbname,
        )
        == 2494
    )


def test_mn_house_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Minnesota",
            "MN House District 31A (Republican Party)",
            dbname=dbname,
        )
        == 2244
    )
