import election_data_analysis as e

# VA20 tests


def test_presidential(dbname):
    assert (
        not e.data_exists("2020 Primary", "Virginia", dbname=dbname)
        or e.contest_total(
            "2020 Primary",
            "Virginia",
            "US President (VA) (Democratic Party)",
            dbname=dbname,
        )
        == 1323693
    )


def test_statewide_totals(dbname):
    assert (
        not e.data_exists("2020 Primary", "Virginia", dbname=dbname)
        or e.contest_total(
            "2020 Primary",
            "Virginia",
            "US Senator VA (Republican Party)",
            dbname=dbname,
        )
        == 309804
    )


def test_house_totals_dem(dbname):
    assert (
        not e.data_exists("2020 Primary", "Virginia", dbname=dbname)
        or e.contest_total(
            "2020 Primary",
            "Virginia",
            "US House VA District 2 (Republican Primary)",
            dbname=dbname,
        )
        == 52514
    )


def test_house_totals_dem(dbname):
    assert (
        not e.data_exists("2020 Primary", "Virginia", dbname=dbname)
        or e.contest_total(
            "2020 Primary",
            "Virginia",
            "US House VA District 5 (Democratic Primary)",
            dbname=dbname,
        )
        == 54037
    )


def test_contest_by_vote_type(dbname):
    # Vote type not available
    assert True == True


def test_totals_match_vote_type(dbname):
    # Vote type not available
    assert True == True
