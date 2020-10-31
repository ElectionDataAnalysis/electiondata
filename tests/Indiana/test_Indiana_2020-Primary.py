import election_data_analysis as e


def test_in_presidential_20(dbname):
    assert (
        not e.data_exists("2020 Primary", "Indiana", dbname=dbname)
        or e.contest_total(
            "2020 Primary",
            "Indiana",
            "US President (IN)",
            dbname=dbname,
        )
        == 1047173
    )


def test_in_statewide_totals_20(dbname):
    assert (
        not e.data_exists("2020 Primary", "Indiana", dbname=dbname)
        or e.contest_total(
            "2020 Primary",
            "Indiana",
            "IN Governor",
            dbname=dbname,
        )
        == 932726
    )


def test_in_senate_totals_20(dbname):
    assert (
        not e.data_exists("2020 Primary", "Indiana", dbname=dbname)
        or e.contest_total(
            "2020 Primary",
            "Indiana",
            "IN Senate District 50",
            dbname=dbname,
        )
        == 6860
    )


def test_in_house_totals_20(dbname):
    assert (
        not e.data_exists("2020 Primary", "Indiana", dbname=dbname)
        or e.contest_total(
            "2020 Primary",
            "Indiana",
            "IN House District 3",
            dbname=dbname,
        )
        == 7975
    )


def test_in_contest_by_vote_type_20(dbname):
    # Vote type not available
    assert True == True
