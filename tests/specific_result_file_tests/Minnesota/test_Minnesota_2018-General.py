import election_data_analysis as e

election = "2018 General"
jurisdiction = "Minnesota"


def test_data_exists(dbname):
    assert e.data_exists("2018 General", "Minnesota", dbname=dbname)


def test_mn_statewide_totals_18(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Minnesota",
            "MN Governor",
            dbname=dbname,
        )
        == 2587287
    )


def test_mn_senate_totals_18(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Minnesota",
            "MN Senate District 13",
            dbname=dbname,
        )
        == 37842
    )


def test_mn_house_totals_18(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "Minnesota",
            "MN House District 13A",
            dbname=dbname,
        )
        == 18601
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
