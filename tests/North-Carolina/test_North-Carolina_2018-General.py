import election_data_analysis as e

election = "2018 General"
jurisdiction = "North Carolina"


def test_data(dbname):
    assert e.data_exists("2018 General", "North Carolina", dbname=dbname)


def test_nc_statewide_totals_18(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "North Carolina",
            "US House NC District 3",
            dbname=dbname,
        )
        == 187901
    )


def test_nc_senate_totals_18(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "North Carolina",
            "NC Senate District 15",
            dbname=dbname,
        )
        == 83175
    )


def test_nc_house_totals_18(dbname):
    assert (
        e.contest_total(
            "2018 General",
            "North Carolina",
            "NC House District 1",
            dbname=dbname,
        )
        == 27775
    )


def test_nc_contest_by_vote_type_18(dbname):
    assert (
        e.count_type_total(
            "2018 General",
            "North Carolina",
            "US House NC District 4",
            "absentee-mail",
            dbname=dbname,
        )
        == 10778
    )


def test_nc_totals_match_vote_type_18(dbname):
    assert (
        e.check_totals_match_vote_types("2018 General", "North Carolina", dbname=dbname)
        == True
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
