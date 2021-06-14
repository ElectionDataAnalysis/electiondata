import election_data_analysis as e

election = "2020 Primary"
jurisdiction = "North Carolina"


def test_data(dbname):
    assert e.data_exists("2020 Primary", "North Carolina", dbname=dbname)


def test_nc_presidential_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "North Carolina",
            "US President (NC) (Democratic Party)",
            dbname=dbname,
        )
        == 1331366
    )


def test_nc_statewide_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "North Carolina",
            "NC Governor (Democratic Party)",
            dbname=dbname,
        )
        == 1293652
    )


def test_nc_senate_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "North Carolina",
            "US Senate NC (Democratic Party)",
            dbname=dbname,
        )
        == 1260090
    )


def test_nc_rep_20_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "North Carolina",
            "US House NC District 4 (Republican Party)",
            dbname=dbname,
        )
        == 36096
    )


def test_nc_contest_by_vote_type_20(dbname):
    assert (
        e.count_type_total(
            "2020 Primary",
            "North Carolina",
            "US House NC District 4 (Republican Party)",
            "absentee-mail",
            dbname=dbname,
        )
        == 426
    )


def test_nc_totals_match_vote_type_20(dbname):
    assert (
        e.check_totals_match_vote_types("2020 General", "North Carolina", dbname=dbname)
        == True
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
