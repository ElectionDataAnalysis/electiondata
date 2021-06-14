import election_data_analysis as e

election = "2016 General"
jurisdiction = "North Carolina"


def test_data(dbname):
    assert e.data_exists("2016 General", "North Carolina", dbname=dbname)


def test_nc_presidential_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "North Carolina",
            "US President (NC)",
            dbname=dbname,
        )
        == 4741564
    )


def test_nc_statewide_totals_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "North Carolina",
            "NC Treasurer",
            dbname=dbname,
        )
        == 4502784
    )


def test_nc_senate_totals_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "North Carolina",
            "US Senate NC",
            dbname=dbname,
        )
        == 4691133
    )


def test_nc_rep_16(dbname):
    assert (
        e.contest_total(
            "2016 General",
            "North Carolina",
            "US House NC District 4",
            dbname=dbname,
        )
        == 409541
    )


def test_nc_contest_by_vote_type_16(dbname):
    assert (
        e.count_type_total(
            "2016 General",
            "North Carolina",
            "US House NC District 4",
            "absentee-mail",
            dbname=dbname,
        )
        == 20881
    )


def test_nc_totals_match_vote_type_16(dbname):
    assert (
        e.check_totals_match_vote_types("2016 General", "North Carolina", dbname=dbname)
        == True
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
