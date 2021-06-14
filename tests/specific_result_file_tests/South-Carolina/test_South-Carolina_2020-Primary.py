import election_data_analysis as e

election = "2020 Primary"
jurisdiction = "South Carolina"


def test_data_exists(dbname):
    assert e.data_exists("2020 Primary", "South Carolina", dbname=dbname)


def test_sc_presidential_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "South Carolina",
            "US President (SC) (Democratic Party)",
            dbname=dbname,
        )
        == 539263
    )


def test_sc_statewide_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "South Carolina",
            "US Senate SC (Republican Party)",
            dbname=dbname,
        )
        == 469043
    )


def test_sc_senate_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "South Carolina",
            "SC Senate District 8 (Republican Party)",
            dbname=dbname,
        )
        == 13838
    )


def test_sc_house_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "South Carolina",
            "SC House District 75 (Democratic Party)",
            dbname=dbname,
        )
        == 3863
    )


def test_sc_contest_by_vote_type_20(dbname):
    assert (
        e.count_type_total(
            "2020 Primary",
            "South Carolina",
            "SC House District 75 (Democratic Party)",
            "absentee-mail",
            dbname=dbname,
        )
        == 1106
    )


def test_sc_totals_match_vote_type_20(dbname):
    assert (
        e.check_totals_match_vote_types("2020 Primary", "South Carolina", dbname=dbname)
        == True
    )


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )
