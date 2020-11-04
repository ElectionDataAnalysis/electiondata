import election_data_analysis as e

def test_data_exists(dbname):
    assert e.data_exists("2020 Primary", "Georgia", dbname=dbname)

def test_ga_presidential_20(dbname):
    assert (e.contest_total(
            "2020 Primary",
            "Georgia",
            "US President (GA) (Republican Party)",
            dbname=dbname,
        )
        == 947352
    )


def test_ga_statewide_totals_20(dbname):
    assert (e.contest_total(
            "2020 Primary",
            "Georgia",
            "US Senate GA (Republican Party)",
            dbname=dbname,
        )
        == 992555
    )


def test_ga_senate_totals_20(dbname):
    assert (e.contest_total(
            "2020 Primary",
            "Georgia",
            "GA Senate District 8 (Democratic Party)",
            dbname=dbname,
        )
        == 9103
    )


def test_ga_house_totals_20(dbname):
    assert (e.contest_total(
            "2020 Primary",
            "Georgia",
            "GA House District 7 (Democratic Party)",
            dbname=dbname,
        )
        == 2193
    )


def test_contest_by_vote_type(dbname):
    assert (e.count_type_total(
            "2020 Primary",
            "Georgia",
            "GA House District 7 (Democratic Party)",
            "absentee-mail",
            dbname=dbname,
        )
        == 1655
    )


def test_ga_totals_match_vote_type_20(dbname):
    assert (e.check_totals_match_vote_types("2020 Primary", "Georgia", dbname=dbname)
        == True
    )
