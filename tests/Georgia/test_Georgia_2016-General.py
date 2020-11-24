import election_data_analysis as e
election = "2016 General"
jurisdiction = "Georgia"

def test_ga_presidential_16(dbname):
    assert (
        not e.data_exists("2016 General", "Georgia", dbname=dbname)
        or e.contest_total(
            "2016 General",
            "Georgia",
            "US President (GA)",
            dbname=dbname,
        )
        == 4092373
    )


def test_ga_statewide_totals_16(dbname):
    assert (
        not e.data_exists("2016 General", "Georgia", dbname=dbname)
        or e.contest_total(
            "2016 General",
            "Georgia",
            "US Senate GA",
            dbname=dbname,
        )
        == 3897792
    )


def test_ga_senate_totals_16(dbname):
    assert (
        not e.data_exists("2016 General", "Georgia", dbname=dbname)
        or e.contest_total(
            "2016 General",
            "Georgia",
            "GA Senate District 13",
            dbname=dbname,
        )
        == 60387
    )


def test_ga_house_totals_16(dbname):
    assert (
        not e.data_exists("2016 General", "Georgia", dbname=dbname)
        or e.contest_total(
            "2016 General",
            "Georgia",
            "GA House District 7",
            dbname=dbname,
        )
        == 21666
    )


def test_ga_contest_by_vote_type_16(dbname):
    assert (
        not e.data_exists("2016 General", "Georgia", dbname=dbname)
        or e.count_type_total(
            "2016 General",
            "Georgia",
            "GA House District 7",
            "absentee-mail",
            dbname=dbname,
        )
        == 1244
    )


def test_ga_totals_match_vote_type_16(dbname):
    assert (
        not e.data_exists("2016 General", "Georgia", dbname=dbname)
        or e.check_totals_match_vote_types("2016 General", "Georgia", dbname=dbname)
        == True
    )



def test_all_candidates_known(dbname):
    assert e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname) == []
