import election_data_analysis as e

def test_data_exists(dbname):
    assert e.data_exists("2018 General","Arkansas",dbname=dbname)

def test_ar_statewide_totals_18(dbname):
    assert (e.contest_total(
            "2018 General",
            "Arkansas",
            "AR Governor",
            dbname=dbname,
        )
            == 891509
    )


def test_ar_senate_totals_18(dbname):
    assert (e.contest_total(
            "2018 General",
            "Arkansas",
            "AR Senate District 5",
            dbname=dbname,
        )
            == 27047
    )


def test_ar_house_totals_18(dbname):
    assert (e.contest_total(
            "2018 General",
            "Arkansas",
            "AR House District 19",
            dbname=dbname,
        )
            == 7927
    )


def test_ar_contest_by_vote_type_18(dbname):
    assert ( not e.data_exists("2018 General","Arkansas", dbname=dbname) or
            e.count_type_total(
            "2018 General",
            "Arkansas",
            "AR Senate District 5",
            "absentee",
            dbname=dbname,
        )
            == 453
    )


def test_ar_totals_match_vote_type_18(dbname):
    assert (not e.data_exists("2018 General","Arkansas", dbname=dbname) or
            e.check_totals_match_vote_types("2018 General","Arkansas" ,dbname=dbname) == True)

