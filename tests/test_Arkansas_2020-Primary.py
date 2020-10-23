import election_data_analysis as e

def test_ar_presidential_20(dbname):
    assert (not e.data_exists("2020 Primary","Arkansas",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Arkansas",
            "US President (AR) (Republican Party)",
            dbname=dbname,
        )
            == 246037
    )


def test_ar_statewide_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Arkansas",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Arkansas",
            "AR Governor",
            dbname=dbname,
        )
            == 891509
    )


def test_ar_senate_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Arkansas",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Arkansas",
            "AR Senate District 5",
            dbname=dbname,
        )
            == 27047
    )


def test_ar_house_totals_20(dbname):
    assert (not e.data_exists("2020 Primary","Arkansas",dbname=dbname) or e.contest_total(
            "2020 Primary",
            "Arkansas",
            "AR House District 19",
            dbname=dbname,
        )
            == 7927
    )


def test_ar_contest_by_vote_type_20(dbname):
    assert ( not e.data_exists("2020 Primary","Arkansas", dbname=dbname) or
            e.count_type_total(
            "2016 General",
            "North Carolina",
"AR Senate District 5",
            "absentee",
            dbname=dbname,
        )
            == 453
    )


def test_ar_totals_match_vote_type_20(dbname):
    assert (not e.data_exists("2020 Primary","Arkansas", dbname=dbname) or
            e.check_totals_match_vote_types("2020 Primary","Arkansas" ,dbname=dbname) == True)

