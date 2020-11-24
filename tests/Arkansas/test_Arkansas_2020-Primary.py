import election_data_analysis as e
election = "2020 Primary"
jurisdiction = "Arkansas"

def test_data_exists(dbname):
    assert e.data_exists("2020 Primary", "Arkansas", dbname=dbname)


def test_ar_presidential_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Arkansas",
            "US President (AR) (Republican Party)",
            dbname=dbname,
        )
        == 246037
    )


# No statewide (non-presidential) contests in 2020


def test_ar_senate_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Arkansas",
            "AR Senate District 25 (Democratic Party)",
            dbname=dbname,
        )
        == 8656
    )


def test_ar_house_totals_20(dbname):
    assert (
        e.contest_total(
            "2020 Primary",
            "Arkansas",
            "AR House District 41 (Democratic Party)",
            dbname=dbname,
        )
        == 3354
    )


def test_ar_contest_by_vote_type_20(dbname):
    assert (
        e.count_type_total(
            "2020 Primary",
            "Arkansas",
            "AR Senate District 13 (Republican Party)",
            "absentee",
            dbname=dbname,
        )
        == 69 + 19
    )


def test_ar_totals_match_vote_type_20(dbname):
    assert (
        e.check_totals_match_vote_types("2020 Primary", "Arkansas", dbname=dbname)
        == True
    )



def test_all_candidates_known(dbname):
    assert e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname) == []
