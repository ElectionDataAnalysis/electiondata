import election_data_analysis as e
election = "2018 General"
jurisdiction = "Michigan"
def test_data_exists(dbname):
    assert e.data_exists("2018 General", "Michigan", dbname=dbname)

def test_mi_statewide_totals_18(dbname):
    assert (e.contest_total(
            "2018 General",
            "Michigan",
            "MI Governor",
            dbname=dbname,
        )
        == 4250585
    )


def test_mi_senate_totals_18(dbname):
    assert (e.contest_total(
            "2018 General",
            "Michigan",
            "MI Senate District 37",
            dbname=dbname,
        )
        == 124414
    )


def test_mi_house_totals_18(dbname):
    assert (e.contest_total(
            "2018 General",
            "Michigan",
            "MI House District 8",
            dbname=dbname,
        )
        == 28017
    )


# vote types not available



def test_all_candidates_known(dbname):
    assert e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname) == []
