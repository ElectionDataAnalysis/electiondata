import election_data_analysis as e

def test_pa_presidential_20(dbname):
    assert (not e.data_exists("2020 General","Pennsylvania",dbname=dbname) or e.contest_total(
            "2020 General",
            "Pennsylvania",
            "US President (PA)",
            dbname=dbname,
        )
            == 2739007
    )


def test_pa_statewide_totals_20(dbname):
    assert (not e.data_exists("2020 General","Pennsylvania",dbname=dbname) or e.contest_total(
            "2020 General",
            "Pennsylvania",
            "PA Governor",
            dbname=dbname,
        )
            == 2484582
    )


def test_pa_senate_totals_20(dbname):
    assert (not e.data_exists("2020 General","Pennsylvania",dbname=dbname) or e.contest_total(
            "2020 General",
            "Pennsylvania",
            "PA Senate District 20",
            dbname=dbname,
        )
            == 67898
    )


def test_pa_house_totals_20(dbname):
    assert (not e.data_exists("2020 General","Pennsylvania",dbname=dbname) or e.contest_total(
            "2020 General",
            "Pennsylvania",
            "PA House District 100",
            dbname=dbname,
        )
            == 6327
    )
