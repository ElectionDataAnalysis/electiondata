import election_data_analysis as e
def test_data_exists(dbname):
    assert e.data_exists("2020 Primary","New Hampshire",dbname=dbname)

def test_nh_presidential_20(dbname):
    assert (e.contest_total(
            "2020 Primary",
            "New Hampshire",
            "US President (NH) (Democratic Party)",
            dbname=dbname,
        )
            == 298377 
    )


def test_nh_statewide_totals_20(dbname):
    assert (e.contest_total(
            "2020 Primary",
            "New Hampshire",
            "NH Auditor General (Republican Party)",
            dbname=dbname,
        )
            == 1042092
    )


def test_nh_senate_totals_20(dbname):
    assert (e.contest_total(
            "2020 Primary",
            "New Hampshire",
            "NH Senate District 21 (Democratic Party)",
            dbname=dbname,
        )
            == 18435
    )


def test_nh_house_totals_20(dbname):
    assert (e.contest_total(
            "2020 Primary",
            "New Hampshire",
            "NH House District 100 (Republican Party)",
            dbname=dbname,
        )
            == 6327
    )
