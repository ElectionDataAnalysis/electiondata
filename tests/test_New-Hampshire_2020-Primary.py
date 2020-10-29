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
            "US Senate NH (Republican Party)",
            dbname=dbname,
        )
            == 139117
    )


