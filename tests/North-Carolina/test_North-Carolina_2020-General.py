import election_data_analysis as e

def test_data(dbname):
    assert e.data_exists("2020 General","North Carolina",dbname=dbname)

def test_nc_statewide_totals_20(dbname):
    assert (e.contest_total(
            "2020 General",
            "North Carolina",
            "US House NC District 3",
            dbname=dbname,
        )
            == 358473
    )



def test_nc_senate_totals_20(dbname):
    assert (e.contest_total(
            "2020 General",
            "North Carolina",
            "NC Senate District 15",
            dbname=dbname,
        )
            == 122221
    )



def test_nc_house_totals_20(dbname):
    assert (e.contest_total(
            "2020 General",
            "North Carolina",
            "NC House District 1",
            dbname=dbname,
        )
            == 37758
    )



def test_nc_totals_match_vote_type_20(dbname):
    assert (e.check_totals_match_vote_types("2020 General","North Carolina" ,dbname=dbname) == True)


