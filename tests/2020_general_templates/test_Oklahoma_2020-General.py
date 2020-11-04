import election_data_analysis as e

#OK20g test
#This file has the tests filled out for presidential as well as both US and state house and senate elections
#1st district is default for district level positions. This can be changed by the user if deired
#If an election is not applicable, you can simply delete from the start of the function line
#to just above the start of the next function line (or end of document if it is the last test)
#Once you've determined which elections apply to your jurisdiction, All you need to do is change
#The -1 at the bottom of the function to the value you independantly calculated for that contest.
#Then move the saved file to the correct jurisdiction folder in the directory above and you can run the test

def data_exists(dbname):
    assert e.data_exists("2020 General","Oklahoma",dbname=dbname)

def test_presidential(dbname):
    assert(e.contest_total(
        "2020 General",
        "Oklahoma",
        "US President (OK)",
        dbname=dbname,
        )
        == -1
    )

def test_senate_totals(dbname):
    assert (e.contest_total(
        "2020 General",
        "Oklahoma",
        "US Senate OK",
        dbname=dbname,
        )
        == -1
    )

def test_congressional_totals(dbname):
    assert (e.contest_total(
        "2020 General",
        "Oklahoma",
        "US House OK District 1",
        dbname=dbname,
        )
        == -1
    )

def test_state_senate_totals(dbname):
    assert (e.contest_total(
        "2020 General",
        "Oklahoma",
        "OK Senate District 1",
        dbname=dbname,
        )
        == -1
    )

def test_state_house_totals(dbname):
    assert ( e.contest_total(
        "2020 General",
        "Oklahoma",
        "OK House District 1",
        dbname=dbname,
        )
        == -1
    )

