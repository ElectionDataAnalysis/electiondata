import election_data_analysis as e

# Instructions:
#   Change the constants to values from your file
#   Delete any tests for contest types your state doesn't have in 2020 (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `election_data_analysis/tests`

## constants - CHANGE THESE!! - use internal db names
election = "2020 General"
jurisdiction = 'Michigan'
abbr = 'MI'
single_vote_type = 'CandidateVotes'  # pick any one from your file
single_county = 'Michigan;ALCONA'  # pick any one from your file

def test_data_exists(dbname):
    assert e.data_exists(election,jurisdiction,dbname=dbname)

def test_presidential(dbname):
    assert(e.contest_total(
        election,
        jurisdiction,
        f"US President ({abbr})",
        dbname=dbname,
        )
        == 5519346
    )

def test_statewide_totals(dbname):
    assert (e.contest_total(
        election,
        jurisdiction,
        f"{abbr} Member of the State Board of Education",
        dbname=dbname,
        )
        == 10029018 #note: there are two positions being filled here, so it makes sense that this is almost double the other totals
    )

def test_US_Senator_totals(dbname):
    assert (e.contest_total(
        election,
        jurisdiction,
        f"{abbr} Member of the State Board of Education",
        dbname=dbname,
        )
        == 5460467
    )    
    
    

def test_congressional_totals(dbname):
    assert (e.contest_total(
        election,
        jurisdiction,
        f"US House {abbr} District 10",
        dbname=dbname,
        )
        == 409573
    )

def test_state_senate_totals(dbname):
    assert (e.contest_total(
        election,
        jurisdiction,
        f"{abbr} Senate District 35",
        dbname=dbname,
        )
        == 122414
    )


def test_state_house_totals(dbname):
    assert (e.contest_total(
        election,
        jurisdiction,
        f"{abbr} House District 100",
        dbname=dbname,
        ) 
        == 46201 #I think House District is our internal State House Representative? This is meant to be this office: 100th District Representative in State Legislature 
    )


def test_standard_vote_types(dbname):
    assert e.check_count_types_standard(election, jurisdiction, dbname=dbname)


def test_vote_type_counts_consistent(dbname):
    assert e.check_totals_match_vote_types(election, jurisdiction, dbname=dbname)


def test_count_type_subtotal(dbname):
    assert (e.count_type_total(
        election,
        jurisdiction,
        f"US President ({abbr})",
        single_vote_type,
        dbname=dbname,
        )
        == 5519346 #data does not split by vote type currently, so I think this is what it is supposed to be? 
    )


def test_one_county_vote_type(dbname):
    assert (e.contest_total(
        election,
        jurisdiction,
        f"US President ({abbr})",
        dbname=dbname,
        county=single_county,
        vote_type=single_vote_type,
        )
        == 2142+4848+50+12+11 #should be total votes cast for all Presidential candiates in Alcona County
            )