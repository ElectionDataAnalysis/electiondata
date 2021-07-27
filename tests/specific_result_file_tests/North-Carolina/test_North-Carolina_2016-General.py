import electiondata as e

# Instructions:
#   Copy this template and rename, including your jurisdiction and the timestamp of the results file
#       following this model: test_North-Carolina_2020_General_20201113_1645.py
#           * start with test_ so pytest will find the file
#           * get the underscores and hyphens right, so the system will find the file
#           * timestamp is YYYYMMDD_xxxx, where xxxx is the military-time Pacific Standard Time
#           * (timestamp is crucial for files collected during recounts and evolving canvass counts)
#   Change the constants to values from your file
#   "triple-quote" out any tests for contest types your state doesn't have in 2020
#   (e.g., Florida has no US Senate contest)
#   (Optional) Change district numbers
#   Replace each '-1' with the correct number calculated from the results file.
#   Move this testing file to the correct jurisdiction folder in `elections/tests`

# # # constants - CHANGE THESE!! - use internal db names
election = "2016 General"
jurisdiction = "North Carolina"
abbr = "NC"
total_pres_votes = 4741564  # total of all votes for US President
cd = 3  # US House congressional district
total_cd_votes = (
    217531 + 106170
)  # total votes in that US House contest in the chosen cd
shd = 4  # state house district
total_shd_votes = 24646  # total votes in that State House contest
ssd = 15  # state senate district
total_ssd_votes = 117985  # total votes in that State Senate contest
single_vote_type = "early"  # pick any one with corresponding data in your file, but use internal db name
pres_votes_vote_type = 2929797  # total votes for US President of that vote type
county_or_other = "county"  # Change this only if results are subdivided by something other than counties
#  e.g., 'parish' in LA, 'state-house' in Alaska, 'ward' in Philadelphia
single_county = "North Carolina;Bertie County"  # pick any one from your file, but use internal db name
pres_votes_county = (
    5778 + 3456 + 70 + 40 + 3
)  # total votes for US President in that county

"""
Walter B. Jones	REP	217,531	67.20%
Ernest T. Reeves	DEM	106,170	32.80%

NAME ON BALLOT	PARTY	BALLOT COUNT	PERCENT
John Alexander	REP	58,999	50.01%
Laurel Deegan-Fricke	DEM	53,905	45.69%
Brad Hessel	LIB	5,081	4.31%

hoice	Total Votes	Percent	Election Day	Absentee One-Stop	Absentee By-Mail	Provisional
Donald J. Trump	2,362,631	49.83%	875,482	1,376,149	98,147	12,853
Hillary Clinton	2,189,316	46.17%	624,895	1,460,223	91,980	12,218
Gary Johnson	130,126	2.74%	59,212	63,575	6,330	1,009
Write-In (Miscellaneous)	47,386	1.00%	20,520	23,914	2,668	284
Jill Stein	12,105	0.26%	5,451	5,936	650	68
"""


def test_data_exists(dbname):
    assert e.data_exists(election, jurisdiction, dbname=dbname)


def test_presidential(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            sub_unit_type=county_or_other,
            dbname=dbname,
        )
        == total_pres_votes
    )


def test_congressional_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US House {abbr} District {cd}",
            sub_unit_type=county_or_other,
            dbname=dbname,
        )
        == total_cd_votes
    )


def test_state_senate_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"{abbr} Senate District {ssd}",
            sub_unit_type=county_or_other,
            dbname=dbname,
        )
        == total_ssd_votes
    )


def test_state_house_totals(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"{abbr} House District {shd}",
            sub_unit_type=county_or_other,
            dbname=dbname,
        )
        == total_shd_votes
    )


def test_standard_vote_types(dbname):
    assert e.check_count_types_standard(election, jurisdiction, dbname=dbname)


def test_vote_type_counts_consistent(dbname):
    assert e.check_totals_match_vote_types(election, jurisdiction, dbname=dbname)


def test_all_candidates_known(dbname):
    assert (
        e.get_contest_with_unknown_candidates(election, jurisdiction, dbname=dbname)
        == []
    )


def test_count_type_subtotal(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            dbname=dbname,
            sub_unit_type=county_or_other,
            vote_type=single_vote_type,
        )
        == pres_votes_vote_type
    )


def test_county_subtotal(dbname):
    assert (
        e.contest_total(
            election,
            jurisdiction,
            f"US President ({abbr})",
            dbname=dbname,
            county=single_county,
            sub_unit_type=county_or_other,
        )
        == pres_votes_county
    )
