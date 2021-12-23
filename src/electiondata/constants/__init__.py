import csv
import re
import os
from typing import Dict, List, Any
import pandas as pd

# NB: "if 1:" statements are cosmetic, to allow code folding by topic

# jurisdiction info
if 1:
    array_of_jurisdictions = """Alabama
    Alaska
    American Samoa
    Arizona
    Arkansas
    California
    Colorado
    Connecticut
    Delaware
    District of Columbia
    Florida
    Georgia
    Guam
    Hawaii
    Idaho
    Illinois
    Indiana
    Iowa
    Kansas
    Kentucky
    Louisiana
    Maine
    Maryland
    Massachusetts
    Michigan
    Minnesota
    Mississippi
    Missouri
    Montana
    Nebraska
    Nevada
    New Hampshire
    New Jersey
    New Mexico
    New York
    North Carolina
    North Dakota
    Northern Mariana Islands
    Ohio
    Oklahoma
    Oregon
    Pennsylvania
    Puerto Rico
    Rhode Island
    South Carolina
    South Dakota
    Tennessee
    Texas
    Utah
    US Virgin Islands
    Vermont
    Virginia
    Washington
    West Virginia
    Wisconsin
    Wyoming"""
    abbr = {
        "Alabama": "AL",
        "Alaska": "AK",
        "Arizona": "AZ",
        "Arkansas": "AR",
        "California": "CA",
        "Colorado": "CO",
        "Connecticut": "CT",
        "Delaware": "DE",
        "Florida": "FL",
        "Georgia": "GA",
        "Hawaii": "HI",
        "Idaho": "ID",
        "Illinois": "IL",
        "Indiana": "IN",
        "Iowa": "IA",
        "Kansas": "KS",
        "Kentucky": "KY",
        "Louisiana": "LA",
        "Maine": "ME",
        "Maryland": "MD",
        "Massachusetts": "MA",
        "Michigan": "MI",
        "Minnesota": "MN",
        "Mississippi": "MS",
        "Missouri": "MO",
        "Montana": "MT",
        "Nebraska": "NE",
        "Nevada": "NV",
        "New Hampshire": "NH",
        "New Jersey": "NJ",
        "New Mexico": "NM",
        "New York": "NY",
        "North Carolina": "NC",
        "North Dakota": "ND",
        "Ohio": "OH",
        "Oklahoma": "OK",
        "Oregon": "OR",
        "Pennsylvania": "PA",
        "Rhode Island": "RI",
        "South Carolina": "SC",
        "South Dakota": "SD",
        "Tennessee": "TN",
        "Texas": "TX",
        "Utah": "UT",
        "Vermont": "VT",
        "Virginia": "VA",
        "Washington": "WA",
        "West Virginia": "WV",
        "Wisconsin": "WI",
        "Wyoming": "WY",
        "American Samoa": "AS",
        "District of Columbia": "DC",
        "District Of Columbia": "DC",
        "Guam": "GU",
        "Marshall Islands": "MH",
        "Northern Mariana Islands": "MP",
        "Puerto Rico": "PR",
        "US Virgin Islands": "VI",
    }
    default_subdivision_type = "county"
    subdivision_reference_file_path = os.path.join(
        "jurisdictions",
        "000_for_all_jurisdictions",
        "major_subjurisdiction_types.txt",
    )
    def jurisdiction_wide_contests(abbr: str) -> List[str]:
        """
        Inputs:
            abbr: str, abbreviation for jurisdiction (e.g., TX)

        Returns:
            List[str], standard list of jurisdiction-wide contets
        """
        return [
            f"US President ({abbr})",
            f"{abbr} Governor",
            f"US Senate {abbr}",
            f"{abbr} Attorney General",
            f"{abbr} Lieutenant Governor",
            f"{abbr} Treasurer",
            f"{abbr} Secretary of State",
        ]

# analysis parameters
if 1:
    # z-scores below this value are considered not particularly anomalous by
    # outlier-curating algorithm
    outlier_zscore_cutoff = 2.3
    # max number of different reporting units to show in each bar chart.
    # If there are more in the contest district, least interesting ones
    # will be averaged into the last set of bars in the bar chart.
    max_rus_per_bar_chart = 8

# display information
if 1:
    """maps ReportingUnitType of election district of contest to the user-facing label for that type of contest
    for use in Analyzer.display_options()"""
    contest_type_mappings = {
        "congressional": "Congressional",
        "state": "Statewide",
        "state-house": "State House",
        "state-senate": "State Senate",
        "city": "Citywide",
        "ward": "Ward",
        "territory": "Territory-wide",
    }
    contest_types_model = contest_type_mappings.keys()

# encodings
if 1:
    default_encoding = "utf_8"
    recognized_encodings = {
        "iso2022jp",
        "arabic",
        "cp861",
        "csptcp154",
        "shiftjisx0213",
        "950",
        "IBM775",
        "IBM861",
        "shift_jis",
        "shift-jis",
        "euc_jp",
        "euc-jp",
        "ibm1026",
        "ascii",
        "ASCII",
        "IBM437",
        "EBCDIC-CP-BE",
        "csshiftjis",
        "cp1253",
        "jisx0213",
        "latin",
        "cp874",
        "861",
        "windows-1255",
        "cp1361",
        "macroman",
        "ms950",
        "iso-2022-jp-3",
        "iso8859_14",
        "cp949",
        "utf_16",
        "utf-16",
        "932",
        "cp737",
        "iso2022_jp_2004",
        "ks_c-5601",
        "iso-2022-kr",
        "ms936",
        "cp819",
        "iso-8859-3",
        "windows-1258",
        "csiso2022kr",
        "iso-8859-2",
        "iso2022_jp_ext",
        "hz",
        "iso-8859-13",
        "IBM855",
        "cp1140",
        "866",
        "862",
        "iso2022jp-2004",
        "cp1250",
        "windows-1254",
        "cp1258",
        "gb2312-1980",
        "936",
        "L6",
        "iso-8859-6",
        "ms932",
        "macgreek",
        "cp154",
        "big5-tw",
        "maccentraleurope",
        "iso-8859-7",
        "ks_x-1001",
        "csbig5",
        "cp1257",
        "latin1",
        "mac_roman",
        "mac-roman",
        "euckr",
        "latin3",
        "eucjis2004",
        "437",
        "cp500",
        "mac_latin2",
        "CP-GR",
        "IBM863",
        "hz-gb-2312",
        "iso2022jp-3",
        "iso-8859-15",
        "koi8_r",
        "sjisx0213",
        "windows-1252",
        "850",
        "cp855",
        "windows1256",
        "eucjisx0213",
        "hkscs",
        "gb18030",
        "iso-2022-jp-2004",
        "L1",
        "cyrillic-asian",
        "iso2022jp-ext",
        "cp1006",
        "utf16",
        "iso2022_kr",
        "iso2022jp-2",
        "shiftjis",
        "IBM037",
        "gb2312-80",
        "IBM500",
        "865",
        "UTF-16BE",
        "IBM864",
        "EBCDIC-CP-CH",
        "iso-8859-4",
        "cp856",
        "iso2022_jp_1",
        "eucjp",
        "iso-2022-jp-1",
        "iso8859_3",
        "gb18030-2000",
        "cp860",
        "mskanji",
        "iso2022jp-1",
        "iso-8859-8",
        "iso-2022-jp-ext",
        "csiso58gb231280",
        "shift_jis_2004",
        "L2",
        "ms1361",
        "cp852",
        "ms949",
        "IBM865",
        "cp437",
        "iso8859_4",
        "iso8859_2",
        "cp1255",
        "euc_jisx0213",
        "cp1252",
        "macturkish",
        "iso8859_9",
        "ptcp154",
        "949",
        "cp864",
        "s_jisx0213",
        "big5-hkscs",
        "korean",
        "iso2022_jp_2",
        "cp932",
        "euc-cn",
        "latin5",
        "utf_8",
        "utf-8",
        "ibm1140",
        "cp862",
        "euc_kr",
        "euc-kr",
        "iso8859_8",
        "iso-8859-9",
        "utf8",
        "cp1251",
        "863",
        "cp850",
        "cp857",
        "greek",
        "latin8",
        "iso2022_jp_3",
        "iso-8859-10",
        "big5hkscs",
        "ms-kanji",
        "iso2022kr",
        "646",
        "iso8859_7",
        "koi8_u",
        "mac_greek",
        "mac-greek",
        "windows-1251",
        "cp775",
        "IBM860",
        "u-jis",
        "iso-8859-5",
        "us-ascii",
        "maccyrillic",
        "IBM866",
        "L3",
        "sjis2004",
        "cp1256",
        "sjis_2004",
        "sjis-2004",
        "852",
        "windows-1250",
        "latin4",
        "cp037",
        "shift_jisx0213",
        "greek8",
        "latin6",
        "latin2",
        "mac_turkish",
        "mac-turkish",
        "IBM862",
        "iso8859-1",
        "cp1026",
        "IBM852",
        "pt154",
        "iso-2022-jp-2",
        "ujis",
        "855",
        "iso-8859-14",
        "iso-2022-jp",
        "utf_16_be",
        "chinese",
        "maclatin2",
        "U7",
        "hzgb",
        "iso8859_5",
        "857",
        "IBM850",
        "8859",
        "gb2312",
        "cp866",
        "CP-IS",
        "latin_1",
        "latin-1",
        "L4",
        "euccn",
        "cyrillic",
        "IBM424",
        "cp863",
        "UTF-16LE",
        "mac_cyrillic",
        "mac-cyrillic",
        "iso8859_10",
        "L8",
        "IBM869",
        "ksc5601",
        "860",
        "iso2022_jp",
        "hz-gb",
        "UTF",
        "utf8ascii",
        "utf_7",
        "utf-7",
        "cp936",
        "euc_jis_2004",
        "iso-ir-58",
        "csiso2022jp",
        "IBM039",
        "eucgb2312-cn",
        "cp950",
        "iso8859_13",
        "shiftjis2004",
        "sjis",
        "U8",
        "cp1254",
        "s_jis",
        "s-jis",
        "gbk",
        "hebrew",
        "U16",
        "big5",
        "cp865",
        "cp424",
        "uhc",
        "windows-1257",
        "869",
        "iso-8859-1",
        "windows-1253",
        "ksx1001",
        "johab",
        "IBM857",
        "L5",
        "iso8859_6",
        "cp869",
        "cp875",
        "mac_iceland",
        "mac-iceland",
        "iso8859_15",
        "maciceland",
        "utf_16_le",
        "EBCDIC-CP-HE",
        "ks_c-5601-1987",
    }

# parameters for user-created files (run_time.ini, <result_file>.ini, etc.)
if 1:
    sdl_pars_req = [
        "munger_list",
        "results_file",
        "results_short_name",
        "results_download_date",
        "results_source",
        "results_note",
        "jurisdiction",
        "election",
        "is_preliminary",
    ]
    sdl_pars_opt = [
        "CandidateContest",
        "BallotMeasureContest",
        "BallotMeasureSelection",
        "Candidate",
        "Party",
        "CountItemType",
        "ReportingUnit",
        "Contest",
    ]
    multi_data_loader_pars = [
        "results_dir",
        "archive_dir",
        "repository_content_root",
        "reports_and_plots_dir",
    ]
    optional_mdl_pars = [
        "unloaded_dir",
    ]
    req_for_combined_file_loading = [
        "results_file",
        "munger_list",
        "results_download_date",
        "results_source",
        "results_note",
        "secondary_source",
        "results_short_name",
    ]
    prep_pars = [
        "name",
        "abbreviated_name",
        "count_of_state_house_districts",
        "count_of_state_senate_districts",
        "count_of_us_house_districts",
        "reporting_unit_type",
    ]
    optional_prep_pars = []
    analyze_pars = ["db_paramfile", "db_name"]
    error_keys = {
        "ini",
        "munger",
        "jurisdiction",
        "file",
        "system",
        "database",
        "test",
        "dictionary",
    }
    warning_keys = {f"warn-{ek}" for ek in error_keys}
    no_param_file_types = {"nist_v2_xml"}
    opt_munger_data_types: Dict[str, str] = {
        "count_location": "string-with-opt-list",
        "munge_field_types": "list-of-strings",
        "sheets_to_read_names": "list-of-strings",
        "sheets_to_skip_names": "list-of-strings",
        "sheets_to_read_numbers": "list-of-integers",
        "sheets_to_skip_names_numbers": "list-of-integers",
        "rows_to_skip": "integer",
        "flat_text_delimiter": "string",
        "quoting": "string",
        "thousands_separator": "string",
        "encoding": "string",
        "namespace": "string",
        "count_field_name_row": "int",
        "string_field_column_numbers": "list-of-integers",
        "count_header_row_numbers": "list-of-integers",
        "noncount_header_row": "int",
        "all_rows": "string",
        "multi_block": "string",
        "merged_cells": "string",
        "max_blocks": "integer",
        "constant_over_file": "list-of-strings",
    }
    munger_dependent_reqs: Dict[str, Dict[str, List[str]]] = {
        "file_type": {
            "flat_text": ["flat_text_delimiter", "count_location"],
            "xml": ["count_location"],
            "json-nested": ["count_location"],
            "excel": ["count_location"],
        },
    }
    req_munger_parameters: Dict[str, Dict[str, Any]] = {
        "file_type": {
            "data_type": "string",
            "allowed_values": [
                "excel",
                "json-nested",
                "xml",
                "flat_text",
                "nist_v2_xml",
            ],
        },
    }
    string_location_reqs: Dict[str, List[str]] = {
        "by_column_name": [],
        "in_count_headers": ["count_header_row_numbers"],
        "constant_over_file": [],
        "constant_over_sheet_or_block": ["constant_over_sheet_or_block"],
    }
    single_ej_munge_elements = [
        "BallotMeasureContest",
        "CandidateContest",
        "BallotMeasureSelection",
        "Candidate",
        "Party",
        "ReportingUnit",
        "CountItemType",
    ]
    standard_juris_csv_reading_kwargs = {
        "index_col": False,
        "encoding": default_encoding,
        "quoting": csv.QUOTE_MINIMAL,
        "sep": "\t",
        "keep_default_na": False,
        "na_values": "",
    }
    bmselections = ["Yes", "No"]


# reporting
if 1:
    juris_load_report_keys = [
        "database",
        "dictionary",
        "file",
        "ini",
        "jurisdiction",
        "munger",
        "warn-database",
        "warn-dictionary",
        "warn-file",
        "warn-ini",
        "warn-jurisdiction",
        "warn-munger",
        "warn-test",
    ]
    regex_failure_string = " <- Does not match regular expression"
# regex patterns
if 1:
    brace_pattern = re.compile(r"{<([^,]*)>,([^{}]*|[^{}]*{[^{}]*}[^{}]*)}")
    pandas_default_pattern = r"^Unnamed: (\d+)_level_(\d+)$"

# constants dictated by NIST
if 1:
    default_nist_format = "json"  # other option is "xml"
    default_issuer = (
        "unspecified user of code base at github.com/ElectionDataAnalysis/electiondata"
    )
    default_issuer_abbreviation = "unspecified"
    default_status = "unofficial-partial"  # choices are limited by xsd schema
    default_vendor_application_id = (
        "open source software at github.com/ElectionDataAnalysis/electiondata"
    )
    nist_schema_location = "https://github.com/usnistgov/ElectionResultsReporting/raw/version2/NIST_V2_election_results_reporting.xsd"
    nist_namespace = "http://itl.nist.gov/ns/voting/1500-100/v2"
    nist_standard = {
        "CountItemType": [
            "absentee",
            "absentee-fwab",
            "absentee-in-person",
            "absentee-mail",
            "early",
            "election-day",
            "provisional",
            "seats",
            "total",
            "uocava",
            "write-in",
        ],
        "CountItemStatus": [
            "completed",
            "in-process",
            "not-processed",
        ],
        "ElectionType": [
            "general",
            "partisan - primary - closed",
            "partisan - primary - open",
            "primary",
            "runoff",
            "special",
        ],
        "ReportingUnitType": [
            "ballot-batch",
            "ballot-style-area",
            "borough",
            "city",
            "city-council",
            "combined-precinct",
            "congressional",
            "country",
            "county",
            "county-council",
            "drop-box",
            "judicial",
            "municipality",
            "polling-place",
            "precinct",
            "school",
            "special",
            "split-precinct",
            "state",
            "state-house",
            "state-senate",
            "town",
            "township",
            "utility",
            "village",
            "vote-center",
            "ward",
            "water",
        ],
    }
    cit_from_raw_nist_df = pd.DataFrame(
        [["CountItemType", x, x] for x in nist_standard["CountItemType"]],
        columns=["cdf_element", "cdf_internal_name", "raw_identifier_value"],
    )


# constants related to MIT Election Data Science presidential data set
if 1:
    mit_pres_datafile_info = {
        "download_date": "2021-06-20",
        "note": "Presidential only, total only, results by county",
        "source": 'MIT Election Data and Science Lab, 2018, "County Presidential Election Returns 2000-2020",'
        "https://doi.org/10.7910/DVN/VOQCHQ, Harvard Dataverse, V8, UNF:6:20+0NUTez42tTN5eqIKd5g== [fileUNF]",
    }
    mit_pres_cols = {
        "Jurisdiction": "state",
        "Election": "year",
        "Count": "candidatevotes",
        "ReportingUnit_raw": "county_fips",
        "CandidateContest_raw": "office",
        "Party_raw": "party",
        "Candidate_raw": "candidate",
        "CountItemType_raw": "mode",
    }
    mit_party = {
        "DEMOCRAT": "Democratic Party",
        "OTHER": "none or unknown",
        "REPUBLICAN": "Republican Party",
        "LIBERTARIAN": "Libertarian Party",
        "GREEN": "Green Party",
        "a better rhode island": "A Better Rhode Island Party",
        "american": "American Party",
        "american constitution": "American Constitution Party",
        "amigo constitution liberty": "Amigo Constitution Liberty Party",
        "approval voting": "Approval Voting Party",
        "berlin-northfield alliance": "Berlin-Northfield Alliance Party",
        "bringing back manufacturing": "Bringing Back Manufacturing Party",
        "c4c 2018": "C4C 2018 Party",
        "candid common sense": "Candid Common Sense Party",
        "cannot be bought": "Cannot Be Bought Party",
        "check this column": "Check This Column Party",
        "clear water": "Clear Water Party",
        "common sense independent": "Common Sense Independent Party",
        "compassion": "Compassion Party",
        "conservative": "Conservative Party",
        "constitution": "Constitution Party",
        "cooperative green economy": "Cooperative Green Economy Party",
        "dem/prog": "Dem / Prog Party",
        "dem/rep": "Dem / Rep Party",
        "democrat": "Democratic Party",
        "democrat/republican": "Democratic / Republican Party",
        "democrat&republican": "Democratic / Republican Party",
        "democratic / republican": "Democratic  / Republican Party",
        "democratic-farmer-labor": "Democratic-Farmer-Labor Party",
        "democratic-npl": "Democratic-Npl Party",
        "downstate united": "Downstate United Party",
        "earth rights": "Earth Rights Party",
        "economic growth": "Economic Growth Party",
        "ed the barber": "Ed The Barber Party",
        "fair representation vt": "Fair Representation Vt Party",
        "for the people": "For The People Party",
        "freedom, responsibility, action": "Freedom, Responsibility, Action Party",
        "grassroots-legalize cannabis": "Grassroots-Legalize Cannabis Party",
        "green": "Green Party",
        "green independent": "Green Independent Party",
        "green mountain": "Green Mountain Party",
        "green party": "Green Party Party",
        "green-rainbow": "Green-Rainbow Party",
        "griebel frank for ct": "Griebel Frank For Ct Party",
        "honesty, integrity, compassion": "Honesty, Integrity, Compassion Party",
        "hope in unity": "Hope In Unity Party",
        "in maio we trust": "In Maio We Trust Party",
        "independence": "Independence Party",
        "independent": "Independent Party",
        "independent american": "Independent American Party",
        "independent and veteran": "Independent And Veteran Party",
        "independent for maine": "Independent For Maine Party",
        "independent nomination": "Independent Nomination Party",
        "independent progressive": "Independent Progressive Party",
        "independent republican": "Independent Republican Party",
        "integrity transparency accountability": "Integrity Transparency Accountability Party",
        "legal marijuana now": "Legal Marijuana Now Party",
        "legal medical now": "Legal Medical Now Party",
        "libertarian": "Libertarian Party",
        "libertarian party of florida": "Libertarian Party Of Florida Party",
        "liberty union": "Liberty Union Party",
        "maine socialist party": "Maine Socialist Party Party",
        "make it simple": "Make It Simple Party",
        "massachusetts independent": "Massachusetts Independent Party",
        "minnesota green": "Minnesota Green Party",
        "moderate": "Moderate Party",
        "mountain": "Mountain Party",
        "NA": "Na Party",
        "natural law": "Natural Law Party",
        "never give up": "Never Give Up Party",
        "new day nj": "New Day Nj Party",
        "new way forward": "New Way Forward Party",
        "no  affiliation": "No  Affiliation Party",
        "no affiliation": "No Affiliation Party",
        "no party": "No Party Party",
        "no party affiliation": "No Party Affiliation Party",
        "no party preference": "No Party Preference Party",
        "nonpartisan": "Nonpartisan Party",
        "pacific green": "Pacific Green Party",
        "people's unenrolled independent": "People's Unenrolled Independent Party",
        "petitioning candidate": "Petitioning Candidate Party",
        "prog/dem": "Prog / Dem Party",
        "progressive": "Progressive Party",
        "reform": "Reform Party",
        "reform party of florida": "Reform Party Of Florida Party",
        "rep/dem": "Rep / Dem Party",
        "repeal bail reform": "Repeal Bail Reform Party",
        "republican": "Republican Party",
        "second american revolution": "Second American Revolution Party",
        "stop the insanity": "Stop The Insanity Party",
        "tax revolt": "Tax Revolt Party",
        "the inclusion candidate": "The Inclusion Candidate Party",
        "time for change": "Time For Change Party",
        "time for truth": "Time For Truth Party",
        "together we can": "Together We Can Party",
        "trade, health, environment": "Trade, Health, Environment Party",
        "unaffiliated": "Unaffiliated Party",
        "unenrolled": "Unenrolled Party",
        "united citizens": "United Citizens Party",
        "united utah": "United Utah Party",
        "unity": "Unity Party",
        "us taxpayers": "Us Taxpayers Party",
        "we deserve better": "We Deserve Better Party",
        "women's equality": "Women's Equality Party",
        "working class": "Working Class Party",
        "working families": "Working Families Party",
        "your voice hard": "Your Voice Hard Party",
    }
    mit_elections = {
        "2000": "2000 General",
        "2004": "2004 General",
        "2008": "2008 General",
        "2012": "2012 General",
        "2016": "2016 General",
        "2018": "2018 General",
    }  # 2020 is there but we won't load it.
    mit_election_types = {
        "2000": "general",
        "2004": "general",
        "2008": "general",
        "2012": "general",
        "2016": "general",
        "2018": "general",
    }
    mit_cit = {
        "TOTAL": "total",
        "absentee": "absentee",
        "absentee by mail": "absentee-mail",
        "absentee mail": "absentee-mail",
        "absentee/early vote": "absentee or early",
        "advance in person": "early",
        "early": "early",
        "early vote": "early",
        "election": "election-day",
        "election day": "election-day",
        "electon day": "election-day",
        "machine": "election-day",
        "mail ballots": "absentee-mail",
        "one stop": "early",
        "provisional": "provisional",
        "total": "total",
    }
    mit_candidates = [
        "MITT ROMNEY",
        "OTHER",
        "DONALD J TRUMP",
        "DONALD TRUMP",
        "HILLARY CLINTON",
        "PRINCESS KHADIJAH M JACOB-FAMBRO",
        "JOHN KERRY",
        "JOHN MCCAIN",
        "BARACK OBAMA",
        "JOSEPH R BIDEN JR",
        "RALPH NADER",
        "JO JORGENSEN",
        "WRITEIN",
        "AL GORE",
        'ROQUE "ROCKY" DE LA FUENTE',
        "GEORGE W. BUSH",
        "BRIAN CARROLL",
        "JEROME M SEGAL",
        "BROCK PIERCE",
        "DON BLANKENSHIP",
    ]
    mit_correction = {
        "District Of Columbia": "District of Columbia",
        "Donald Trump": "Donald J. Trump",
        "Joseph R Biden Jr": "Joseph R. Biden",
        "John Mccain": "John McCain",
        """Roque "Rocky" De La Fuenta""": "Roque 'Rocky' de la Fuenta",
    }

# census
if 1:
    census_noncount_columns = ["Name", "state", "county"]
    fips = {
        "Alabama": "01",
        "Alaska": "02",
        "Arizona": "04",
        "Arkansas": "05",
        "California": "06",
        "Colorado": "08",
        "Connecticut": "09",
        "Delaware": "10",
        "District of Columbia": "11",
        "Florida": "12",
        "Georgia": "13",
        "Hawaii": "15",
        "Idaho": "16",
        "Illinois": "17",
        "Indiana": "18",
        "Iowa": "19",
        "Kansas": "20",
        "Kentucky": "21",
        "Louisiana": "22",
        "Maine": "23",
        "Maryland": "24",
        "Massachusetts": "25",
        "Michigan": "26",
        "Minnesota": "27",
        "Mississippi": "28",
        "Missouri": "29",
        "Montana": "30",
        "Nebraska": "31",
        "Nevada": "32",
        "New Hampshire": "33",
        "New Jersey": "34",
        "New Mexico": "35",
        "New York": "36",
        "North Carolina": "37",
        "North Dakota": "38",
        "Ohio": "39",
        "Oklahoma": "40",
        "Oregon": "41",
        "Pennsylvania": "42",
        "Rhode Island": "44",
        "South Carolina": "45",
        "South Dakota": "46",
        "Tennessee": "47",
        "Texas": "48",
        "Utah": "49",
        "Vermont": "50",
        "Virginia": "51",
        "Washington": "53",
        "West Virginia": "54",
        "Wisconsin": "55",
        "Wyoming": "56",
        "American Samoa": "60",
        "Guam": "66",
        "Northern Mariana Islands": "69",
        "Puerto Rico": "72",
        "U.S. Minor Outlying Islands": "74",
        "U.S. Virgin Islands": "78",
    }
    acs5_columns = {
        # {display category: {census.gov column name: internal column name}}
        "Population": {"B01001_001E": "Population"},
        "Pop. by Income (Avg Household)": {
            "B19001_002E": "Less than $10,000",
            "B19001_003E": "$10,000 to $14,999",
            "B19001_004E": "$15,000 to $19,999",
            "B19001_005E": "$20,000 to $24,999",
            "B19001_006E": "$25,000 to $29,999",
            "B19001_007E": "$30,000 to $34,999",
            "B19001_008E": "$35,000 to $39,999",
            "B19001_009E": "$40,000 to $44,999",
            "B19001_010E": "$45,000 to $49,999",
            "B19001_011E": "$50,000 to $59,999",
            "B19001_012E": "$60,000 to $74,999",
            "B19001_013E": "$75,000 to $99,999",
            "B19001_014E": "$100,000 to $124,999",
            "B19001_015E": "$125,000 to $149,999",
            "B19001_016E": "$150,000 to $199,999",
            "B19001_017E": "$200,000 or more",
        },
        "Pop. by Race": {
            "B01001A_001E": "White",
            "B01001B_001E": "Black",
            "B01001C_001E": "American Indian and Alaska Native",
            "B01001D_001E": "Asian",
            "B01001E_001E": "Hawaiian and Pacific Islander",
            "B01001F_001E": "Some other race (one race)",
            "B01001G_001E": "Some other race (two or more races)",
            "B01001I_001E": "Hispanic",
        },
        "Pop.by Age": {
            "B01001_007E": "Male 18 and 19 years",
            "B01001_008E": "Male 20 years",
            "B01001_009E": "Male 21 years",
            "B01001_010E": "Male 22 to 24 years",
            "B01001_011E": "Male 25 to 29 years",
            "B01001_012E": "Male 30 to 34 years",
            "B01001_013E": "Male 35 to 39 years",
            "B01001_014E": "Male 40 to 44 years",
            "B01001_015E": "Male 45 to 49 years",
            "B01001_016E": "Male 50 to 54 years",
            "B01001_017E": "Male 55 to 59 years",
            "B01001_018E": "Male 60 and 61 years",
            "B01001_019E": "Male 62 to 64 years",
            "B01001_020E": "Male 65 and 66 years",
            "B01001_021E": "Male 67 to 69 years",
            "B01001_022E": "Male 70 to 74 years",
            "B01001_023E": "Male 75 to 79 years",
            "B01001_024E": "Male 80 to 84 years",
            "B01001_025E": "Male 85 years and over",
            "B01001_031E": "Female 18 and 19 years",
            "B01001_032E": "Female 20 years",
            "B01001_033E": "Female 21 years",
            "B01001_034E": "Female 22 to 24 years",
            "B01001_035E": "Female 25 to 29 years",
            "B01001_036E": "Female 30 to 34 years",
            "B01001_037E": "Female 35 to 39 years",
            "B01001_038E": "Female 40 to 44 years",
            "B01001_039E": "Female 45 to 49 years",
            "B01001_040E": "Female 50 to 54 years",
            "B01001_041E": "Female 55 to 59 years",
            "B01001_042E": "Female 60 and 61 years",
            "B01001_043E": "Female 62 to 64 years",
            "B01001_044E": "Female 65 and 66 years",
            "B01001_045E": "Female 67 to 69 years",
            "B01001_046E": "Female 70 to 74 years",
            "B01001_047E": "Female 75 to 79 years",
            "B01001_048E": "Female 80 to 84 years",
            "B01001_049E": "Female 85 years and over",
        },
    }
    acs_5_label_summands = {
        "18 to 29": [
            "Male 18 and 19 years",
            "Male 20 years",
            "Male 21 years",
            "Male 22 to 24 years",
            "Male 25 to 29 years",
            "Female 18 and 19 years",
            "Female 20 years",
            "Female 21 years",
            "Female 22 to 24 years",
            "Female 25 to 29 years",
        ],
        "30 to 49": [
            "Male 30 to 34 years",
            "Male 35 to 39 years",
            "Male 40 to 44 years",
            "Male 45 to 49 years",
            "Female 30 to 34 years",
            "Female 35 to 39 years",
            "Female 40 to 44 years",
            "Female 45 to 49 years",
        ],
        "50 to 64": [
            "Male 50 to 54 years",
            "Male 55 to 59 years",
            "Male 60 and 61 years",
            "Male 62 to 64 years",
            "Female 50 to 54 years",
            "Female 55 to 59 years",
            "Female 60 and 61 years",
            "Female 62 to 64 years",
        ],
        "64+": [
            "Male 65 and 66 years",
            "Male 67 to 69 years",
            "Male 70 to 74 years",
            "Male 75 to 79 years",
            "Male 80 to 84 years",
            "Male 85 years and over",
            "Female 65 and 66 years",
            "Female 67 to 69 years",
            "Female 70 to 74 years",
            "Female 75 to 79 years",
            "Female 80 to 84 years",
            "Female 85 years and over",
        ],
        "<$30,000": [
            "Less than $10,000",
            "$10,000 to $14,999",
            "$15,000 to $19,999",
            "$20,000 to $24,999",
            "$25,000 to $29,999",
        ],
        "$30,000 to $49,999": [
            "$30,000 to $34,999",
            "$35,000 to $39,999",
            "$40,000 to $44,999",
            "$45,000 to $49,999",
        ],
        "$50,000 to $99,999": [
            "$50,000 to $59,999",
            "$60,000 to $74,999",
            "$75,000 to $99,999",
        ],
        "$100,000 to $109,999": [
            "$100,000 to $124,999",
            "$125,000 to $149,999",
            "$150,000 to $199,999",
        ],
    }
