import re
from typing import Dict,List,Any

import pandas as pd

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
    "Northern Mariana Island": "MP",
    "Puerto Rico": "PR",
    "Virgin Islands": "VI",
}
sdl_pars_req = [
    "munger_list",
    "results_file",
    "results_short_name",
    "results_download_date",
    "results_source",
    "results_note",
    "jurisdiction",
    "election",
]
sdl_pars_opt = [
    "jurisdiction_path",
    "CandidateContest",
    "BallotMeasureContest",
    "BallotMeasureSelection",
    "Candidate",
    "Party",
    "CountItemType",
    "ReportingUnit",
    "Contest",
    "is_preliminary",
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
error_keys = {
    "ini",
    "munger",
    "jurisdiction",
    "file",
    "system",
    "database",
    "test",
}
warning_keys = {f"warn-{ek}" for ek in error_keys}
default_encoding = "utf_8"
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
        "allowed_values": ["excel", "json-nested", "xml", "flat_text", "nist_v2_xml"],
    },
}
string_location_reqs: Dict[str, List[str]] = {
    "by_column_name": [],
    "in_count_headers": ["count_header_row_numbers"],
    "constant_over_file": [],
    "constant_over_sheet_or_block": ["constant_over_sheet_or_block"],
}
all_munge_elements = [
    "BallotMeasureContest",
    "CandidateContest",
    "BallotMeasureSelection",
    "Candidate",
    "Party",
    "ReportingUnit",
    "CountItemType",
]

# regex patterns
brace_pattern = re.compile(r"{<([^,]*)>,([^{}]*|[^{}]*{[^{}]*}[^{}]*)}")
pandas_default_pattern = r"^Unnamed: (\d+)_level_(\d+)$"

# constants dictated by NIST
nist_version = "1.0"
default_issuer = "unspecified user of code base at github.com/ElectionDataAnalysis/elections"
default_issuer_abbreviation = "unspecified"
default_status = "unofficial-partial"  # choices are limited by xsd schema
default_vendor_application_id = (
    "open source software at github.com/ElectionDataAnalysis/elections"
)
nist_schema_location = \
    "https://github.com/usnistgov/ElectionResultsReporting/raw/version2/NIST_V2_election_results_reporting.xsd"
nist_namespace = "http://itl.nist.gov/ns/voting/1500-100/v2"
cit_list = [
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
]
cit_from_raw_nist_df = pd.DataFrame(
    [["CountItemType", x, x] for x in cit_list],
    columns=["cdf_element", "cdf_internal_name", "raw_identifier_value"],
)

# constants related to MIT Election Data Science presidential data set
mit_datafile_info = {
    "download_date": "2021-06-20",
    "note":"Presidential only, total only, results by county",
    "source": 'MIT Election Data and Science Lab, 2018, "County Presidential Election Returns 2000-2020",' 
    'https://doi.org/10.7910/DVN/VOQCHQ, Harvard Dataverse, V8, UNF:6:20+0NUTez42tTN5eqIKd5g== [fileUNF]',

}
mit_cols = {
    "Jurisdiction": "state",
    "Election": "year",
    "Count": "candidatevotes",
    "ReportingUnit_raw": "county_fips",
    "CandidateContest_raw": "office",
    "Party_raw": "party",
    "Candidate_raw": "candidate",
    "CountItemType_raw": "mode"
}
mit_party = {
    "DEMOCRAT": "Democratic Party",
    "OTHER": "none or unknown",
    "REPUBLICAN": "Republican Party",
    "LIBERTARIAN": "Libertarian Party",
    "GREEN": "Green Party",
}
mit_elections = {"2000": "2000 General",
                 "2004": "2004 General",
                 "2008": "2008 General",
                "2012": "2012 General",
                "2016": "2016 General",
}  # 2020 is there but we won't load it.
mit_election_types = {
    "2000": "general",
    "2004": "general",
    "2008": "general",
                "2012": "general",
                "2016": "general",
}
mit_cit = {"TOTAL": "total"}
mit_candidates = ['MITT ROMNEY', 'OTHER', 'DONALD J TRUMP', 'DONALD TRUMP',
       'HILLARY CLINTON', 'PRINCESS KHADIJAH M JACOB-FAMBRO',
       'JOHN KERRY', 'JOHN MCCAIN', 'BARACK OBAMA', 'JOSEPH R BIDEN JR',
       'RALPH NADER', 'JO JORGENSEN', 'WRITEIN', 'AL GORE',
       'ROQUE "ROCKY" DE LA FUENTE', 'GEORGE W. BUSH',
       'BRIAN CARROLL', 'JEROME M SEGAL',
       'BROCK PIERCE', 'DON BLANKENSHIP']
mit_correction ={
    "District Of Columbia": "District of Columbia",
    "Donald Trump": "Donald J. Trump",
    "Joseph R Biden Jr": "Joseph R. Biden",
    "John Mccain": "John McCain",
    """Roque "Rocky" De La Fuenta""": "Roque 'Rocky' de la Fuenta"
}
