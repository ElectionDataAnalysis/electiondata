import inspect
import json
import numpy as np
import pandas as pd
from pathlib import Path
from scipy import stats
import scipy.spatial.distance as dist
from sqlalchemy.orm import Session
from typing import Any, Dict, Optional, List

from electiondata import (
    userinterface as ui,
    munge as m,
    database as db,
    constants,
)


def create_scatter(
    session: Session,
    jurisdiction_id: int,
    subdivision_type: str,
    h_election_id: int,
    h_category: str,
    h_count: str,
    h_type: str,
    h_runoff: bool,
    v_election_id: int,
    v_category: str,
    v_count: str,
    v_type: str,
    v_runoff: str,
) -> Optional[dict]:
    """
    :param session: sqlalchemy database session
    :param jurisdiction_id: integer Id of jurisdiction in ReportingUnit database table
    :param subdivision_type: string, name of subdivision type
    :param h_election_id: for horizontal axis, integer Id of election in Election database table
    :param h_category: for horizontal axis string, name of count category, validation depends on h_type
        for h_type starting with "Population", count category is, e.g., "by Race" so that together
            h_type and h_category specify the vlue of the ExternalDataSet.Category field.
        otherwise, count category is interpreted as a CountItemType following conventions of the database
            VoteCount.CountItemType field, e.g., "total", "election-day"
    :param h_count: for horizontal axis, string describing:
        for h_type "Population", the population category, e.g., "White"
        for h_type "candidates", the candidate name
        for h_type "contests", the contest name
        for h_type "parties", the contest type, in language of either keys or values
            of constants.contest_type_mappings ("congressional" or "Congressional"; "state" or "Statewide")
    :param h_type: for horizontal axis, string describing type of count
        ("Population", "candidates", "contests" or "parties")
    :param h_runoff: for horizontal axis, True if contest is a run-off; otherwise False
    :param v_election_id: as above, but for vertical axis
    :param v_category: as above, but for vertical axis
    :param v_count: as above, but for vertical axis
    :param v_type: as above, but for vertical axis
    :param v_runoff: as above, but for vertical axis
    :return:
        if no data found, returns nothing
        if data found, returns dictionary of information for plotting:
            "jurisdiction": string, formal name of jurisdiction
            "subdivision_type:" string, type of geographic unit each point represents, typically "county"
            "x-title": string, horizontal axis title specifying the count category
                for vote counts, all but vote type
                for "Population" counts, all but ExternalDataSet.Label from database
            "y-title": same as above, for vertical axis
            "x-count_item_type": other info to specify count for horizonal axis
                for vote counts, the vote type
                for "Population" counts, the ExternalDataSet.Label
            "x-count_item_type": same as above, for vertical axis
            "x-election": string, name of horizontal axis election
            "y-election": string, name of vertical axis election
            "x": string, shorthand identifier for horizontal data
            "y": string, shorthand identifier for vertical axis
    """

    # get the mappings back to the DB labels
    h_count = ui.get_contest_type_mapping(h_count)
    v_count = ui.get_contest_type_mapping(v_count)

    dfh = get_data_for_scatter(
        session,
        jurisdiction_id,
        subdivision_type,
        h_election_id,
        h_category,
        h_count,
        h_type,
        h_runoff,
    )
    dfv = get_data_for_scatter(
        session,
        jurisdiction_id,
        subdivision_type,
        v_election_id,
        v_category,
        v_count,
        v_type,
        v_runoff,
    )
    if dfh.empty or dfv.empty:
        return None

    unsummed = pd.concat([dfh, dfv])
    jurisdiction = db.name_from_id(session, "ReportingUnit", jurisdiction_id)

    # check if there is only 1 candidate selection (with multiple count types)
    single_selection = len(unsummed["Selection"].unique()) == 1
    # check if there is only one count type
    single_count_type = len(unsummed["CountItemType"].unique()) == 1

    if (h_runoff or v_runoff) and single_selection:
        pivot_col = "Contest"
    elif single_selection and not single_count_type:
        pivot_col = "CountItemType"
    elif single_selection and single_count_type:
        pivot_col = "Election_Id"
    else:  # no runoffs, not single_selection
        pivot_col = "Selection"
    pivot_df = pd.pivot_table(
        unsummed, values="Count", index=["Name"], columns=pivot_col, aggfunc=np.sum
    ).reset_index()
    pivot_df = pivot_df.dropna()
    pivot_df.columns = pivot_df.columns.map(str)
    if pivot_df.empty:
        return None

    # package up results
    if (h_runoff or v_runoff) and single_selection:
        cols = list(pivot_df.columns)
        results = package_results(pivot_df, jurisdiction, cols[-2], cols[-1])
        results["x"] = cols[-2]
        results["y"] = cols[-1]
    elif single_selection and not single_count_type:
        results = package_results(pivot_df, jurisdiction, h_category, v_category)
        results["x"] = h_count
        results["y"] = v_count
    elif single_selection and single_count_type:
        results = package_results(
            pivot_df, jurisdiction, str(h_election_id), str(v_election_id)
        )
        results["x"] = h_count
        results["y"] = v_count
    else:  # neither is runoff; not single_selection
        results = package_results(pivot_df, jurisdiction, h_count, v_count)
    results["x-election"] = db.name_from_id(session, "Election", h_election_id)
    results["y-election"] = db.name_from_id(session, "Election", v_election_id)
    results["subdivision_type"] = subdivision_type
    results["x-count_item_type"] = h_category
    results["y-count_item_type"] = v_category
    results["x-title"] = scatter_axis_title(
        session,
        results["x"],
        results["x-election"],
        dfh.iloc[0]["Contest"],
        jurisdiction_id,
    )
    results["y-title"] = scatter_axis_title(
        session,
        results["y"],
        results["y-election"],
        dfv.iloc[0]["Contest"],
        jurisdiction_id,
    )
    h_preliminary = db.is_preliminary(session,h_election_id,jurisdiction_id)
    v_preliminary = db.is_preliminary(session,v_election_id,jurisdiction_id)
    results["preliminary"] = h_preliminary or v_preliminary

    # only keep the ones where there are an (x, y) to graph
    to_keep = []
    for result in results["counts"]:
        # need reporting unit, x, y, and x_ y_ pcts
        # otherwise it's invalid
        if len(result) == 5:
            to_keep.append(result)
    if not to_keep:
        return None

    results["counts"] = to_keep
    return results


def package_results(
        data: pd.DataFrame,
        jurisdiction: str,
        x: str,
        y: str,
        restrict: Optional[int] = None,
) -> Dict[str,Any]:
    """
    :param data: dataframe
        if "x" not equal "y", columns are "Name" (values are reporting units within the
            jurisdiction), "x" and "y" (values are vote counts)
        if "x" equals "y", columns are "Name" and a single columns of counts
    :param jurisdiction: string, formal name of jurisdiction
    :param x: string, shorthand name for first count column
    :param y: string, shorthand name for second count column
    :param restrict: (optional) integer, if given forces return of data for at most
        <restrict> subdivisions, along with data for the average of all other subdivisions
    :return: dictionary
        "jurisdiction": string, name of jurisdiction
        "x": string, shorthand name for "x" counts
        "y": string, shorthand name for "y" counts
        "counts: dictionary
            "name": string, name of geographical subdivision
            "x": number, value of "x" count
            "y": number, value of "y" count
            "x_pct": number (divide "x" count by sum of "x" and "y" counts if that sum is not 0,
                otherwise 0)
            "y_pct": number (divide "y" count by sum of "x" and "y" counts if that sum is not 0,
                otherwise 0)
    """
    results = {"jurisdiction": jurisdiction, "x": x, "y": y, "counts": []}
    if restrict and len(data.index) > restrict:
        data = get_remaining_averages(data, restrict)
    for i, row in data.iterrows():
        total = row[x] + row[y]
        if total == 0:
            x_pct = y_pct = 0
        else:
            x_pct = round(row[x] / total, 3)
            y_pct = round(row[y] / total, 3)
        results["counts"].append(
            {
                "name": row["Name"],
                "x": row[x],
                "y": row[y],
                "x_pct": x_pct,
                "y_pct": y_pct,
            }
        )
    return results


def get_data_for_scatter(
    session: Session,
    jurisdiction_id: int,
    subdivision_type: str,
    election_id: int,
    count_item_type: str,
    filter_str: str,
    count_type: str,
    is_runoff: bool,
) -> pd.DataFrame:
    """
    :param session: sqlalchemy database session
    :param jurisdiction_id: integer Id of jurisdiction in database ReportingUnit table
    :param subdivision_type: string ReportingUnitType
    :param election_id: integer Id of election in database Election table
    :param count_item_type: CountItemType characterizing vote counts
    :param filter_str: string used to filter data
        for vote counts: "All contests" or "All candidates" or a value for Contest.Name or Candidate.BallotName
        for other datasets: value for ExternalDataSet.Label
    :param count_type: string used to filter data:
        for vote counts: "candidates" or "parties" or "contests"
        for other datasets: value for ExternalDataSet.Category
    :param is_runoff: True for a run-off contest associated to the election, otherwise False

    :return: dataframe of results rolled up to ReportingUnits of <subdivision> ReportingUnitType,restricted by <count_item_type>, <filter_str> and <count_type>. Columns:
        "Election_Id"
        "Name" (ReportingUnit name)
        "Selection"
        "Contest_Id"
        "Candidate_Id"
        "Contest"
        "CountItemType"
        "Count"
    """
    if count_type.startswith("Population"):
        return get_external_data(
            session,
            jurisdiction_id,
            election_id,
            f"{count_type} {count_item_type}".strip(),  # category
            filter_str,  # Label
            subdivision_type=subdivision_type,
        )
    else:
        return get_votecount_data(
            session,
            jurisdiction_id,
            subdivision_type,
            election_id,
            count_item_type,
            filter_str,
            count_type,
            is_runoff,
        )


def get_external_data(
    session,
    jurisdiction_id,
    election_id,
    category,
    label,
    subdivision_type,
) -> pd.DataFrame:
    """
    :param session: sqlalchemy database session
    :param jurisdiction_id: integer Id of jurisdiction in database ReportingUnit table
    :param subdivision_type: string ReportingUnitType characterizing subdivisions for points in scatter
    :param category: value for ExternalDataSet.Category
    :param label: value for ExternalDataSet.Label
    :param subdivision_type: value for ReportingUnit.ReportingUnitType
    :return: dataframe of results rolled up to ReportingUnits of <subdivision> ReportingUnitType
        restricted by <category> and <label>. Format is designed to match votecount format
        Columns are:
                "Election_Id",
                "Name", (name of ReportingUnit)
                "Selection", (set to <label>)
                "Contest_Id", dummy set to 0
                "Candidate_Id", dummy set to 0
                "Contest", (set to <category>)
                "CountItemType", (set to "total")
                "Count",
    """
    # specify output columns
    cols = [
                "Election_Id",
                "Name",
                "Selection",
                "Contest_Id",
                "Candidate_Id",
                "Contest",
                "CountItemType",
                "Count",
            ]

    # get the census data
    census_df = db.read_external(
        session,
        election_id,
        jurisdiction_id,
        ["Name", "Category", "Label", "Value", "Source"],
        restrict_by_label=label,
        restrict_by_category=category,
        subdivision_type=subdivision_type,
    )

    # reshape data so it can be unioned with other results
    if not census_df.empty:
        census_df["Election_Id"] = election_id
        census_df["Contest_Id"] = 0
        census_df["Candidate_Id"] = 0
        census_df["Contest"] = category
        census_df["CountItemType"] = "total"
        census_df.rename(
            columns={"Label": "Selection", "Value": "Count"},
            inplace=True,
        )
        census_df = census_df[cols]
        return census_df
    return pd.DataFrame(columns=cols)


def get_votecount_data(
    session: Session,
    jurisdiction_id: int,
    subdivision_type: str,
    election_id: int,
    count_item_type: str,
    filter_str: str,
    count_type: str,
    is_runoff: bool,
) -> pd.DataFrame:
    """
    :param session: sqlalchemy database session
    :param jurisdiction_id: integer Id for the jurisdiction in the database ReportingUnit table
    :param subdivision_type: string ReportingUnitType
    :param election_id: integer Id for the election in the database Election table
    :param count_item_type: count item type for the vote count ("total", "absentee-mail", etc.)
    :param filter_str: string, "All contests" or "All candidates" or a contest name or a candidate name
    :param count_type: "candidates" or "parties" or "contests"
    :param is_runoff: True if contest name must contain "runoff" to be included; otherwise False
    :return: dataframe of vote counts rolled up to ReportingUnits of type <subdivision>
        (or larger, if that's all that's available), Contest,
        Selection, VoteCountType along with various database Ids
    """
    unsummed = db.unsummed_vote_counts_with_rollup_subdivision_id(
        session,
        election_id,
        jurisdiction_id,
        subdivision_type,
    )

    # limit to relevant data - runoff
    if is_runoff:
        unsummed = unsummed[unsummed["Contest"].str.contains("runoff", case=False)]
    else:
        unsummed = unsummed[~(unsummed["Contest"].str.contains("runoff", case=False))]

    # limit to relevant data - count type
    if count_type == "candidates":
        filter_column = "Selection"
    elif count_type == "contests":
        filter_column = "Contest"
    elif count_type == "parties":
        filter_str = ui.get_contest_type_mapping(filter_str)
        # create new column to filter on
        unsummed["party_district_type"] = (
            (unsummed["Party"].str.replace("Party", "")).str.strip()
            + " "
            + unsummed["contest_district_type"]
        )
        filter_column = "party_district_type"
    else:
        filter_column = ""  # to keep syntax checker happy

    # limit to relevant data - all data or not
    keep_all = filter_str.startswith("All ")
    if not keep_all:
        unsummed = unsummed[unsummed[filter_column].isin([filter_str])]
    if "party_district_type" in unsummed.columns:
        del unsummed["party_district_type"]
    unsummed = unsummed[unsummed["CountItemType"] == count_item_type]

    # cleanup for purposes of flexibility
    unsummed = unsummed[
        [
            "Election_Id",
            "ParentName",
            "Count",
            "Selection",
            "Contest_Id",
            "Candidate_Id",
            "Contest",
            "CountItemType",
        ]
    ].rename(columns={"ParentName": "Name"})

    # if All contests or All candidates
    if keep_all or count_type == "parties":
        unsummed["Selection"] = filter_str
        unsummed["Contest_Id"] = -1
        unsummed["Candidate_Id"] = -1
        unsummed["Contest"] = filter_str

    if count_type == "contests" and not keep_all:
        connection = session.bind.raw_connection()
        unsummed["Selection"] = filter_str

    columns = list(unsummed.drop(columns="Count").columns)
    summed = unsummed.groupby(columns)["Count"].sum().reset_index()

    return summed


def create_bar(
    session: Session,
    election_id: int,
    jurisdiction_id: int,
    subdivision_type: str,
    contest_district_type: Optional[str] = None,
    contest_or_contest_group: Optional[str] = None,
    for_export: bool = True,
) -> Optional[List[dict]]:

    """
    :param session: sqlalchemy database session
    :param election_id: integer Id for the election in the database Election table
    :param jurisdiction_id: integer Id for the jurisdiction in the database ReportingUnit table
    :param subdivision_type: string ReportingUnitType
    :param contest_district_type: (optional string)
    :param contest_or_contest_group: (optional string) from user-facing menu, either the name of a contest or of a
            group of contests, e.g., "All congressional"
    :param for_export: (optional)
    :return: List of dictionaries, where each dictionary contains information to create a bar
            chart. The bar charts in the list are chosen via an algorithm favoring charts with a single outlier
            county whose impact on the margin is large. Bar charts are restricted to results for the
            <contest_or_contest_group> , if given,and also from the contests with districts of type
            <contest_district_type>, if given. Details of algorithm given in an article by Singer,
            Srungavarapu & Tsai in _MAA Focus_, Feb/March 2021, pp. 10-13.
            See http://digitaleditions.walsworthprintgroup.com/publication/?m=7656&i=694516&p=10&ver=html5
    """
    unsummed = db.unsummed_vote_counts_with_rollup_subdivision_id(
        session, election_id, jurisdiction_id, subdivision_type
    )

    if contest_district_type:
        contest_district_type = ui.get_contest_type_mapping(contest_district_type)
        unsummed = unsummed[unsummed["contest_district_type"] == contest_district_type]

    # through VoteVisualizer front end, contest_type must be truthy if contest is truthy
    # Only filter when there is an actual contest passed through, as opposed to
    # "All congressional" as an example
    if contest_or_contest_group and not contest_or_contest_group.startswith("All "):
        unsummed = unsummed[unsummed["Contest"] == contest_or_contest_group]

    multiple_ballot_types = len(unsummed["CountItemType"].unique()) > 1
    groupby_cols = [
        "ReportingUnitType",
        "ParentName",
        "ParentReportingUnitType",
        "Candidate_Id",
        "CountItemType",
        "Contest_Id",
        "Contest",
        "Selection",
        "Selection_Id",
        "contest_type",
        "contest_district_type",
        "Party",
    ]
    unsummed = unsummed.groupby(groupby_cols).sum().reset_index()

    # Now process data - this is the heart of the scoring/ranking algorithm
    try:
        ranked = assign_anomaly_score(unsummed)
        ranked["margins_pct"] = ranked["Count"] / ranked["reporting_unit_total"]
        ranked_margin = ranked
        votes_at_stake = calculate_votes_at_stake(ranked_margin)
        if not for_export:
            top_ranked = get_most_anomalous(votes_at_stake, 3)
        else:
            top_ranked = votes_at_stake
    except Exception:
        return None
    if top_ranked.empty:
        return None

    # package into list of dictionary
    result_list = []
    ids = top_ranked["bar_chart_id"].unique()
    for idx in ids:
        temp_df = top_ranked[top_ranked["bar_chart_id"] == idx]
        # some cleaning here to make the pivoting work
        scores_df = temp_df[temp_df["rank"] != 1]
        scores_df = scores_df[["ReportingUnit_Id", "score", "margins_pct"]]
        scores_df.rename(
            columns={
                "score": "max_score",
                "margins_pct": "max_margins_pct",
            },
            inplace=True,
        )
        temp_df = temp_df.merge(scores_df, how="left", on="ReportingUnit_Id")
        temp_df.drop(columns=["score", "margins_pct"], inplace=True)
        temp_df.rename(
            columns={
                "max_score": "score",
                "max_margins_pct": "margins_pct",
            },
            inplace=True,
        )

        candidates = temp_df["Candidate_Id"].unique()
        x = db.name_from_id(session, "Candidate", int(candidates[0]))
        y = db.name_from_id(session, "Candidate", int(candidates[1]))
        x_party = unsummed.loc[unsummed["Candidate_Id"] == candidates[0], "Party"].iloc[
            0
        ]
        x_party_abbr = create_party_abbreviation(x_party)
        y_party = unsummed.loc[unsummed["Candidate_Id"] == candidates[1], "Party"].iloc[
            0
        ]
        y_party_abbr = create_party_abbreviation(y_party)
        jurisdiction = db.name_from_id(session, "ReportingUnit", jurisdiction_id)

        pivot_df = pd.pivot_table(
            temp_df, values="Count", index=["Name"], columns="Selection", fill_value=0
        ).reset_index()
        score_df = temp_df.groupby("Name")[
            ["score", "margins_pct", "margin_ratio"]
        ].mean()
        pivot_df = pivot_df.merge(score_df, how="inner", on="Name")
        pivot_df = sort_pivot_by_margins(pivot_df)

        if for_export:
            results = package_results(pivot_df, jurisdiction, x, y)
        else:
            results = package_results(
                pivot_df, jurisdiction, x, y, restrict=constants.max_rus_per_bar_chart)
        results["election"] = db.name_from_id(session, "Election", election_id)
        results["contest"] = db.name_from_id(
            session, "Contest", int(temp_df.iloc[0]["Contest_Id"])
        )
        results["subdivision_type"] = subdivision_type
        results["count_item_type"] = temp_df.iloc[0]["CountItemType"]

        # display votes at stake, margin info
        results["votes_at_stake_raw"] = temp_df.iloc[0]["votes_at_stake"]
        results["margin_raw"] = (
            temp_df[temp_df["rank"] == 1].iloc[0]["selection_total"]
            - temp_df[temp_df["rank"] != 1].iloc[0]["selection_total"]
        )
        votes_at_stake = human_readable_numbers(results["votes_at_stake_raw"])
        if votes_at_stake[0] == "-":
            votes_at_stake = votes_at_stake[1:]
            acted = "narrowed"
        else:
            acted = "widened"
        results["votes_at_stake"] = f"Outlier {acted} margin by ~ {votes_at_stake}"
        results["margin"] = human_readable_numbers(results["margin_raw"])
        results["preliminary"] = db.is_preliminary(session,election_id,jurisdiction_id)

        # display ballot info
        if multiple_ballot_types:
            results[
                "ballot_types"
            ] = f"""{results["jurisdiction"]} provides data by vote type"""
        else:
            results["ballot_types"] = "Data by vote type unavailable"

        # display name with party
        results["x"] = f"""{results["x"]} ({x_party_abbr})"""
        results["y"] = f"""{results["y"]} ({y_party_abbr})"""

        results["score"] = temp_df["score"].max()
        results[
            "title"
        ] = f"""{results["count_item_type"].replace("-", " ").title()} Ballots Reported"""
        download_date = db.data_file_download(session,election_id,jurisdiction_id)
        if db.is_preliminary(session,election_id,jurisdiction_id) and download_date:
            results[
                "title"
            ] = f"""{results["title"]} as of {download_date} (preliminary)"""

        result_list.append(results)
    return result_list


def assign_anomaly_score(data: pd.DataFrame) -> pd.DataFrame:
    """

    :param data: dataframe with required columns:
        "ReportingUnitType",
        "ParentReportingUnit_Id",
        "ParentName",
        "ParentReportingUnitType",
        "Candidate_Id",
        "CountItemType",
        "Contest_Id",
        "Contest",
        "Selection",
        "Selection_Id",
        "contest_type",
        "contest_district_type",
        "Count",
        and possibly other columns, such as: "Party"

    :return: dataframe obtained by appending columns to <data>:
        "score": value between 0 and 1 (1 is more anomalous) indicating, among all reporting units for the Contest,
            how anomalous the vote share is between the Selection and the contest winner. Note that winner always
            scores 0, as vote share of a selection with itself is always (.5,.5)
        "bar_chart_id": identifies the set of vote counts within which the anomaly score of the single vote count was
            calculated. A single vote count's anomaly score depends on the set of vote counts within which it is
            considered.
        "reporting_unit_total": total votes for all selections in given contest for given reporting unit
        "selection_total": total votes for given selection in given contest in entire jurisdiction
        "rank": 1 for contest winner, 2 for second place, etc.
        "contest_total": total votes cast in the contest in entire jurisdiction
        "index": integer internal id denoting the set over which the anomaly score is calculated
        "bar_chart_id_tmp": artifact from calculation
    and preserving columns
        "ReportingUnit_Id" (renamed from "ParentReportingUnit_Id")
        "Name", (renamed from "ParentName", the name of the ReportingUnit)
        "ReportingUnitType", (renamed from "ParentReportingUnitType")
        "Candidate_Id",
        "CountItemType",
        "Contest_Id",
        "Contest",
        "Selection",
        "contest_type",
        "contest_district_type",
        "Count",
        "Selection_Id",
     """

    # Assign a ranking for each candidate by votes for each contest

    # # create <total_data> dataframe with "total" CountItemType only
    if "total" not in data["CountItemType"].unique():
        groupby_cols = list(data.columns)
        groupby_cols.remove("Count")
        total_data = data.groupby(groupby_cols).sum().reset_index()
    else:
        total_data = data[data["CountItemType"] == "total"]

    # # create <ranked_df> of contest-candidate pairs,
    # column "rank": winners 1, second-place 2, etc. (tied candidates get same rank);
    # column "selection_total" with total votes for candidate;
    # column "contest_total" with total votes in contest

    # # # Append total votes for selection
    ranked_df = (
        total_data.groupby(["Contest_Id", "Selection", "Selection_Id"], as_index=False)[
            "Count"
        ]
        .sum()
        .sort_values(["Contest_Id", "Count"], ascending=False)
    )
    # # # Append rank
    ranked_df["rank"] = ranked_df.groupby("Contest_Id")["Count"].rank(
        "dense", ascending=False
    )
    ranked_df.rename(columns={"Count": "selection_total"}, inplace=True)

    # # # Append total votes for the entire contest
    contest_df = ranked_df.groupby("Contest_Id")["selection_total"].sum().reset_index()
    contest_df.rename(columns={"selection_total": "contest_total"}, inplace=True)
    ranked_df = ranked_df.merge(contest_df, how="inner", on="Contest_Id")

    # Group data by parent info. This works because each child is also its own
    # parent in the DB table
    grouped_df = (
        data.groupby(
            [
                "ParentReportingUnit_Id",
                "ParentName",
                "ParentReportingUnitType",
                "Candidate_Id",
                "CountItemType",
                "Contest_Id",
                "Contest",
                "Selection",
                "contest_type",
                "contest_district_type",
            ],
            as_index=False,
        )["Count"]
        .sum()
        .reset_index()
    )
    grouped_df.drop(columns="index", inplace=True)
    grouped_df.rename(
        columns={
            "ParentReportingUnit_Id": "ReportingUnit_Id",
            "ParentName": "Name",
            "ParentReportingUnitType": "ReportingUnitType",
        },
        inplace=True,
    )
    grouped_df = grouped_df.merge(
        ranked_df, how="inner", on=["Contest_Id", "Selection"]
    )

    # assign temporary bar_chart_ids to unique combination of contest,
    # ru_type, and count type. These will be updated later to account
    # for 2 candidate pairings
    df_unit = grouped_df[
        ["Contest_Id", "ReportingUnitType", "CountItemType"]
    ].drop_duplicates()
    df_unit = df_unit.reset_index()
    df_unit["bar_chart_id_tmp"] = df_unit.index
    df_with_units = grouped_df.merge(
        df_unit, how="left", on=["Contest_Id", "ReportingUnitType", "CountItemType"]
    )

    # loop through each unit ID and assign anomaly scores
    # also update the "real" bar_chart_id which takes into account pairing of candidates
    bar_chart_ids_tmp = df_with_units["bar_chart_id_tmp"].unique()
    bar_chart_id = 0 # increments on each pass through for loop
    df = pd.DataFrame() # collects records on each pass through for loop
    # for each unit ID
    for bar_chart_id_tmp in bar_chart_ids_tmp:
        # grab all the data there
        temp_df = df_with_units[df_with_units["bar_chart_id_tmp"] == bar_chart_id_tmp]
        for i in range(2, int(temp_df["rank"].max()) + 1):
            selection_df = temp_df[temp_df["rank"].isin([1, i])].copy()
            selection_df["bar_chart_id"] = bar_chart_id
            bar_chart_id += 1
            total = (
                selection_df.groupby("ReportingUnit_Id")["Count"].sum().reset_index()
            )
            total.rename(columns={"Count": "reporting_unit_total"}, inplace=True)
            selection_df = selection_df.merge(total, how="inner", on="ReportingUnit_Id")
            if selection_df.shape[0] >= 12 and len(selection_df["rank"].unique()) > 1:
                pivot_df = (
                    pd.pivot_table(
                        selection_df,
                        values="Count",
                        index=["ReportingUnit_Id", "reporting_unit_total"],
                        columns="Selection",
                    )
                    .sort_values("ReportingUnit_Id")
                    .reset_index()
                )
                pivot_df = pivot_df[pivot_df["reporting_unit_total"] > 0]
                pivot_df_values = pivot_df.drop(
                    columns=["ReportingUnit_Id", "reporting_unit_total"]
                )
                to_drop = [
                    selection_df[selection_df["rank"] == 1]["Selection"].unique()[0],
                    selection_df[selection_df["rank"] == i]["Selection"].unique()[0],
                ]
                # pass in proportions instead of raw vlaues
                vote_proportions = pivot_df_values.div(
                    pivot_df["reporting_unit_total"], axis=0
                )
                np.nan_to_num(vote_proportions, copy=False)
                # assign z score and then add back into final DF
                scored = euclidean_zscore(vote_proportions.to_numpy())
                pivot_df["score"] = scored
                pivot_df = pivot_df[["ReportingUnit_Id", to_drop[1], "score"]]
                pivot_df["Selection"] = to_drop[1]
                pivot_df.rename(columns={to_drop[1]: "Count"}, inplace=True)
                scored_df = selection_df.merge(
                    pivot_df, how="left", on=["ReportingUnit_Id", "Selection", "Count"]
                )
                df = pd.concat([df, scored_df])
    if "score" in df.columns:
        df["score"] = df["score"].fillna(0)
    return df


def get_most_anomalous(data: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    :param data: dataframe with required columns:
        "margin_ratio": number of votes at stake divided by overall contest margin between the two candidates
        "score": anomaly z-score for the given ReportingUnit_Id within the given bar chart
            (specified by bar_chart_id)
        "Contest_Id":
        "ReportingUnit_Id":
        "ReportingUnitType":
        "CountItemType":
        "Count":


        "Name": (name of reporting unit)
        "Candidate_Id",
        "Contest":
        "Selection": 
        "contest_type": "BallotMeasure" or "Candidate"
        "contest_district_type": ReportingUnitType for contest district
        "Selection_Id":
        "selection_total": total votes for given selection in given contest in entire jurisdiction
        "rank": candidate rank within contest
        "contest_total": number of votes for all candidates in the given contest, over entire district
        "index": 
        "bar_chart_id_tmp": artifact from calculation
        "bar_chart_id": internal integer id identifying the set of points within which the anomaly score was calculated
        "reporting_unit_total": number of votes for all candidates in the given contest and reporting unit
        "margins_pct":
        "votes_at_stake": number of votes that would change if anomaly were brought in line with nearest point (see http://digitaleditions.walsworthprintgroup.com/publication/?m=7656&i=694516&p=10&ver=html5)

    :param n: integer, number of anomalous datasets to return

    :return: dataframe
    """

    """Gets n contests, with <n>-1 from largest votes at stake ratio
    and 1 with largest score. If <n>-1 from votes at stake cannot be found
    (bc of threshold for score) then we fill in the top n from scores"""
    # filter out very small votes at stake (relative to total contest margin)
    data = data[(data["margin_ratio"] > 0.01) | (data["margin_ratio"] < -0.01)]

    # identify bar charts with significant outliers (z-score above constants.outlier_zscore_cutoff)
    # get ordering of sufficiently anomalous bar charts (descending by votes-at-stake-to-margin ratio)
    # and ordering by descending z-score
    margin_data = data[data["score"] > constants.outlier_zscore_cutoff]
    bar_charts_by_margin = bar_chart_ids_by_column_value(margin_data,"margin_ratio")
    bar_charts_by_score = bar_chart_ids_by_column_value(data,"score")

    # pick top n bar charts: up to n-1 from margin data if there are enough, and the rest
    #  from z-score
    bar_chart_ids_all = bar_charts_by_margin[0 : n - 1] + bar_charts_by_score
    bar_chart_ids = list(dict.fromkeys(bar_chart_ids_all).keys())[0:n]
    data = data[data["bar_chart_id"].isin(bar_chart_ids)]

    # Drop from <data> any rows corresponding to contest-reportingunit-countitemtype tuples with no votes
    zeros_df = data[
        [
            "Contest_Id",
            "ReportingUnitType",
            "CountItemType",
            "ReportingUnit_Id",
            "Count",
        ]
    ]
    zeros_df = zeros_df.groupby(
        ["Contest_Id", "ReportingUnitType", "ReportingUnit_Id", "CountItemType"]
    ).sum()
    zeros_df = zeros_df.reset_index()
    no_zeros = zeros_df[zeros_df["Count"] != 0]
    data = data.merge(
        no_zeros,
        how="inner",
        on=["Contest_Id", "ReportingUnitType", "ReportingUnit_Id", "CountItemType"],
    )
    data.rename(columns={"Count_x": "Count"}, inplace=True)
    data.drop(columns=["Count_y"], inplace=True)

    data.sort_values(
        by=["margin_ratio", "Name", "Selection"],
        ascending=[False, True, True],
        inplace=True,
    )

    # now we get the top reporting unit IDs, in terms of anomaly score, of the winner and most anomalous (number of reportingunit IDs is constants.max_rus_per_bar_chart
    ids = data["bar_chart_id"].unique()
    df = pd.DataFrame()
    for idx in ids:
        temp_df = data[data["bar_chart_id"] == idx]
        max_score = temp_df["score"].max()
        if max_score > 0:
            rank = temp_df[temp_df["score"] == max_score].iloc[0]["rank"]
            temp_df = temp_df[temp_df["rank"].isin([1, rank])]
            df = pd.concat([df, temp_df])
    return df


def euclidean_zscore(li: List[List[float]]) -> List[float]:
    """Take a list of vectors -- all in the same R^k,
    returns a list of the z-scores of the vectors -- each relative to the ensemble"""
    distance_list = [sum([dist.euclidean(item, y) for y in li]) for item in li]
    if len(set(distance_list)) == 1:
        # if all distances are the same, which yields z-score nan values
        return [0] * len(li)
    else:
        return list(stats.zscore(distance_list))


def calculate_votes_at_stake(data: pd.DataFrame) -> pd.DataFrame:
    """
    :param data: dataframe with required columns
        "ReportingUnit_Id": 
        "Count" 
        "selection_total" 
        "bar_chart_id" (records with same bar_chart_id belong to a single bar chart plot, i.e., one pair of
            candidates and one vote type)
        "score"
        "Selection"
        "margins_pct"
        "reporting_unit_total"
        "rank"

    :return: dataframe with all records from <data> (row order not necessarily preserved), with additional
        columns (constant over all records with same bar_chart_id):
         "votes_at_stake": # of votes that would change if the outlier ReportingUnit behaved like its nearest neighbor
         "margin_ratio": ratio of votes_at_stake to overall contest margin between the two selections in the bar chart
    """
    df = pd.DataFrame()
    bar_chart_ids = data["bar_chart_id"].unique()
    for bar_chart_id in bar_chart_ids:
        # create dataframe of data from that one bar chart
        one_chart_df = data[data["bar_chart_id"] == bar_chart_id].copy()
        try:
            # get a df of the most anomalous reporting_unit/candidate combination
            # # find index of a record with maximum score. Note that
            # # contest winners always have score 0,
            # # and others' scores are relative to the winner, so non-winners
            # # always score higher than winners (in one-winner contests, anyway).
            max_score = one_chart_df["score"].max()
            index = one_chart_df.index[one_chart_df["score"] == max_score][0]

            # # define outlier by restricting to 2 records:
            # # the one with the max score, and the winner, both with same (outlier) ReportingUnit
            reporting_unit_id = one_chart_df.loc[index, "ReportingUnit_Id"]
            selection = one_chart_df.loc[index, "Selection"]
            margin_pct = one_chart_df.loc[index, "margins_pct"]
            reporting_unit_total = one_chart_df.loc[index, "reporting_unit_total"]
            outlier_df = (
                one_chart_df[
                    (one_chart_df["ReportingUnit_Id"] == reporting_unit_id)
                    & (
                        (one_chart_df["score"] == max_score)
                        | (one_chart_df["rank"] == 1) # note OR here
                        & (one_chart_df["reporting_unit_total"] == reporting_unit_total)
                    )
                ]
                .sort_values("rank", ascending=False)
                .drop_duplicates()
            )

            # Create dataframe with records for the ReportingUnit closest to the outlier in terms of margins
            # ("closest neighbor")
            # # rule out the outlier reporting unit
            filtered_df = one_chart_df[
                (one_chart_df["ReportingUnit_Id"] != reporting_unit_id)
                & (one_chart_df["Selection"] == selection)
            ]
            # find index of the closest margin on either side (+/-)
            next_index = filtered_df.iloc[
                (filtered_df["margins_pct"] - margin_pct).abs().argsort()[:1]
            ].index[0]
            next_reporting_unit_id = one_chart_df.loc[next_index, "ReportingUnit_Id"]
            next_margin_pct = one_chart_df.loc[next_index, "margins_pct"]
            next_reporting_unit_total = one_chart_df.loc[next_index, "reporting_unit_total"]
            next_anomalous_df = (
                one_chart_df[
                    (one_chart_df["ReportingUnit_Id"] == next_reporting_unit_id)
                    & (
                        (one_chart_df["margins_pct"] == next_margin_pct)
                        | (one_chart_df["rank"] == 1)
                        & (one_chart_df["reporting_unit_total"] == next_reporting_unit_total)
                    )
                ]
                .sort_values("rank", ascending=False)
                .drop_duplicates()
            )

            # move the outlier pct vote share to the closest neighbor's pct vote share,
            # holding constant the number of votes in the neighbor reporting unit,
            # calculate what the (signed) change to the Contest margin would be,
            # store that change in a new column called "votes_at_stake"
            # and store the ratio of votes at stake to the margin in new "margin_ratio" column
            winner_bucket_total = int(outlier_df[outlier_df["rank"] == 1]["Count"])
            not_winner_bucket_total = int(
                outlier_df[outlier_df["rank"] != 1]["Count"]
            )
            reported_bucket_total = int(outlier_df["Count"].sum())
            next_bucket_total = int(next_anomalous_df["Count"].sum())
            adj_margin = (
                next_anomalous_df[next_anomalous_df["rank"] != 1].iloc[0]["Count"]
                / next_bucket_total
            )
            not_winner_adj_bucket_total = int(reported_bucket_total * adj_margin)
            winner_adj_bucket_total = (
                reported_bucket_total - not_winner_adj_bucket_total
            )

            # # calculate margins by raw numbers for the bucket
            contest_margin = winner_bucket_total - not_winner_bucket_total
            adj_contest_margin = winner_adj_bucket_total - not_winner_adj_bucket_total

            # # calculate margins by raw numbers for the entire contest
            contest_margin_ttl = (
                outlier_df[outlier_df["rank"] == 1].iloc[0]["selection_total"]
                - outlier_df[outlier_df["rank"] != 1].iloc[0]["selection_total"]
            )
            one_chart_df["votes_at_stake"] = contest_margin - adj_contest_margin
            one_chart_df["margin_ratio"] = one_chart_df["votes_at_stake"] / contest_margin_ttl
        except Exception:
            one_chart_df["margin_ratio"] = 0
            one_chart_df["votes_at_stake"] = 0
        df = pd.concat([df, one_chart_df])
    return df


def create_candidate_contests(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    contest_df = (
        df["VoteCount"]
        .merge(df["Contest"], how="left", left_on="Contest_Id", right_index=True)
        .rename(columns={"Name": "Contest", "Id": "ContestSelectionJoin_Id"})
        .merge(
            df["CandidateSelection"],
            how="left",
            left_on="Selection_Id",
            right_index=True,
        )
        .merge(df["Candidate"], how="left", left_on="Candidate_Id", right_index=True)
        .rename(columns={"BallotName": "Selection"})
        .merge(
            df["CandidateContest"], how="left", left_on="Contest_Id", right_index=True
        )
        .merge(df["Office"], how="left", left_on="Office_Id", right_index=True)
    )
    contest_df = contest_df[columns]
    if contest_df.empty:
        contest_df["contest_type"] = None
    else:
        contest_df = m.add_constant_column(contest_df, "contest_type", "Candidate")
    return contest_df


def create_ballot_measure_contests(
    df: pd.DataFrame, columns: List[str]
) -> pd.DataFrame:
    ballotmeasure_df = (
        df["ContestSelectionJoin"]
        .merge(
            df["BallotMeasureContest"],
            how="right",
            left_on="Contest_Id",
            right_index=True,
        )
        .rename(columns={"Name": "Contest"})
        .merge(
            df["BallotMeasureSelection"],
            how="left",
            left_on="Selection_Id",
            right_index=True,
        )
    )
    ballotmeasure_df = ballotmeasure_df[columns]
    if ballotmeasure_df.empty:
        ballotmeasure_df["contest_type"] = None
    else:
        ballotmeasure_df = m.add_constant_column(
            ballotmeasure_df, "contest_type", "BallotMeasure"
        )
    return ballotmeasure_df


def bar_chart_ids_by_column_value(data: pd.DataFrame,column: str) -> List[int]:
    """
    Given a dataframe of results, return a list of unique bar_chart_ids
    that are sorted in desc order by the column's value
    :param data: dataframe with required columns "bar_chart_id" and <column>
    :param column: name of a column
    :return: list of unique bar_chart_ids values sorted in descending order by max of <column>'s value
        among all rows with the given bar_chart_id. I.e., first item in list will be the bar_chart_id of
        the row of <data> with the highest value of <column>; second will be the bar_chart_id of the
        row of <data> with the highest value (among all rows with a bar_chart_id different from the first item)
         of <column>.
    """

    data = data[["bar_chart_id", column]]
    data = data.groupby("bar_chart_id").max(column).sort_values(by=column, ascending=False)
    data = data.reset_index()
    return list(data["bar_chart_id"].unique())


def human_readable_numbers(value: float) -> str:
    abs_value = abs(value)
    if abs_value < 10:
        return str(value)
    elif abs_value < 100:
        return str(round(value, -1))
    elif abs_value < 1000:
        return str(round(value, -2))
    else:
        return "{:,}".format(round(value, -3))


def sort_pivot_by_margins(df: pd.DataFrame) -> pd.DataFrame:
    """
    :param df: Dataframe with required columns "score" and "margins_pct"
    :return: dataframe whose first row is a row of <df> with the maximum value of "score",
        and whose other rows are the remaining rows of <df>, sorted by the value of "margins_pct" follows:
            if the "margins_pct" value for the first row is less than max of the others', ascending order
            otherwise, in descending order
    """
    # create an DF of just the most anomalous row
    i = df.index[df["score"] == df["score"].max()][0]
    anomalous_df = df.iloc[i, :].to_frame().transpose().reset_index(drop=True)

    # get the rest of the rows and sort based on margins
    remainder_df = df.drop(index=i)
    sort_ascending = (
        anomalous_df.iloc[0, anomalous_df.columns.get_indexer(["margins_pct"])[0]]
        < remainder_df["margins_pct"].max()
    )
    remainder_df = remainder_df.sort_values(
        "margins_pct", ascending=sort_ascending
    ).reset_index(drop=True)
    remainder_df.index = remainder_df.index + 1

    return pd.concat([anomalous_df, remainder_df])


def get_remaining_averages(df: pd.DataFrame, restrict: int) -> pd.DataFrame:
    """
    :param df: dataframe, required column "Name", all other columns must be interpretable as numeric
    :param restrict: number of rows to preserve as is
    :return: dataframe whose first <restrict> rows match <df>'s, and with one other row
        with "Name" value "Average of all others", with each numeric column containing the average of
        the corresponding values in the omitted rows.
    """

    """Take a dataframe and keep a number of the rows as-is up to the restrict
    number. The remaining rows get aggregated, either summed by the vote counts
    or averaged for the other metrics."""
    columns = df.columns.to_list()
    columns.remove("Name")
    df[columns] = df[columns].apply(pd.to_numeric)

    actual_df = df.iloc[0 : restrict - 1, :]
    average_df = df.iloc[restrict - 1 :, :].copy()

    average_df["Name"] = "Average of all others"
    average_df = average_df.groupby("Name").mean().reset_index()
    average_df.index = [restrict - 1]
    return pd.concat([actual_df, average_df])


def create_party_abbreviation(party):
    if party.strip().lower().startswith("none "):
        return "N/A"
    else:
        return (party.strip())[0].upper()


def dedupe_scatter_title(category: str, election: str, contest:str):
    """
    :param category:
    :param election:
    :param contest:
    :return: title string combining <category>, <election> and <contest>, removing any redundancy.
    """

    title = f"{category} - {election}"
    if category != contest:
        title = f"{title} - {contest}"
    return title


def scatter_axis_title(
    session: Session,
    label: str,
    election: str,
    contest_or_external_category: str,
    jurisdiction_id: int,
) -> str:
    if contest_or_external_category.startswith("Population"):
        election_id = db.name_to_id(session, "Election", election)
        # get the actual year of data and source of data
        df = db.read_external(
            session,
            election_id,
            jurisdiction_id,
            ["Year", "Source"],
            restrict_by_category=contest_or_external_category,
            restrict_by_label=label,
        )
        data_year = df.iloc[0]["Year"]
        data_source = df.iloc[0]["Source"]
        return f"{data_year} {contest_or_external_category} - {label}"
    else:
        title = dedupe_scatter_title(label, election, contest_or_external_category)
        return ui.get_contest_type_display(title)


def nist_candidate_contest(session, election_id, jurisdiction_id):
    """return all the candidate contest info, including info related to
    the contest, the selection, and the actual vote counts themselves"""
    vote_count_df = db.read_vote_count(
        session,
        election_id=election_id,
        jurisdiction_id=jurisdiction_id,
        fields=[
            "VoteCount_Id",
            "ReportingUnit_Id",
            "CountItemType",
            "Count",
            "Selection_Id",
        ],
        aliases=["Id", "GpUnitId", "CountItemType", "Count", "Selection_Id"],
    )

    selection_df = db.read_vote_count(
        session,
        election_id=election_id,
        jurisdiction_id=jurisdiction_id,
        fields=["Selection_Id", "Party_Id", "Candidate_Id", "Contest_Id"],
        aliases=["Id", "PartyId", "CandidateId", "ContestId"],
    )

    contest_df = db.read_vote_count(
        session,
        election_id=election_id,
        jurisdiction_id=jurisdiction_id,
        fields=[
            "Contest_Id",
            "ContestName",
            "ContestType",
            "Office_Id",
            "ElectionDistrict_Id",
        ],
        aliases=["Id", "ContestName", "ContestType", "OfficeId", "ElectionDistrictId"],
    )
    contest_df = contest_df[contest_df["ContestType"] == "Candidate"]
    contest_df["Type"] = "CandidateContest"
    contest_df.drop(columns="ContestType", inplace=True)

    result = contest_df.to_json(orient="records")
    contests = json.loads(result)
    # for each contest, get the selections
    for contest in contests:
        contest["BallotSelection"] = []
        contest_id = contest["Id"]
        tmp_selection_df = selection_df[selection_df["ContestId"] == contest_id]
        selection_ids = tmp_selection_df["Id"].unique()

        # for each selection, get the selection/candidate info and vote count info
        for selection_id in selection_ids:
            # first the selection info
            selection_result_df = tmp_selection_df[
                tmp_selection_df["Id"] == selection_id
            ][["Id", "CandidateId"]].drop_duplicates()
            selection_result_df["Type"] = "CandidateSelection"
            selection_result = json.loads(
                selection_result_df.to_json(orient="records")
            )[0]

            # then the votecount info
            vote_count_by_selection_df = vote_count_df[
                vote_count_df["Selection_Id"] == selection_id
            ][["GpUnitId", "CountItemType", "Count"]]
            vote_count_result = vote_count_by_selection_df.to_json(orient="records")
            selection_result["VoteCounts"] = json.loads(vote_count_result)
            contest["BallotSelection"].append(selection_result)

    return contests


def nist_reporting_unit(session, election_id, jurisdiction_id):
    """A ReportingUnit is a GPUnit"""
    df = db.read_vote_count(
        session,
        election_id=election_id,
        jurisdiction_id=jurisdiction_id,
        fields=["GP_Id", "GPReportingUnitName", "GPType"],
        aliases=["Id", "Name", "ReportingUnitType"],
    )
    df["Type"] = "ReportingUnit"
    result = df.to_json(orient="records")
    return json.loads(result)


def nist_party(session, election_id, jurisdiction_id):
    df = db.read_vote_count(
        session,
        election_id=election_id,
        jurisdiction_id=jurisdiction_id,
        fields=["Party_Id", "PartyName"],
        aliases=["Id", "Name"],
    )
    result = df.to_json(orient="records")
    return json.loads(result)


def nist_election(session, election_id, jurisdiction_id):
    df = db.read_vote_count(
        session,
        election_id=election_id,
        jurisdiction_id=jurisdiction_id,
        fields=["Election_Id", "ElectionName", "ElectionType"],
        aliases=["Id", "Name", "Type"],
    )
    # Currently all our elections are at the state level.
    # Once we start handling local elections, this might need to be updated.
    df["ReportingUnit"] = jurisdiction_id
    # we also do not collect start date or end date of election at the moment
    df["StartDate"] = "uncollected"
    df["EndDate"] = "uncollected"
    result = df.to_json(orient="records")
    return json.loads(result)


def nist_office(session, election_id, jurisdiction_id):
    df = db.read_vote_count(
        session,
        election_id=election_id,
        jurisdiction_id=jurisdiction_id,
        fields=["Office_Id", "OfficeName", "ElectionDistrict_Id", "NumberElected"],
        aliases=["Id", "Name", "ElectoralDistrictId", "NumberElected"],
    )
    result = df.to_json(orient="records")
    return json.loads(result)


def nist_candidate(session: Session, election_id: int, jurisdiction_id: int):
    df = db.read_vote_count(
        session,
        election_id=election_id,
        jurisdiction_id=jurisdiction_id,
        fields=["Candidate_Id", "BallotName", "Party_Id"],
        aliases=["Id", "BallotName", "PartyId"],
    )
    result = df.to_json(orient="records")
    return json.loads(result)


def rollup_dataframe(
    session: Session,
    df: pd.DataFrame,
    count_col: str,
    ru_id_column: str,
    new_ru_id_column: str,
    rollup_rut: str = constants.default_subdivision_type,
    ignore: Optional[List[str]] = None,
) -> (pd.DataFrame(), Optional[dict]):
    """
    :param session: sqlalchemy database session
    :param df: dataframe of results
    :param count_col: string, name of column with counts
    :param ru_id_column: string, name of column with database Ids of ReportingUnits
    :param new_ru_id_column: string, name of column in returned dataframe with database Ids
        of newly rolled-up ReportingUnits
    :param rollup_rut: string, ReportingUnitType to roll up to (e.g., "county")
    :param ignore: (optional) list of names of columns to drop from <df>
    :return:
        dataframe of results rolled up to the given ReportingUnitType (NB: for reporting units without
        parents of the given type (e.g., sometimes absentee votes are reported by state), preserve the reporting unit
        dictionary of errors and warnings
    """

    err = None

    # drop from dataframe any columns in <ignore>
    if ignore:
        working = df.copy().drop(ignore, axis=1)
    else:
        working = df.copy()

    group_cols = [c for c in working.columns if (c not in (ru_id_column, count_col))]
    parents, err_str = db.parents(
        session, df[ru_id_column].unique(), subunit_type=rollup_rut
    )
    if err_str:
        err = ui.add_new_error(
            err,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Unable to read parents reporting unit info from column {ru_id_column}",
        )
        return pd.DataFrame(), err
    try:
        new_working = (
            working.reset_index()
            .merge(parents, how="left", left_on=ru_id_column, right_on="child_id")
            .set_index("index")[group_cols + ["parent_id", count_col]]
        )
    except KeyError as ke:
        err = ui.add_new_error(
            err,
            "system",
            f"{Path(__file__).absolute().parents[0].name}.{inspect.currentframe().f_code.co_name}",
            f"Unexpected error while merging dataframes to capture nesting relationships of ReportingUnits: {ke}",
        )
        return pd.DataFrame(), err

    # if no parent is found (e.g., for reporting unit that is whole state), keep the original
    mask = new_working.parent_id.isnull()
    if mask.any():
        new_working.loc[mask, "parent_id"] = working.loc[mask, ru_id_column]

    rollup_df = (
        new_working.fillna("")
        .groupby(group_cols + ["parent_id"])
        .sum(count_col)
        .reset_index()
        .rename(columns={"parent_id": new_ru_id_column})
    )

    return rollup_df, err
