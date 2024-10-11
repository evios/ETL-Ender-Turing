from ETL.Extract import EnderTuringAPIBaseDicts, EnderTuringAPISessionsData
import pandas as pd
import numpy as np

from ETL.utils import safe_to_datetime, unicode_to_utf8
from settings import settings, logger


class EnderTuringAPIBaseDictsTransformed(EnderTuringAPIBaseDicts):
    category_labels: list[dict]
    scorecard_categories: list[dict]
    scorecard_points: list[dict]
    tag_labels: list[dict]


class EnderTuringAPISessionsDataTransformed(EnderTuringAPISessionsData):
    pass


et_default_user = {
    'id': 0,
    'full_name': 'Ender Turing',
    'email': 'ender.turing@enderturing.com',
    'is_active': False,
    'is_superuser': False,
    'invite_expires': '1900-01-01T00:00:00.000'
}


def enrich_df_et(et, df):
    # make readable Agent/Team names instead of IDs
    agents = et.agent.get_agents()
    agents = {a["id"]: a["name"] for a in agents}
    teams = et.agent.get_groups()
    teams = {t["id"]: t["name"] for t in teams}
    df["agent_name"] = df["agent_id"].apply(lambda x: agents[x])
    df["group_name"] = df["group_id"].apply(lambda x: teams[x])
    # split date-time into two columns
    df[['start_dt_date', 'start_dt_time']] = df['start_dt'].str.split('T', expand=True)
    df['start_dt_time'] = df['start_dt_time'].apply(lambda x: x[:5])
    # add ET link
    df["et_link"] = f"{et.http_client.config.url}/main/conversations/view?session_id=" + df["id"]

    # fetch additional_info (not in main sessions' endpoint) - need for ticket_system_id
    for idx, row in df.iterrows():
        # additional_info
        session_meta = et.sessions.get_session(session_id=row["id"])
        df.at[idx, "additional_info"] = session_meta.get("additional_info")
        if idx % 100 == 0:
            print(f"Done: {idx} of {len(df)}")

    return df


def transform_base_dicts(et_data: EnderTuringAPIBaseDicts) -> EnderTuringAPIBaseDictsTransformed:
    logger.info(f"--- STEP 2. Transform data from Ender Turing to DB format ---")
    logger.info(f"Transforming Base Dictionaries")

    logger.info(f"Convert each dictionary to dataframe")
    # convert each dictionary to dataframe for transformations easiness
    df_et_data = {}
    for dict_name in et_data.keys():
        if et_data[dict_name]:
            df_et_data[dict_name] = pd.DataFrame(et_data[dict_name])

    logger.info(f"Flattening columns containing lists -> into separate table")
    logger.debug(f"To Flatten Agents first record: %s", et_data["agents"][0])
    # Transform 'agents' -> separate 'agent_group_associations'
    df_et_data["agent_group_associations"] = pd.json_normalize(
        et_data["agents"], record_path="groups", meta=["id"], meta_prefix="agent_"
    )
    # remove original 'groups' from 'agents' as we will load it to DB in separate table
    df_et_data["agents"].drop(columns=["groups"], inplace=True)

    # Transform 'categories' -> separate 'category_labels'
    # normalize and store 'category_labels' in separate dict prepared to load to separate DB table
    logger.debug(f"To Flatten Categories first record: %s", et_data["categories"][0])
    try:
        df_et_data["category_labels"] = pd.json_normalize(
            et_data["categories"],
            record_path="labels",
            record_prefix="label_",
            meta=["id"],
            meta_prefix="category_"
        )[["category_id", "label_id"]]
    except KeyError:
        logger.warning("No labels created, skipping..")
    # remove original 'labels' from 'categories' as we will load it to DB in separate table
    df_et_data["categories"].drop(columns=["labels"], inplace=True)

    # Transform 'scorecards' -> separate 'scorecard_categories' and 'scorecard_points'
    logger.debug(f"To Flatten Scorecards first record: %s", et_data["scorecards"][0])
    df_et_data["scorecard_categories"] = pd.json_normalize(et_data["scorecards"],
                                                           record_path="categories")
    df_et_data["scorecard_points"] = pd.json_normalize(
        df_et_data["scorecard_categories"].to_dict(orient='records'), record_path="points"
    )
    # remove original 'categories' from 'scorecards' as we will load it to DB in separate table
    df_et_data["scorecards"].drop(columns=["categories"], inplace=True)
    df_et_data["scorecard_categories"].drop(columns=["points"], inplace=True)

    # Transform 'tags' -> separate 'tag_labels'
    logger.debug(f"To Flatten Tags first record: %s", et_data["tags"][0])
    try:
        df_et_data["tag_labels"] = pd.json_normalize(
            et_data["tags"],
            record_path="labels",
            record_prefix="label_",
            meta=["id"],
            meta_prefix="tag_"
        )[["tag_id", "label_id"]]
    except KeyError:
        logger.warning("No labels created, skipping..")
    # remove original 'labels' from 'tags' as we will load it to DB in separate table
    df_et_data["tags"].drop(columns=["labels"], inplace=True)

    # Transform 'users' -> add Default Ender Turing user if absent
    if not et_data["users"]:
        # users were not fetched
        df_et_data["users"] = pd.DataFrame(columns=['invite_expires', 'role_ids', 'permissions'])
    else:
        logger.debug(f"To Flatten Users first record: %s", et_data["users"][0])
        if 0 not in df_et_data["users"]['id'].values:
            # Create default Ender Turing user if absent in API return
            df_et_data["users"] = pd.concat(
                [df_et_data["users"], pd.DataFrame([et_default_user])], ignore_index=True
            ).reset_index(drop=True)

    logger.info(f"Parsing DateTimes")
    # DateTime-like columns -> Convert to DateTimes objects acceptable by DB
    df_et_data["agents"]["deactivated_at"] = pd.to_datetime(df_et_data["agents"]["deactivated_at"]).dt.round('s')
    df_et_data["categories"]["created_at"] = pd.to_datetime(df_et_data["categories"]["created_at"]).dt.round('s')
    df_et_data["categories"]["updated_at"] = pd.to_datetime(df_et_data["categories"]["updated_at"]).dt.round('s')
    df_et_data["tags"]["archived_at"] = pd.to_datetime(df_et_data["tags"]["archived_at"]).dt.round('s')
    df_et_data["users"]["invite_expires"] = pd.to_datetime(df_et_data["users"]["invite_expires"]).dt.round('s')
    # Replace out-of-bounds dates with a default value (e.g., pd.NaT, or pd.Timestamp.min)
    # ET has # '0001-01-01T00:00:00' as default start date of Agent in Group
    df_et_data["agent_group_associations"]["start_dt"] = df_et_data["agent_group_associations"][
        "start_dt"].apply(safe_to_datetime, default_value=pd.Timestamp('1900-01-01'))

    logger.info(f"Remove unneeded for DWH case columns")
    # Controlled Remove unneeded for DWH case columns,
    # to log errors on some new data appeared tht is not in scheme, hence silently skipped from Load
    df_et_data["agents"].drop(columns=["user", "reactions", "phone_number_aliases"], inplace=True)
    df_et_data["groups"].drop(columns=["additional_scorecards"], inplace=True)
    df_et_data["labels"].drop(columns=["color"], inplace=True)
    df_et_data["scorecards"].drop(columns=["team_ids"], inplace=True)
    df_et_data["scorecard_points"].drop(columns=["score_values", "user_data"], inplace=True)
    df_et_data["tags"].drop(columns=["words", "phrases", "color"], inplace=True)
    df_et_data["users"].drop(columns=["role_ids", "permissions"], inplace=True)

    logger.info(f"Convert each dataframe back to dict")
    # Convert each dataframe back to dict, as DB Load acceptable format
    et_data_transformed: EnderTuringAPIBaseDictsTransformed = {}
    for dict_name in df_et_data.keys():
        # Replace NaT/NaN values with None, as DB Load acceptable format
        df_et_data[dict_name].replace({np.nan: None}, inplace=True)
        et_data_transformed[dict_name] = df_et_data[dict_name].to_dict(orient='records')
        if len(et_data_transformed[dict_name]):
            logger.debug("First record in %s: %s", dict_name, et_data_transformed[dict_name][0])
            logger.debug("Last record in %s: %s", dict_name, et_data_transformed[dict_name][-1])

    return et_data_transformed


def transform_session_data(
        et_data: EnderTuringAPISessionsData) -> EnderTuringAPISessionsDataTransformed:
    logger.info(f"--- STEP 2. Transform data from Ender Turing to DB format ---")
    logger.info(f"Transforming Sessions/Scores data for %s sessions", len(et_data["sessions"]))

    logger.info(f"Convert each dictionary to dataframe: %s", et_data.keys())
    # convert each dictionary to dataframe for transformations easiness
    df_et_data = {}
    for dict_name in et_data.keys():
        df_et_data[dict_name] = pd.DataFrame(et_data[dict_name])
        logger.info(f"Saving '{dict_name}.pkl' for debug purposes")
        df_et_data[dict_name].to_pickle(f"{dict_name}.pkl")

    logger.info(f"Flattening columns containing lists -> into separate table")
    logger.info(f"Flattening dataframe columns: %s", df_et_data["sessions"].columns)
    logger.info(f"To Flatten first record: %s", et_data["sessions"][0])
    # Transform 'sessions' -> separate 'tags', 'categories', 'reviewers', 'summaries', 'comments'
    # separate 'tags'
    df_et_data["sessions_tags"] = pd.json_normalize(
        et_data["sessions"], record_path="tags", meta=["id"], meta_prefix="session_", max_level=0
    )
    df_et_data["sessions_tags"] = pd.json_normalize(
        df_et_data["sessions_tags"].to_dict(orient='records'),
        record_path="match", meta=["id", "session_id"],
        meta_prefix="tag_",
        max_level=0
    ).rename(columns={"tag_session_id": "session_id"})

    # separate 'categories'
    df_et_data["sessions_categories"] = pd.json_normalize(
        et_data["sessions"], record_path="categories", meta=["id"], meta_prefix="session_", max_level=0
    ).rename(columns={"id": "category_id"})

    # separate 'reviewers'
    df_et_data["sessions_reviewers"] = pd.json_normalize(
        et_data["sessions"], record_path="reviewers", meta=["id"], meta_prefix="session_", max_level=0
    ).rename(columns={"id": "reviewer_id"})

    # separate 'scores'
    if "sessions" in et_data and len(et_data["sessions"]) and "scores" not in et_data["sessions"][0]:
        logger.error("'scores' not present in 'sessions' dict. Saving to 'sessions_broken_scores.pkl' for debug")
        df_et_data["sessions"].to_pickle('sessions_broken_scores.pkl')
    else:
        df_et_data["sessions_scores"] = pd.json_normalize(
            et_data["sessions"], record_path="scores", max_level=0
        )
        # one more expand, we store all points in same list under session in API to decrease size
        df_et_data["sessions_scores"] = pd.json_normalize(
            df_et_data["sessions_scores"].to_dict(orient='records'),
            record_path="point_scores",
            meta=["session_id", "scorecard_id", "reviewer_id", ],
            max_level=0
        )

    # separate 'comments'
    df_et_data["sessions_comments"] = pd.json_normalize(
        et_data["sessions"], record_path="comments", max_level=0
    )

    # separate 'summaries'
    df_et_data["sessions_summaries"] = pd.json_normalize(
        et_data["sessions"], record_path="summary", max_level=0
    )

    # separate 'crm_statuses'
    df_et_data["sessions_crm_statuses"] = pd.json_normalize(
        et_data["sessions"], record_path="crm_statuses", meta=["id"], meta_prefix="session_", max_level=0
    )

    # remove original 'tags', 'categories', 'reviewers' from 'sessions',
    # as we will load it to DB in separate table
    df_et_data["sessions"].drop(
        columns=[
            "tags", "categories", "reviewers", "crm_statuses", "scores", "comments", "summary"
        ], inplace=True
    )

    logger.info(f"Parsing DateTimes")
    # DateTime-like columns -> Convert to DateTimes objects acceptable by DB
    try:
        df_et_data["sessions"]["start_dt"] = pd.to_datetime(df_et_data["sessions"]["start_dt"]).dt.round('s')
    except:
        logger.error("Issue on converting 'start_dt' to datetime")
        df_et_data["sessions"]["start_dt"] = df_et_data["sessions"]["start_dt"].str.extract(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})')[0]
        df_et_data["sessions"]["start_dt"] = pd.to_datetime(df_et_data["sessions"]["start_dt"]).dt.round('s')
    # df_et_data["sessions"]["end_dt"] = pd.to_datetime(df_et_data["sessions"]["end_dt"], format="%Y-%m-%dT%H:%M:%S.%f").dt.round('s')
    # df_et_data["sessions"]["created_at"] = pd.to_datetime(df_et_data["sessions"]["created_at"], format="%Y-%m-%dT%H:%M:%S.%f").dt.round('s')
    # df_et_data["sessions"]["updated_at"] = pd.to_datetime(df_et_data["sessions"]["updated_at"], format="%Y-%m-%dT%H:%M:%S.%f").dt.round('s')

    if len(["sessions_reviewers"]) and "last_reviewed_at" in df_et_data["sessions_reviewers"].columns:
        df_et_data["sessions_reviewers"]["last_reviewed_at"] = pd.to_datetime(df_et_data["sessions_reviewers"]["last_reviewed_at"]).dt.round('s')

    if len(["sessions_comments"]) and "created_at" in df_et_data["sessions_comments"].columns:
        df_et_data["sessions_comments"]["created_at"] = pd.to_datetime(df_et_data["sessions_comments"]["created_at"]).dt.round('s')
    if len(["sessions_comments"]) and "updated_at" in df_et_data["sessions_comments"].columns:
        df_et_data["sessions_comments"]["updated_at"] = pd.to_datetime(df_et_data["sessions_comments"]["updated_at"]).dt.round('s')

    if len(["sessions_summaries"]) and "created_at" in df_et_data["sessions_summaries"].columns:
        df_et_data["sessions_summaries"]["created_at"] = pd.to_datetime(df_et_data["sessions_summaries"]["created_at"]).dt.round('s')
    if len(["sessions_summaries"]) and "updated_at" in df_et_data["sessions_summaries"].columns:
        df_et_data["sessions_summaries"]["updated_at"] = pd.to_datetime(df_et_data["sessions_summaries"]["updated_at"]).dt.round('s')

    logger.info(f"Remove unneeded for DWH case columns")
    # Controlled Remove unneeded for DWH case columns,
    # to log errors on some new data appeared tht is not in scheme, hence silently skipped from Load
    df_et_data["sessions"].drop(columns=[
        "end_dt", "created_at", "updated_at",  # to avoid pd.to_datetime exceptions and time to fix
        "compliance_matches", "ptp_kept_prediction", "comment_author_ids",  # might need in future
        "group", "agent", "agent_name", "category_ids",  # already stored in other keys/tables
        "emotions", "activity", "sentiments", "events_call_id", "low_quality",  # don't need
    ], inplace=True)
    if len(["sessions_reviewers"]) and "name" in df_et_data["sessions_reviewers"].columns:
        df_et_data["sessions_reviewers"].drop(columns=["name"], inplace=True)
    if len(["sessions_summaries"]) and "id" in df_et_data["sessions_summaries"].columns:
        df_et_data["sessions_summaries"].drop(columns=["id"], inplace=True)
    if len(["sessions_scores"]) and "id" in df_et_data["sessions_scores"].columns:
        df_et_data["sessions_scores"].drop(columns=["id"], inplace=True)

    logger.info(f"Convert each dataframe back to dict")
    # Convert each dataframe back to dict, as DB Load acceptable format
    et_data_transformed: EnderTuringAPISessionsDataTransformed = {}
    for dict_name in df_et_data.keys():
        # # Replace NaT/NaN values with None, as DB Load acceptable format
        df_et_data[dict_name].replace({np.nan: None}, inplace=True)
        et_data_transformed[dict_name] = df_et_data[dict_name].to_dict(orient='records')
        if len(et_data_transformed[dict_name]):
            logger.debug("First record in %s: %s", dict_name, et_data_transformed[dict_name][0])
            logger.debug("Last record in %s: %s", dict_name, et_data_transformed[dict_name][-1])
        else:
            logger.warning("No records for %s", dict_name)

    return et_data_transformed