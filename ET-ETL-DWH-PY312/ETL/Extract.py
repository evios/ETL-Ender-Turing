import urllib
from datetime import datetime
from typing import TypedDict

from ETL.utils import split_half_days, request_et_api, init_et, init_et_by_pat_token
from settings import settings, logger


class EnderTuringAPIBaseDicts(TypedDict):
    agents: list[dict]
    categories: list[dict]
    groups: list[dict]
    labels: list[dict]
    scorecards: list[dict]
    tags: list[dict]
    users: list[dict]


class EnderTuringAPISessionsData(TypedDict):
    sessions: list[dict]


def get_et_sessions(
        et,
        start_dt: datetime,  # datetime object like "2024-05-01"
        stop_dt: datetime,  # datetime object like "2024-05-15"
        filters=None,
        page_limit=500,
) -> list:
    if settings.test_mode:
        page_limit = settings.test_mode_limit_sessions  # these results will be enough to test
    sessions = []
    for half_day_interval in split_half_days(start_dt, stop_dt):
        # Define Filters for request
        if filters:
            # filters = f"date_range,{start_dt},{stop_dt}||±{filters}"  # without half day split
            url_filters = f"date_range,{half_day_interval}±{urllib.parse.unquote(filters)}"
        else:
            url_filters = f"date_range,{half_day_interval}"  # date/half-day filter

        skip = 0
        page_number = 1
        while True:
            logger.debug(f"Searching sessions on page={page_number} skip={skip} with pageSize={page_limit}")
            logger.info(f"Searching with filters: {url_filters}")
            url = f"/sessions?skip={skip}&limit={page_limit}&filters={urllib.parse.quote(url_filters)}"
            logger.info(f"Requesting ET sessions on: {url}")
            # TODO add try:
            response = request_et_api(et, url=url)
            sessions_page = response["items"]
            logger.info(f"Found {len(sessions_page)} matched calls on ET API page {page_number}")
            sessions.extend(sessions_page)
            if len(sessions_page) != page_limit:
                break  # last page
            if settings.test_mode:
                logger.warning("Stopped pagination by test_mode flag")
                break  # these results will be enough to test
            page_number += 1
            skip += len(sessions_page)
        logger.info(f"Got {len(sessions)} results")
        if settings.test_mode:
            logger.warning("Stopped half-daily search by test_mode flag")
            break  # these results will be enough to test
    return sessions


def get_et_session_details(
        et, sessions: list, endpoint_suffix: str = None, column_should_contain_data: str = False
) -> list:
    """
    Fetch session additional meta.
    Can be on session level to fetch additional_info (not in main sessions' endpoint)
        need for ticket_system_id, crm_system_id, etc.
    Can also be for Scores, with endpoint_suffix="/scores"
        which end up at /sessions/{session_id}/scores
    :param et:
    :param sessions:
    :param endpoint_suffix:
    :return:
    """
    logger.info(f"Extracting {endpoint_suffix} for {len(sessions)} sessions")

    for idx, session in enumerate(sessions):
        if idx % settings.log_every == 0:
            logger.info(f"Fetching progress {endpoint_suffix}: {idx} of {len(sessions)}")
        session_id = session["id"]
        if column_should_contain_data and not session[column_should_contain_data]:
            logger.debug(
                "Skipping session with no data in session '%s' column: %s %s",
                column_should_contain_data,
                session[column_should_contain_data],
                session_id,
            )
            continue
        try:
            session_meta = request_et_api(et, url=f'/sessions/{session_id}{endpoint_suffix}')
        except Exception as e:
            logger.error(
                "Error loading details from %s for session %s %s", endpoint_suffix, session["id"], e
            )
            continue
        # df.at[idx, "additional_info"] = session_meta.get("additional_info")
        if "/" in endpoint_suffix:
            # append to key 'scores', 'transcripts', 'summary', 'comments', etc
            session[endpoint_suffix.replace("/", "")] = session_meta
        else:
            # requested top level session meta, just replace all
            session = session_meta  # just update session, it will update data in original dict
    logger.info(f"Fetched sessions details DONE for {endpoint_suffix}: {len(sessions)}")
    # # fetch additional_info (not in main sessions' endpoint) - additional meta, just in case
    # for idx, row in df.iterrows():
    #     # additional_info
    #     session_meta = et.sessions.get_session(session_id=row["id"])
    #     df.at[idx, "additional_info"] = session_meta.get("additional_info")
    #     if idx % 100 == 0:
    #         print(f"Done: {idx} of {len(df)}")
    return sessions


def extract_sessions(
    start_dt: datetime,  # datetime object like "2024-05-01"
    stop_dt: datetime,  # datetime object like "2024-05-15"
    filters: str = None,
    get_session_scores_detailed: bool = True,
    get_session_additional_meta: bool = False,
    get_session_transcripts: bool = False,
    get_session_summaries: bool = True,
    get_session_comments: bool = False,
) -> EnderTuringAPISessionsData:
    """
    This ETL flow step just extract as-is all data from the Ender Turing API
    Extract /Fetch data from ET API and returns it as dict of dicts,
    which will be parsed and transform in transform step
    :param start_dt: str datetime like object in format "%Y-%m-%d", e.g. "2024-01-01"
    :param stop_dt: str datetime like object in format "%Y-%m-%d", e.g. "2024-01-01"
    :param filters: no filters = means all conversations for a period,
        you can copy filter from UI to download only necessary sessions
    :param get_session_scores_detailed:
    :param get_session_additional_meta:
    :param get_session_transcripts:
    :param get_session_summaries:
    :param get_session_comments:
    :return:
    """
    logger.info(f"--- STEP 1. Extract from Ender Turing ---")
    # 1 - Log in to ET system
    if settings.et_auth_by_token:
        logger.info("Auth to ET using Token")
        et = init_et_by_pat_token(domain=settings.et_domain)
    else:
        logger.info("Auth to ET using User/Password")
        et = init_et(domain=settings.et_domain, user=settings.et_user)

    # 2 - Fetch conversations/sessions
    # without transcripts and summaries, only list of conversations with high level metadata
    sessions = get_et_sessions(et, start_dt=start_dt, stop_dt=stop_dt, filters=filters)

    # TODO - test correctness by comparing with data from docs#/sessions/get_number_of_sessions_api_v1_sessions_filter_number_of_sessions_get

    # 3 - Fetch scores for conversations
    # Initialize "scores" key with an empty list for each dictionary in the list
    for session in sessions:
        session["scores"] = []
    if get_session_scores_detailed:
        # "reviewers": [] - skip, only fetch for those that has ones, not all sessions
        # "reviewers": [{"id": 10, "last_reviewed_at": "2024-07-08T07:30:44.947975"}] - process
        sessions = get_et_session_details(
            et, sessions, endpoint_suffix="/scores", column_should_contain_data="reviewers"
        )

    # 4 - Fetch additional metadata, like CRM ID, Ticket System ID, etc
    if get_session_additional_meta:
        sessions = get_et_session_details(et, sessions)

    # 5 - Fetch transcripts
    # Initialize "transcripts" key with an empty list for each dictionary in the list
    for session in sessions:
        session["transcripts"] = []
    if get_session_transcripts:
        raise NotImplemented  # TODO
        # sessions = get_et_session_details(et, sessions, endpoint_suffix="/transcripts")

    # 6 - Fetch summaries
    # Initialize "summary" key with an empty list for each dictionary in the list
    for session in sessions:
        session["summary"] = []
    if get_session_summaries:
        # TODO - only fetch for those that has ones, not all sessions
        sessions = get_et_session_details(et, sessions, endpoint_suffix="/summary")

    # 7 - Fetch comments
    # Initialize "comment" key with an empty list for each dictionary in the list
    for session in sessions:
        session["comments"] = []
    if get_session_comments:
        # "comments_count": 0, - skip, only fetch for those that has ones, not all sessions
        # "comment_author_ids": [],
        sessions = get_et_session_details(
            et, sessions, endpoint_suffix="/comments", column_should_contain_data="comments_count"
        )

    return {
        "sessions": sessions,
    }


def extract_base_dicts() -> EnderTuringAPIBaseDicts:
    """
    This ETL flow step just extract as-is all data from the Ender Turing API
    """
    logger.info(f"--- STEP 1. Extract from Ender Turing ---")
    # 1 - Log in to ET system
    if settings.et_auth_by_token:
        logger.info("Auth to ET using Token")
        et = init_et_by_pat_token(domain=settings.et_domain)
    else:
        logger.info("Auth to ET using User/Password")
        et = init_et(domain=settings.et_domain, user=settings.et_user)

    # 2 - Fetch base dictionaries
    logger.info(f"Extracting data for 'agents'")
    agents = request_et_api(et, url="/agents", params={"limit": 999})

    logger.info(f"Extracting data for 'categories'")
    categories = request_et_api(et, url="/categories")

    logger.info(f"Extracting data for 'groups'")
    groups = request_et_api(et, url="/agent-groups")

    logger.info(f"Extracting data for 'labels'")
    labels = request_et_api(et, url="/labels")

    logger.info(f"Extracting data for 'scorecards'")
    scorecards = request_et_api(et, url="/scorecards")

    logger.info(f"Extracting data for 'tags'")
    tags = request_et_api(et, url="/tags", params={"limit": 9_999})

    logger.info(f"Extracting data for 'users'")
    users = request_et_api(et, url="/users", params={"limit": 999})

    return {
        "agents": agents,
        "categories": categories,
        "groups": groups,
        "labels": labels,
        "scorecards": scorecards,
        "tags": tags,
        "users": users,
    }
