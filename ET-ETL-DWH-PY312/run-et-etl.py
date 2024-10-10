#!/usr/bin/env python3
import argparse

from datetime import datetime, timedelta

from ETL.Extract import extract_base_dicts, extract_sessions, EnderTuringAPIBaseDicts
from ETL.Load import load
from ETL.Transform import transform_base_dicts, transform_session_data
from ETL.utils import parse_start_stop_dates, dt2file, file2dt, log_exceptions
from settings import settings, logger


@log_exceptions
def etl_base_dicts(load_to: str) -> EnderTuringAPIBaseDicts:
    """
    ETL Base Dictionaries
    Extract base dictionaries: agents, categories, groups, labels, scorecards, tags, users
    """
    logger.info(f"Running Base dictionaries sync ETL ")
    run_start = datetime.now()
    # 1 - Extract data from ET
    _et_data_base_dicts = extract_base_dicts()
    # 2 - Transformations for base dictionaries
    _et_data_base_dicts = transform_base_dicts(_et_data_base_dicts)
    # 3 - Load data to DB/API/File
    load(load_to, et_data=_et_data_base_dicts)
    logger.info("Base dictionaries ETL run time: %s seconds",
                int((datetime.now() - run_start).total_seconds()))
    return _et_data_base_dicts


@log_exceptions
def etl_sessions_period(load_to: str, _start_dt: datetime, _stop_dt: datetime, filters: str = None):
    """
    ETL Conversations / Scores / Summaries
    for period:
        - daily sync for previous day
        - historical sync for longer period
    """
    logger.info(f"Running Period conversations sync ETL from {_start_dt} to {_stop_dt}")
    run_start = datetime.now()
    # 1 - Extract data from ET
    # Extract sessions/scores data for selected period: sessions, scores, summaries, transcripts
    et_sessions = extract_sessions(
        start_dt=_start_dt,
        stop_dt=_stop_dt,
        filters=filters,
        get_session_scores_detailed=True,
        get_session_additional_meta=False,
        get_session_transcripts=False,
        get_session_summaries=True,
        get_session_comments=False,
    )
    if "sessions" and not len(et_sessions["sessions"]):
        logger.info("No sessions extracted, skipping Transform and Load")
    else:
        # 2 - Transform data from ET to DB format
        et_sessions = transform_session_data(et_sessions)
        # 3 - Load data to DB/API/File
        load(load_to, et_data=et_sessions)
    logger.info("Period ETL run time: %s seconds",
                int((datetime.now() - run_start).total_seconds()))


@log_exceptions
def etl_sessions_incremental(
        load_to: str,
        _et_data_base_dicts: EnderTuringAPIBaseDicts,
        _stop_dt: datetime,
        _last_synced: datetime,
):
    """
    Sync updated data - if new scores/analytics updated for already loaded period.
    For incremental sync of already loaded period:
    - Quality/Scoring related
        - manual scores updated for historical, already loaded period
    - Analytics Related
        - categories updated for historical, already loaded period
        - categories updated for historical, already loaded period
    ! Runs only for daily sync, no need to run for historical sync
    """
    # TODO only sync info for up to first record - no sense to download -30 days if we have only 3 days in DB
    _start_dt = _stop_dt - timedelta(settings.incremental_sync_n_days)
    logger.info(f"Running Incremental Quality/Analytics sync ETL from {_start_dt} to {_stop_dt}")
    run_start = datetime.now()
    # Extract new Manual Quality Scoring for last N days,
    # as they can apper in few days after conversation appeared in ET
    # no need to incrementally extract Automated as they appear same day
    logger.info(f"Incremental Quality sync ETL from {_start_dt} to {_stop_dt}")
    etl_sessions_period(
        load_to=load_to, _start_dt=_start_dt, _stop_dt=_stop_dt, filters="is_scored,manual"
    )

    # Extract Analytics related updated sessions for last N days
    # - Categories
    # get updated categories after last sync
    logger.info(f"Incremental Analytics sync ETL from {_start_dt} to {_stop_dt}")
    updated_categories = [str(i["id"]) for i in _et_data_base_dicts["categories"] if
                          i["updated_at"].to_pydatetime() > _last_synced]
    etl_sessions_period(
        load_to=load_to,
        _start_dt=_start_dt,
        _stop_dt=_stop_dt,
        filters=f"categories,{','.join(updated_categories)}|or"
    )

    # - Tags - NotImplementer on ET side (no 'updated_at' field)
    # get updated tags after last sync
    # updated_tags = [str(i["id"]) for i in _et_data_base_dicts["tags"] if
    #                       i["updated_at"].to_pydatetime() > last_sync]
    # etl_sessions_period(
    #     load_to=load_to, _stop_dt=_stop_dt, filters=f"tags,{','.join(updated_tags)}|or"
    # )
    logger.info("Incremental ETL run time: %s seconds",
                int((datetime.now() - run_start).total_seconds()))


if __name__ == "__main__":
    """
    ETL pipeline to sync conversations from ET to DB/API/file.
    Can sync for period:
        - [Default mode] daily sync for previous day -> No input arguments required
        - historical sync for longer period -> 'start_dt' argument is required
    Daily shall be cron run after at the beginning of the day, midnight, e.g. 00:05
    Add CRON TASK by:
    (crontab -l 2>/dev/null; echo "*/5 * * * * python3 /path/run-et-etl.python") | crontab -

    """
    parser = argparse.ArgumentParser(
        description="ETL pipeline to Extract Ender Turing data and load to DB/API/file"
    )
    parser.add_argument("--load-to", default="db")
    parser.add_argument(
        "--start-dt", default=None, help="start date for historical sync, in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--stop-dt",
        default=None,
        help="stop date for historical sync, in YYYY-MM-DD format. "
             "If only start date provided, "
             "stop date will be calculated to last full day (yesterday) automatically."
    )
    # Adding the test-mode boolean flag
    parser.add_argument(
        "--test-mode",
        action="store_true",  # This will set the flag to True if provided
        help="Run the script in test mode without making changes."
    )

    # Adding the test_mode_limit_sessions with default to 200
    parser.add_argument(
        "--test-mode-limit-sessions",
        type=int,  # Ensure the argument is an integer
        default=200,  # Default value
        help="Limit the number of sessions in test mode (default: 200)."
    )
    args = parser.parse_args()

    logger.info("Script running with args: %s", args)
    if args.test_mode:
        logger.info("Switching ON TEST mode with limit %s records", args.test_mode_limit_sessions)
        settings.test_mode = True
        settings.test_mode_limit_sessions = args.test_mode_limit_sessions
        logger.info("Switching LOG_LEVEL to DEBUG")
        settings.log_level = "DEBUG"

    daily_sync_mode = True
    if args.start_dt:
        # historical sync mode
        daily_sync_mode = False
        logger.info("Switching to HISTORICAL SYNC mode")

    start_dt, stop_dt = parse_start_stop_dates(_args=args)
    logger.info(f"\n\n\n------- Starting Ender Turing sync ETL at {datetime.now()} -------")
    logger.info(
        f"Running ETL in {'daily' if daily_sync_mode else 'historical'} mode from '{start_dt}' to '{stop_dt}'"
    )

    # Load last successful sync date
    last_synced = file2dt(settings.last_synced_fpath)
    logger.info(f"Last synced timestamp: {last_synced}")

    # Run sync for BASE DICTIONARIES: agents, categories, groups, labels, scorecards, tags, users
    et_data_base_dicts: EnderTuringAPIBaseDicts = etl_base_dicts(load_to=args.load_to)

    # Run SYNC for period 'DAILY' or 'HISTORICAL'
    etl_sessions_period(load_to=args.load_to, _start_dt=start_dt, _stop_dt=stop_dt)

    if daily_sync_mode:
        # Run INCREMENTAL SYNC - runs only for daily sync mode, no need to run for historical mode
        # Sync changes for already fetched sessions (scores/analytics)
        etl_sessions_incremental(
            load_to=args.load_to,
            _et_data_base_dicts=et_data_base_dicts,
            _stop_dt=stop_dt,
            _last_synced=last_synced
        )

    # store last successful sync
    dt2file(dt=datetime.now(), file_path=settings.last_synced_fpath)
    logger.info(f"------- Sync ETL successfully finished at {datetime.now()} -------")
