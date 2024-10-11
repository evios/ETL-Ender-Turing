import argparse
import functools
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import pandas as pd
from sqlalchemy import inspect, UniqueConstraint
from sqlalchemy.exc import IntegrityError
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log, before_log

from settings import settings, logger
import requests
from enderturing import Config, EnderTuring
import getpass


def dt2file(dt: datetime, file_path: Path) -> None:
    """ Function to write the datetime to a file """
    with open(file_path, 'w') as file:
        json.dump(dt.isoformat(), file)


def file2dt(file_path: Path) -> datetime | None:
    """ Function to read the datetime from a file """
    if not file_path.exists():
        # first run - was never synced before
        return datetime.min
    try:
        with open(file_path, 'r') as file:
            dt_str = json.load(file)
            return datetime.fromisoformat(dt_str)
    except FileNotFoundError:
        return None
    except ValueError:
        return None


def dt2str(dt: datetime) -> str:
    """ Format the date as "YYYY-MM-DD" """
    return dt.strftime("%Y-%m-%d")


def str2dt(str_dt: str, is_short: bool = True) -> datetime:
    """ Check date format correctness and parse into datetime object """
    try:
        if is_short:
            datetime_format = "%Y-%m-%d"
        else:
            datetime_format = "%Y-%m-%dT%H:%M:%S.%f"
        return datetime.strptime(str_dt, datetime_format)
    except ValueError:
        logger.error(
            f"Provide dates in correct format YYYY-MM-DD, e.g. '2024-05-01' instead of '{str_dt}'")
        exit(-1)


def parse_start_stop_dates(_args: argparse) -> (datetime, datetime):
    _start_dt, _stop_dt = _args.start_dt, _args.stop_dt
    if _stop_dt:
        _stop_dt = str2dt(_stop_dt)
    else:
        # 'stop_dt' not provided - automatically generate
        # We will always finish sync on last full day - yesterday, no need to sync unfinished day
        # Calculate yesterday's date by subtracting one day
        # TODO - cover timezones, as -1 day (-24 hours) can be -2 days already if GMT -1 and more
        yesterday = datetime.today() - timedelta(days=1)
        _stop_dt = yesterday

    if _start_dt:
        # historical sync mode
        _start_dt = str2dt(_start_dt)
    else:
        # daily sync mode - start and stop is same day
        _start_dt = _stop_dt

    return _start_dt, _stop_dt


def log_exceptions(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.exception(f"Exception occurred in {func.__name__}")
            raise
    return wrapper


# Extract utils
def split_half_days(start_dt: datetime, stop_dt: datetime) -> list[str]:
    # TMP workaround while ET fixing 10K+ sessions in single run functionality
    half_day_intervals = []
    current_date = start_dt
    while current_date <= stop_dt:
        # First half of the day
        half_day_intervals.append(f"{dt2str(current_date)},{dt2str(current_date)}||00:00,12:00")
        # Second half of the day
        half_day_intervals.append(f"{dt2str(current_date)},{dt2str(current_date)}||12:01,23:59")
        # Move to the next day
        current_date += timedelta(days=1)
    return half_day_intervals


def init_et(
        domain: str = settings.et_domain,
        user: str = settings.et_user,
        password: str = settings.et_password
) -> EnderTuring:
    if password:
        logger.debug(f"Using password from environment variables")
    else:
        password = getpass.getpass(f'Please input Password for {user}@{domain}:')
    logger.debug(f"Connecting to {domain} with username {user}")
    return EnderTuring(Config.from_url(f"https://{user}:{password}@{domain}"))


def init_et_by_pat_token(
    domain: str = settings.et_domain,
    token: str = settings.et_token
) -> EnderTuring:
    if token:
        logger.debug(f"Using token from environment variables")
    else:
        token = getpass.getpass(f'Please input token for {domain}:')
    et = EnderTuring(Config(url=f"https://{domain}"))
    et.http_client._get_auth_headers = lambda: {"Authorization": f"Bearer {token}"}
    return et


def request_et_api(
    et, url, method="GET", params: dict = None, data: dict =None, headers: dict =None
) -> list[dict] | dict | None:
    """
    Fetch/Upload data from the provided API URL.
    Args:
    url (str): The API endpoint URL.
    params (dict, optional): Dictionary of query parameters. Defaults to None.
    headers (dict, optional): Dictionary of HTTP headers. Defaults to None.
    Returns:
    dict: Parsed JSON response from the API.
    Raises:
    ValueError: If the API response contains a status code indicating an error.
    requests.exceptions.RequestException: For network-related errors.
    """
    @retry(
        stop=stop_after_attempt(10),
        wait=wait_exponential(multiplier=1, min=5, max=30),
        # retry=retry_if_exception_type(RequestException),
        reraise=True,
        before=before_log(logger, logging.DEBUG),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def get_with_retry():
        return et.http_client.get(url, params=params)

    try:
        logger.debug(f"Requesting data from '{url}' using '{method}' with params '{params}'")
        method = method.upper()
        if method == 'GET':
            response = get_with_retry()
        elif method == 'POST':
            response = et.http_client.post(url, params=params, json=data, headers=headers)
        elif method == 'PUT':
            response = et.http_client.put(url, params=params, json=data, headers=headers)
        elif method == 'DELETE':  # prohibited
            raise NotImplemented
            # response = et.http_client.delete(url, params=params, json=data, headers=headers)
        else:
            logger.error(f"Invalid HTTP method: {method}")
            return
        return response
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
        raise
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"Connection error occurred: {conn_err}")
        raise
    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"Timeout error occurred: {timeout_err}")
        raise
    except requests.exceptions.RequestException as req_err:
        logger.error(f"An error occurred: {req_err}")
        raise
    except ValueError as json_err:
        logger.error(f"JSON decoding error: {json_err}")
        raise
    except Exception as err:
        logger.exception(f"An unexpected error occurred: {err}")
        raise

# Transform utils
# Function to handle out-of-bounds dates
def safe_to_datetime(date_str: str, default_value: datetime | None = None) -> datetime:
    """
    Handle an out-of-bounds date '0001-01-01T00:00:00',
    ET uses them as start of agent group association
    """
    try:
        return pd.to_datetime(date_str).round('s')
    except pd.errors.OutOfBoundsDatetime:
        return default_value


def unicode_to_utf8(unicode_str):
    """Function to convert unicode escape sequences to UTF-8"""
    try:
        return unicode_str.encode('utf-8').decode('unicode_escape')
    except AttributeError:
        return unicode_str  # Return the value as is if it's not a string


# DB Utils
def is_table_exists(engine, table_name) -> bool:
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()


class SessionContext:
    """ DB session state manager, for auto close/commit/rollback """
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                self.session.commit()
            else:
                self.session.rollback()
        except IntegrityError:
            self.session.rollback()
        finally:
            self.session.close()


def get_primary_key_columns(model) -> list:
    """ Function to get primary key columns """
    return [key.name for key in inspect(model).primary_key]


def get_unique_constraint_columns(model) -> list:
    """ Function to get unique constraint columns for SQLlite"""
    unique_constraints = [uc for uc in model.__table__.constraints if isinstance(uc, UniqueConstraint)]
    unique_constraint_columns = [col.name for uc in unique_constraints for col in uc.columns]

    # fallback to primary key if no constraints defined
    return unique_constraint_columns or get_primary_key_columns(model)


def anonymize_database_url(url: str) -> str:
    """ Function to anonymize the DATABASE_URL """
    if "@" not in url:
        return url
    parsed_url = urlparse(url)
    netloc = parsed_url.netloc.split('@')
    userinfo, hostinfo = netloc[0], netloc[1]
    userinfo = userinfo.split(':')[0] + ':****'
    anonymized_netloc = f"{userinfo}@{hostinfo}"
    anonymized_url = urlunparse(parsed_url._replace(netloc=anonymized_netloc))
    return str(anonymized_url)


def get_subclasses(base) -> list:
    """ Function to get all classes inheriting from Base """
    return base.__subclasses__()


def get_columns(model) -> list:
    """ Function to get all columns of a given model using SQLAlchemy's inspect """
    return inspect(model).columns


def unmatched_tables(et_data: dict, db_classes) -> list:
    db_tables = [db_cls.__tablename__ for db_cls in db_classes]
    return [k for k in et_data.keys() if k not in db_tables]
