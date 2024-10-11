import json

import pandas as pd
from sqlalchemy import create_engine, update, select, insert, inspect
from sqlalchemy.dialects.sqlite import insert as insert_sqlite
from sqlalchemy.orm import sessionmaker
from settings import settings, logger
from ETL.Extract import EnderTuringAPIBaseDicts, EnderTuringAPISessionsData
from ETL.schema import Base
from ETL.utils import SessionContext, is_table_exists, anonymize_database_url, \
    get_subclasses, unmatched_tables, get_columns, get_unique_constraint_columns


def create_db_tables(engine, db_models):
    tables = []
    absent_db_tables = []
    for db_model in db_models:
        db_table = db_model.__tablename__
        tables.append(db_table)
        # check each table existence in DB
        if not is_table_exists(engine, db_table):
            absent_db_tables.append(db_table)
        # TODO - check all columns existed to create new column if added in schema

    if absent_db_tables:
        logger.warning(f"Absent DB Tables. Creating all ... {absent_db_tables}")
        Base.metadata.create_all(engine)
    else:
        logger.info(f"All DB Tables Already Existed. Skip Creation. {tables}")


def et_data_vs_schema(et_data: EnderTuringAPIBaseDicts | EnderTuringAPISessionsData):
    db_models = get_subclasses(Base)
    if unmatched_keys := unmatched_tables(et_data, db_models):
        logger.warning(
            "New data in 'et_data' appeared without corresponding tables in schema %s",
            unmatched_keys
        )
    for db_model in db_models:
        db_table = db_model.__tablename__
        if db_table not in et_data:
            continue
        et_dict = et_data[db_table]
        if len(et_dict) == 0:
            logger.info(f"Skipping load to DB Table {db_table}, empty dict from API.")
            continue

        logger.info(f"Loading data to DB Table {db_table} ...")
        # Get the column names from the Table model,
        # to filter out keys in et_data without corresponding column in DB
        table_columns = get_columns(db_model).keys()

        # Warn on not loaded data, show skipped columns that won't be loaded to DB
        if absent_columns := {k for k in et_dict[0].keys() if k not in table_columns}:
            logger.warning(
                "Next data won't be loaded to DB Table '%s' (no column exists): %s",
                db_table,
                absent_columns
            )


def load2db(et_data: EnderTuringAPIBaseDicts | EnderTuringAPISessionsData):
    db_models = get_subclasses(Base)
    if unmatched_keys := unmatched_tables(et_data, db_models):
        logger.warning(
            "New data in 'et_data' appeared without corresponding tables in schema %s",
            unmatched_keys
        )

    logger.info(f"Connecting to DB {anonymize_database_url(settings.DATABASE_URL)}")
    engine = create_engine(settings.DATABASE_URL)
    if settings.init_db_tables:
        create_db_tables(engine, db_models)
    SessionDB = sessionmaker(bind=engine)

    logger.info(f"Loading data for: {list(et_data.keys())}")
    with SessionContext(SessionDB()) as session:
        for db_model in db_models:
            db_table = db_model.__tablename__
            if db_table not in et_data:
                continue
            et_dict = et_data[db_table]
            if len(et_dict) == 0:
                logger.info(f"Skipping load to DB Table {db_table}, empty dict from API.")
                continue

            logger.info(f"Loading data to DB Table {db_table} ...")
            # Get the column names from the Table model,
            # to filter out keys in et_data without corresponding column in DB
            table_columns = get_columns(db_model).keys()

            # Warn on not loaded data, show skipped columns that won't be loaded to DB
            if absent_columns := {k for k in et_dict[0].keys() if k not in table_columns}:
                logger.warning(
                    "Next data won't be loaded to DB Table '%s' (no column exists): %s",
                    db_table,
                    absent_columns
                )

            # Upsert data
            for idx, item in enumerate(et_dict):
                if idx % settings.log_every == 0:
                    logger.info(f"Loading to DB progress {db_table}: {idx} of {len(et_dict)}")
                unique_columns = None
                conditions = None
                existing_record = None
                update_values = None
                # Filter out keys from data that do not have corresponding column
                filtered_item = {k: v for k, v in item.items() if k in table_columns}
                logger.debug(f"Loading data to DB Table {db_table}: {filtered_item}")
                try:
                    if settings.DATABASE_URL.startswith("sqlite"):
                        row = insert_sqlite(db_model).values(**filtered_item)
                        row = row.on_conflict_do_update(
                            index_elements=get_unique_constraint_columns(db_model),
                            set_=filtered_item
                        )
                        session.execute(row)
                    elif settings.DATABASE_URL.startswith("mssql"):
                        # For MSSQL, check if the row exists and then update, else insert
                        unique_columns = get_unique_constraint_columns(db_model)
                        logger.debug("Unique columns for Table %s: %s", db_table, unique_columns)
                        # check all unique columns present in data to load
                        missed_in_loading_data = [col for col in unique_columns if col not in filtered_item]
                        if missed_in_loading_data:
                            logger.error(
                                "Error loading to Table '%s', required key '%s' missed in data: %s",
                                db_table,
                                missed_in_loading_data,
                                filtered_item,
                            )
                            exit(-1)  # do not continue loading in case of any errors

                        conditions = [
                            getattr(db_model.__table__.c, col) == filtered_item[col] for col in unique_columns
                        ]

                        # Check if the record exists
                        logger.debug(
                            "Check if record existed by conditions: %s",
                            [str(condition) for condition in conditions]
                        )
                        existing_record = session.execute(select(db_model).where(*conditions)).fetchone()
                        if existing_record:
                            try:
                                logger.debug("Record already existed dict: %s", existing_record[0].__dict__)
                            except:
                                logger.debug("Record already existed: %s", existing_record)
                            # Exclude unique columns from the update statement
                            update_values = {k: v for k, v in filtered_item.items() if k not in unique_columns}
                            if update_values:  # Ensure there are columns to update
                                logger.debug("Updating existed record with: %s", update_values)
                                row = update(db_model).where(*conditions).values(**update_values)
                                session.execute(row)
                            else:
                                logger.debug("Record already exists and no updates needed:", filtered_item)
                        else:
                            logger.debug("Record not existed, inserting: %s", filtered_item)
                            # Insert the new record if it doesn't exist
                            row = insert(db_model).values(**filtered_item)
                            session.execute(row)
                except Exception:
                    logger.exception(
                        "Error loading data to Table %s: for %s record %s",
                        db_table,
                        "existed" if existing_record else "NOT existed",
                        filtered_item,
                    )
                    # Extensive Logging
                    logger.error("Unique columns for Table %s: %s", db_table, unique_columns)
                    logger.error(
                        "Checked record existence by conditions: %s",
                        [str(condition) for condition in conditions] if conditions else None,
                    )
                    if existing_record:
                        logger.error(
                            "Existing record already present for current update: %s",
                            existing_record[0].__dict__,
                        )
                        logger.error("Values used for updating existed record: %s", update_values)
                    exit(-1)  # do not continue loading in case of any errors
    logger.info("--- Data loaded to DB. Done ---")


def load2file(et_data: EnderTuringAPIBaseDicts | EnderTuringAPISessionsData):
    def default_converter(obj):
        # Convert all non-serializable objects
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        # Add more conversions here if needed
        return str(obj)

    et_data_vs_schema(et_data)
    file_name = "sessions.json" if "sessions" in et_data else "dicts.json"

    with open(file_name, 'w') as json_file:
        json.dump(et_data, json_file, default=default_converter, indent=4)
    logger.info("--- Data loaded to File %s. Done ---", file_name)


def load(load_to="db", **kwargs):
    """
    This ETL flow step just load as-is all data provided from the Transform Step in 'et_data' dict.
    Load is in UPSERT mode, we believe all datta in ET is latest and source of truth
    """
    logger.info(f"--- STEP 3. Load to {load_to} ---")
    if load_to == "db":
        load2db(**kwargs)
    if load_to == "looker":
        logger.info("Load to %s NotImplemented", load_to)
        raise NotImplemented
    if load_to in "file":
        logger.info("Load to: %s", load_to)
        load2file(**kwargs)
