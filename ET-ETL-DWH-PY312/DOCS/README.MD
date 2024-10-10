# ETL Pipeline to sync data from ET to external systems (e.g. corporate DB/BI system, SaaS BI/Analytics software)

# Flow
Flow delivered in ETL pipeline.
ETL stands for Extract, Transform, and Load. It is a process used in data warehousing and business intelligence to move data from one or more sources into a target system. Here's a detailed description of each step and how it applies to Python scripts that fetch data from an API and load it into a BI tool or a database:

1. **Extract**: The `extract` file functions fetches data from an API.
This ETL flow step just extract as-is all data from the Ender Turing API.
Extract has 2 separate functions:
- Extract Base Dictionaries - stable, rarely changed dicts for Agents/ Users/ Tags, etc --- 
- Extract Data - data, changing heavily during a day: conversations, tags, scores, etc ---
2. **Transform**: The `transform` file functions processes the raw data into a structured format suitable for loading.
Transform has 2 separate functions:
- Transform Base Dictionaries
- Transform Data
3. **Load**: The `load` file functions loads the transformed data into a database.
This ETL flow step (3) just load as-is all data provided from the Transform Step.
Load is in UPSERT mode, we believe all datta in ET is latest and source of truth.

# Settings
Settings described in `settings.py` file. This will automatically read ENV variable or .env file for variables described in `settings.py`.
### Expected variables in ENV or `.env` file 
```
# Extract settings
et_domain=app.enderturing.com
et_user=et_user@enderturing.com
et_password=password
# Load settings
DATABASE_URL="mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server"
```

## Pre-requisites
### Base OS level requirements
Python3.10+ and PIP package manager required
### Additional Python packages requirements
```bash
pip install -r requirements.txt
```

# How to RUN
### Test mode run
```bash
# Test mode will fetch max 200 sessions
python3 run-et-etl.py --test-mode
# if you need more sessions to fetch in Test mode
python3 run-et-etl.py --test-mode --test-mode-limit-sessions 500  
```

### Manual run (for historical, or for long period)
```bash
python3 run-et-etl.py
```
### CRON run (dor daily run)
Daily shall be cron run after at the beginning of the day, midnight, e.g. 00:05
To add daily cron job use example below. Change `path` to your own
```bash
(crontab -l 2>/dev/null; echo "*/5 * * * * python3 /path/run-et-etl.python") | crontab -
```

# DB scheme and DB initialization
DB scheme described in ./ETL/schema.py file.
If tables is not existed in DB, they will be auto created on first run

# Customization
This ETL pipeline can be extended or modified to suit more complex data processing needs.
Most customization shall be done in Transform step, as other steps just extract from API and load to DB/API/file


# Detailed ETL description 
1. **Extract**:
   - **Definition**: Extracting involves retrieving data from different sources. In this context, it means fetching data from APIs.
   - **Python Implementation**: Using libraries like `requests` to make HTTP requests to the API endpoints and retrieve the raw data.
   - **Example**:
     ```python
     import requests

     def extract_data(api_url, params=None, headers=None):
         response = requests.get(api_url, params=params, headers=headers)
         response.raise_for_status()
         return response.json()
     ```

2. **Transform**:
   - **Definition**: Transforming data means cleaning, formatting, and modifying the data to fit the target schema or to enhance its quality. This step often includes data validation, type conversions, removing duplicates, and aggregating data.
   - **Python Implementation**: Using libraries like `pandas` for data manipulation, applying business logic to the raw data, and converting it into a structured format.
   - **Example**:
     ```python
     import pandas as pd

     def transform_data(raw_data):
         df = pd.DataFrame(raw_data)
         # Example transformations:
         df['date'] = pd.to_datetime(df['date'])
         df.drop_duplicates(inplace=True)
         df['value'] = df['value'].astype(float)
         return df
     ```

3. **Load**:
   - **Definition**: Loading is the process of inserting the transformed data into the target system, such as a database or a BI tool.
   - **Python Implementation**: Using libraries like `sqlalchemy` for databases or APIs of BI tools to load the data.
   - **Example**:
     ```python
     from sqlalchemy import create_engine

     def load_data(df, db_connection_string, table_name):
         engine = create_engine(db_connection_string)
         with engine.connect() as connection:
             df.to_sql(table_name, con=connection, if_exists='replace', index=False)
     ```

### Putting It All Together

Here's a complete example that demonstrates a simple ETL process in Python:

```python
import requests
import pandas as pd
from sqlalchemy import create_engine
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_data(api_url, params=None, headers=None):
    try:
        response = requests.get(api_url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        raise

def transform_data(raw_data):
    try:
        df = pd.DataFrame(raw_data)
        df['date'] = pd.to_datetime(df['date'])
        df.drop_duplicates(inplace=True)
        df['value'] = df['value'].astype(float)
        return df
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise

def load_data(df, db_connection_string, table_name):
    try:
        engine = create_engine(db_connection_string)
        with engine.connect() as connection:
            df.to_sql(table_name, con=connection, if_exists='replace', index=False)
        logger.info("Data loaded successfully")
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

if __name__ == "__main__":
    api_url = "https://api.example.com/data"
    db_connection_string = "postgresql://user:password@localhost:5432/mydatabase"
    table_name = "api_data"

    try:
        raw_data = extract_data(api_url)
        transformed_data = transform_data(raw_data)
        load_data(transformed_data, db_connection_string, table_name)
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
```
