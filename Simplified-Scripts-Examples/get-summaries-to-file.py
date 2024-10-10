import argparse
import urllib
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import getpass

from enderturing import Config, EnderTuring

def init_et(domain="app.enderturing.com", user="et-user-email@org.com"):
    password = getpass.getpass(f'Provide Password for {domain}:')
    config = Config.from_url(f"https://{user}:{password}@{domain}")
    et = EnderTuring(config)
    return et


def get_et_sessions(et, limit=1_000, start_dt="2024-10-01", stop_dt="2024-10-01", filters=None):
    # Filters
    # by date
    url = f"date_range,{start_dt},{stop_dt}||"
    # Other defined external filters
    if filters:
        if "%" in filters:
            filters = urllib.parse.unquote(filters)
        url = f"date_range,{start_dt},{stop_dt}||±{filters}"

    calls = []
    skip = 0
    page_number = 1
    while True:
        print(f"Searching Calls for page={page_number}, skip={skip} with pageSize={limit}")
        print(f"Requesting ET filters:", f'/sessions?skip={skip}&limit={limit}&filters={url}')
        response = et.http_client.get(
            f'/sessions?skip={skip}&limit={limit}&filters=' + urllib.parse.quote(url))
        calls_page = response["items"]
        print(f"Found '{len(calls_page)}' matched calls on Recording API page '{page_number}'")
        calls.extend(calls_page)
        if len(calls_page) != limit:
            break  # last page
        page_number += 1
        skip += len(calls_page)
    print(f'Got {len(calls)} results')
    et_sessions = pd.DataFrame(calls)
    return et_sessions


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
    # add ET link - to easy jump directly to conversation from file/BI system
    df["et_link"] = f"{et.http_client.config.url}/main/conversations/view?session_id=" + df["id"]

    # fetch additional_info (not in main sessions' endpoint) - need for ticket_system_id
    for idx, row in df.iterrows():
        # additional_info
        session_meta = et.sessions.get_session(session_id=row["id"])
        df.at[idx, "additional_info"] = session_meta.get("additional_info")
        if idx % 100 == 0:
            print(f"Done: {idx} of {len(df)}")

    return df

def download_transcripts(et, df):
    print("Downloading transcripts:")
    df["text"] = ""
    for idx, row in df.iterrows():
        print(f'Downloading transcript for session {row["id"]}')
        transcript = et.sessions.get_transcripts(session_id=row["id"])
        # return transcript
        df.at[idx, "text"] = transcript["items"]
        if idx % 200 == 0:
            print(f"\tProgress downloading transcripts: {idx} of {len(df)}, {round(idx / len(df) * 100)}%")
    return df


def download_summaries(et, df):
    print("Downloading summaries:")
    df["summary"] = ""
    for idx, row in df.iterrows():
        session_id = row["id"]
        url = f'/sessions/{session_id}/summary'
        summary = et.http_client.get(url)
        if idx % 200 == 0:
            print(f"\tProgress downloading summaries: {idx} of {len(df)}, {round(idx / len(df) * 100)}%")
        if not summary:
            continue
        df.at[idx, "summary"] = summary
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get summaries for day and save to file")
    parser.add_argument("--user", default="et-user-email@org.com")
    parser.add_argument("--domain", default="app.enderturing.com")
    parser.add_argument("--get-last-n-days", default=7)
    parser.add_argument("--save-to", default="./")
    args = parser.parse_args()

    # 1 - Log in to ET system
    et = init_et(domain=args.domain, user=args.user)

    # 2 - Get today to make date range filter
    now_dt = datetime.now()
    start_dt = (now_dt - timedelta(days=args.get_last_n_days)).strftime("%Y-%m-%d")
    stop_dt = now_dt.strftime("%Y-%m-%d")

    # 3 - Download conversations (without transcripts and summaries, only list of conversations with metadata)
    df = get_et_sessions(
        et,
        start_dt=start_dt,
        stop_dt=stop_dt,
        filters='',  # no filters - means all conversations for a day, copy filter from UI to download only necessary
    )

    # 4 - Append additional metadata (like Team, Agent ful name, link to ET, etc)
    df = enrich_df_et(et, df)

    # 5 - Download transcripts - uncomment if needed
    df = download_transcripts(et, df)

    # 6 - Download summaries
    df = download_summaries(et, df)

    # 7 - Save results to file
    save_to = Path(args.save_to)
    dst_fname = save_to / f"summaries_{start_dt}_{stop_dt}.csv"
    print(f"Saving to", dst_fname)
    save_to.mkdir(parents=True, exist_ok=True)
    df.to_csv(dst_fname, index=False)

    # Additional API examples
    # print(len(df), df.head())
    scorecards_dict = et.scorecards.get_scorecards()
    # print(scorecards_dict)
    agents_dict = et.agent.get_agents()
    # print(agents_dict)
    groups_dict = et.agent.get_groups()
    # print(groups_dict)
