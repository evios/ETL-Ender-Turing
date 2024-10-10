import urllib
import pandas as pd
import getpass

from enderturing import Config, EnderTuring

def init_et(domain="app.enderturing.com", user="et-user-email@org.com"):
    password = getpass.getpass(f'Provide Password for {domain}:')
    config = Config.from_url(f"https://{user}:{password}@{domain}")
    et = EnderTuring(config)
    return et


def get_et_sessions(
    et,
    limit=10_000,
    start_dt="2024-04-15",
    stop_dt="2024-04-21",
    use_num_sessions_endpoint=False,
    filters=None,
    debug=False,
):
    # Filters
    # date
    url = f"date_range,{start_dt},{stop_dt}||"
    # Other defined external filters
    if filters:
        url += f"±{filters}"
    print(f"Requesting ET filters: {url}")

    if use_num_sessions_endpoint:
        # through number_of_sessions endpoint - just return number of matched records, no details
        url = f'/sessions/filter/number_of_sessions?filters=' + urllib.parse.quote(url)
        response = et.http_client.get(url)
        if debug:
            print(response)
        return response  # [sessions, leads]
    else:
        # through general sessions endpoint - most detailed results
        url = f'/sessions?skip=0&limit={limit}&filters=' + urllib.parse.quote(url)
        response = et.http_client.get(url)
        print(f'Got {len(response["items"])} results')
        if debug:
            print(response)
        et_sessions = pd.DataFrame(response["items"])
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

    df["ticket_system_url"] = df["additional_info"].apply(lambda x: x.get("ticket_system_url"))
    df["ticket_system_id"] = df["additional_info"].apply(
        lambda x: int(x.get("ticket_system_id")) if x.get("ticket_system_id") else None
    )

    return df

et = init_et(domain="app.enderturing.com", user="et-user-email@org.com")
df = get_et_sessions(
    et,
    start_dt="2024-04-15",
    stop_dt="2024-04-21",
    filters="reviewers,true"
)
# print(len(df), df.head())
scorecards_dict = et.scorecards.get_scorecards()
# print(scorecards_dict)
agents_dict = et.agent.get_agents()
# print(agents_dict)
groups_dict = et.agent.get_groups()
# print(groups_dict)
df = enrich_df_et(et, df)
