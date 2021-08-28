"""
UPLOADER.py

mangages google calendar and twitter account.
"""
import Manager as M

import datetime, json, os.path, pytz
import numpy as np
import pandas as pd

from coo import Coo # tweet scheduler
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

DAILY_TWEET_TIME="08:00"
MINS_BEFORE_LIFT=15 # how many minutes before a lift to tweet
HASHTAGS=["#GetLifting","#ItsLiftingTime","#LiftingAllDay","#LiftIsLife","#YouOnlyLiftOnce","#GottaGetLifting"]

def find_todays_lifts(lift_data_df,today=None):
    today=datetime.datetime.now().date() if today is None else today
    todays_lifts=(lift_data_df["date"].dt.date==today)
    todays_lifts=lift_data_df[todays_lifts]

    return todays_lifts

"""
ACCESS GOOGLE CALENDAR API
"""
def connect_to_gcal():
    # access required
    SCOPES = ['https://www.googleapis.com/auth/calendar.events',"https://www.googleapis.com/auth/calendar.readonly"]
    creds=None

    # credential file exists
    if os.path.exists("token.json"):
        creds=Credentials.from_authorized_user_file("token.json",SCOPES)

    # no valid credentials (may open browser to confirm scopes)
    if (not creds) or (not creds.valid):

        if creds and creds.expired and creds.refresh_token: # refresh credentials
            creds.refresh(Request())
        else: # new credentials
            flow=InstalledAppFlow.from_client_secrets_file("google_credentials.json",SCOPES)
            creds=flow.run_local_server(port=0)

        # save credentials
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    # build api service
    service = build('calendar', 'v3', credentials=creds)

    return service

"""
TWITTER
"""
def generate_daily_tweet_schedule(lift_data_df:pd.DataFrame) -> [(str,None,str)]:

    if (lift_data_df.shape[0]==0): return []

    # assumes tweets are in order
    lift_data_df["day"]=lift_data_df["date"].dt.date
    lift_data_df["time"]=lift_data_df["date"].dt.time
    group_by_day=lift_data_df.groupby(by="day",axis=0)
    day_data=group_by_day["time"].agg([lambda x:len(np.unique(x)),lambda x:list(np.unique(x))])

    first_date=datetime.datetime.now().date()
    last_date=lift_data_df["day"].sort_values(ascending=True).iloc[-1]

    delta=last_date-first_date

    schedule=[]
    for i in range(delta.days+1):
        day=first_date+datetime.timedelta(days=i)

        # scheduled time for tweet
        date_str=day.strftime("%Y-%m-%d")
        tweet_time="{} {}".format(date_str,DAILY_TWEET_TIME)

        if day in day_data.index: # lifts occur
            r=day_data.loc[day,:]
            lift_count=r["<lambda_0>"]
            lift_times=r["<lambda_1>"]
            lift_times_strs=[t.strftime("%H:%M") for t in lift_times]
            tweet_text="{}.\nTower Bridge will lift {} time{} today".format(date_str,lift_count,"" if lift_count==1 else "s")

            if lift_count==0:
                tweet_text+="."
            elif lift_count==1:
                tweet_text+=", at {}.".format(lift_times_strs[0])
            else:
                tweet_text+=", at {} & {}.".format(", ".join(lift_times_strs[:-1]),lift_times_strs[-1])

            tweet_text+="\n\n#TowerBridge #London #MorningReport"
            hashtags=np.random.choice(HASHTAGS,size=(2),replace=False)
            tweet_text+=" "+" ".join(hashtags)


        else: # no lifts
            tweet_text="No lifts today folks :(\n\n#ABoyGottaRest"

        schedule.append((tweet_time,None,tweet_text)) # None for tweet template


    return schedule

def generate_individual_lift_tweet_schedule(lift_data_df:pd.DataFrame) -> [(str,None,str)]:

    if (lift_data_df.shape[0]==0): return []

    schedule=[]
    for _,r in lift_data_df.iterrows():
        tweet_text,tweet_time=generate_lift_tweet(r)
        schedule.append((tweet_time,None,tweet_text))

    return schedule

def generate_lift_tweet(event_dict):
    time_str=event_dict["date"].strftime("%H:%M")
    vessel_name=event_dict["vessel_name"]
    direction=event_dict["direction"].lower()

    tweet_text="Tower Bridge will lift at {} to allow {} to travel {}. Enjoy the show!\n\n#TowerBridge #London".format(time_str,vessel_name,direction)
    hashtags=np.random.choice(HASHTAGS,size=(2),replace=False)
    tweet_text+=" "+" ".join(hashtags)

    tweet_time=event_dict["date"]-datetime.timedelta(minutes=MINS_BEFORE_LIFT)
    tweet_time_str=tweet_time.strftime("%Y-%m-%d %H:%M")

    return tweet_text,tweet_time_str

def schedule_tweets(schedule):

    twitter_creds=json.load(open("twitter_credentials.json"))

    at = Coo(
        twitter_creds["api_key"],
        twitter_creds["api_secret_key"],
        twitter_creds["access_token"],
        twitter_creds["access_token_secret"]
        )

    at.schedule(schedule,time_zone="Europe/London")

def todays_twitter(todays_events):
    # create schedule
    daily_schedule=generate_daily_tweet_schedule(todays_events) # day summary
    inidividual_schedule=generate_individual_lift_tweet_schedule(todays_events)
    joint_schedule=daily_schedule+inidividual_schedule

    # remove tweets which should have alredy been sent
    now=datetime.datetime.now()
    final_schedule=[]
    for tweet in joint_schedule:
        dt=datetime.datetime.strptime(tweet[0],"%Y-%m-%d %H:%M")
        if dt>now: final_schedule.append(tweet)
        else: print("Skipping ",tweet)

    # print twitter schedule
    if len(final_schedule)!=0:
        print("TODAY'S SCHEDULE")
        for x in final_schedule: print(x)
        schedule_tweets(final_schedule)
    else:
        print("EMPTY schedule")

"""
MAINTAIN CALENDAR
"""
def prepare_calendar_event(details) -> dict:
    local_timezone=pytz.timezone("Europe/London")
    local_dt=local_timezone.localize(details["date"]).astimezone(pytz.utc)
    start_date_str=local_dt.strftime("%Y-%m-%dT%H:%M:00Z")

    end_date_str=(local_dt+datetime.timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:00Z")
    event={
        "summary":"Tower Bridge Lift",
        "location":"Tower Bridge Rd, London SE1 2UP",
        "description":"Tower Bridge will lift to allow {} to travel {}.".format(details["vessel_name"],details["direction"]),
        "start":{
            "dateTime":start_date_str,
            "timeZone":"Europe/London"
        },
        "end":{
            "dateTime":end_date_str,
            "timeZone":"Europe/London"
        },
    }
    return event

def get_event_list(calendar_id,service) ->[dict]:
    event_list=service.events().list(calendarId=calendar_id).execute()["items"]
    return event_list

def parse_google_event(event_dict) -> dict:
    # parse event from `get_event_list`
    event={
        "start_dateTime":event_dict["start"]["dateTime"],
        "description":event_dict["description"]
    }

    return event

def add_event(event_dict:dict,calendar_id:str,service) -> str:
    return service.events().insert(calendarId=calendar_id, body=event_dict).execute()

def add_events(new_events:[dict],calendar_id,service,existing_events=None):

    counter=0
    for (i,new_e) in enumerate(new_events):
        print("{}/{} (new={})".format(i,len(new_events),counter),end="\r")
        new_e_dict=prepare_calendar_event(new_e)
        # check if event already exists
        if existing_events is not None:

            already_exist=False
            for e in existing_events:
                if (e["start_dateTime"]==new_e_dict["start"]["dateTime"]) and (e["description"]==new_e_dict["description"]):
                    already_exist=True
                    break

            if not already_exist:
                counter+=1
                add_event(new_e_dict,calendar_id,service)

        print("{}/{} (new={})".format(i,len(new_events),counter),end="\r")

"""
MAIN
"""
def update_calendar():
    # update calendar with events recored locally

    service=connect_to_gcal()

    # get list of calendars
    calendar_list=service.calendarList().list(pageToken=None).execute()["items"]

    # find calendar ID
    calendar_id=None
    for c in calendar_list:
        if c["summary"]=="Tower Bridge Lift Times":
            calendar_id=c["id"]
            break

    if calendar_id is None:
        raise Exception("Calendar cannot be found.")
        return None

    # read events from db
    lift_data=M.load_data("lift_data.csv")
    lift_data=list(lift_data.T.to_dict().values()) # list of dicts

    existing_events=get_event_list(calendar_id,service)
    existing_events=[parse_google_event(e) for e in existing_events]

    add_events(lift_data,calendar_id,service,existing_events)

def today(today=None):
    # set up schedule for todays tweets (set `today` to whichever day you wish to scheule for. defaults to today)
    # e.g. today="2021-08-27"

    # update data
    M.full_update(file_path="lift_data.csv")
    update_calendar()

    # todays tweets
    lift_data_df=M.load_data("lift_data.csv")
    today=datetime.datetime.now().date() if today is None else today

    lift_data_df=M.load_data("lift_data.csv")
    todays_lifts=find_todays_lifts(lift_data_df,today=today)
    todays_twitter(todays_lifts)

if __name__=="__main__":
    # update_calendar()
    today()
