"""
MANAGER.py

manage the local databases.
two databases are used:
 - Lift schedule
 - Tweet schedule
both databases are saved locally as .csv files
"""
import Fetcher as F
import pandas as pd
from datetime import datetime,timedelta
from secrets import PROJECT_PATH

"""
HELPER
"""
def load_data(filename:str) -> pd.DataFrame:
    return pd.read_csv(filename,index_col=0,parse_dates=["date"]).reset_index(drop=True)

def save_data(df:pd.DataFrame,file_path=PROJECT_PATH+"lift_data.csv"):
    df.to_csv(file_path)

"""
LIFT TIMES
"""
def find_new_lifts(new_data:pd.DataFrame,cur_db:pd.DataFrame) -> pd.DataFrame:
    # identify lifts which are in `new_data` but not in `cur_db`
    merged_df=pd.merge(new_data,cur_db,on=["date","vessel_name","direction"],how="left",indicator="Exist")

    new_rows=merged_df["Exist"]=="left_only" # rows only in `new_data`
    if new_rows.sum()==0: return None

    new_df=new_data[new_rows]
    return new_df

def find_removed_lifts(new_data:pd.DataFrame,cur_db:pd.DataFrame) -> pd.DataFrame:
    # identify lifts which are not in `new_data` but are in `cur_db`
    # usually due to lift having happened (but may have been cancelled?)
    dt_now=datetime.now()

    merged_df=cur_db.reset_index().merge(new_data,on=["date","vessel_name","direction"],how="left",indicator="Exist").set_index("index")

    removed_rows=merged_df["Exist"]=="left_only" # rows only in `cur_db`
    already_happened_rows=(merged_df["date"]<dt_now)

    cancelled_rows=(removed_rows & ~already_happened_rows)

    if cancelled_rows.sum()==0: return None

    removed_df=cur_db[cancelled_rows]
    return removed_df

def identify_cancelled_lift(removed_lifts) -> pd.DataFrame:
    # return lifts which have been removed and possibly cancelled (as proposed data has not passed)
    if removed_lifts is None: return None

    today_date=datetime.now()
    cancelled_rows=removed_lifts["date"]>today_date
    cancelled_lifts=removed_lifts[cancelled_rows]

    return cancelled_lifts

def update_data(cur_data,new_lifts,cancelled_lifts) -> pd.DataFrame:
    updated_data=None

    # no updates to perform
    if (cancelled_lifts is None) and (new_lifts is None): return cur_data

    # remove cancelled lifts
    if cancelled_lifts is not None:
        merged_df=pd.merge(cancelled_lifts,cur_data,on=["date","vessel_name","direction"],how="right",indicator="Exist")
        cancelled_rows=(merged_df["Exist"]=="both")

        updated_data=cur_data.loc[~cancelled_rows,["date","vessel_name","direction"]]

    # add new lifts
    if new_lifts is not None:
        if updated_data is not None:
            updated_data=updated_data.append(new_lifts)
        else:
            updated_data=cur_data.append(new_lifts)

    # sort by date (asc)
    updated_data=updated_data.sort_values(by="date").reset_index(drop=True)

    return updated_data

def full_update(file_path,printing=True):
    cur_db=load_data(file_path)
    new_data=pd.DataFrame(F.fetch_listed_lifts())
    new_data=new_data.reset_index(drop=True)

    new_lifts=find_new_lifts(new_data,cur_db)
    removed_lifts=find_removed_lifts(new_data,cur_db)
    cancelled_lifts=identify_cancelled_lift(removed_lifts)

    if printing:
        print("{} lifts originally. {} new lifts found. {} lifts removed, of which {} are believed to be cancelled.".format(
            cur_db.shape[0],
            0 if new_lifts is None else len(new_lifts),
            0 if removed_lifts is None else len(removed_lifts),
            0 if cancelled_lifts is None else len(cancelled_lifts)))

    updated_data=update_data(cur_db,new_lifts,cancelled_lifts)
    save_data(updated_data,file_path=file_path)

"""
TWITTER
"""

if __name__=="__main__":
    full_update(file_path=PROJECT_PATH+"lift_data.csv")
