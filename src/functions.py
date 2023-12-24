import time
import pandas as pd
from src.utils import timer, cache, decode_time
import polyline
import datetime

#this version prints args
@timer()
def example_func_sleep(x, y):
    time.sleep(x)
    time.sleep(y)
    return x+y


#this version does note prints args
@timer(False)
def example_func_sleep(x, y):
    time.sleep(x)
    time.sleep(y)
    return x+y


def process_routes(r_df):
    id2route = dict()
    id2times = dict()

    for ind, row in r_df.iterrows():
        id2route[row["_id_oid"]] = polyline.decode(row["encoded_latlons"], precision=7)
        id2times[row["_id_oid"]] = [row["start_time"]+datetime.timedelta(seconds=i) if i else None for i in decode_time(row["encoded_time"])] if (not decode_time(row["encoded_time"]) is None) else [None for i in range(len(id2route[row["_id_oid"]]))]

    out=[]
    for id in id2route:
        for wp, ts in zip(id2route[id], id2times[id]):
            temp = dict()
            temp["order"] = id
            temp["lon"] = wp[1]
            temp["lat"] = wp[0]
            temp["time"] = ts
            out.append(temp)

    return pd.DataFrame(out)