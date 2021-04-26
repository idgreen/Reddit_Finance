import pandas as pd
import praw
from psaw import PushshiftAPI
import datetime
import itertools
import collections
from collections import defaultdict
import pprint
from itertools import dropwhile
import yfinance as yf
import argparse


parser = argparse.ArgumentParser(description="Script to retreive Reddit data.", fromfile_prefix_chars="@")

parser.add_argument("--client-id", dest="client_ident",type=str, required=True)
parser.add_argument("--client-secret", dest="client_sec",type=str, required=True)
parser.add_argument("--subreddit", dest="subreddit",type=str, required=True)
parser.add_argument("--max_response", dest="max_response",type=int, required=True)
parser.add_argument("--lookback_days", dest="lookback_days",type=int, required=True)
parser.add_argument("--count_threshold", dest="count_threshold",type=int, required=True)

args = parser.parse_args()


reddit = praw.Reddit(client_id=args.client_ident,
client_secret=args.client_sec,
user_agent="WSB Ticker Script")


api = PushshiftAPI(reddit,rate_limit_per_minute=None)
start_epoch=datetime.datetime.now() - datetime.timedelta(days=args.lookback_days)
print(f"Parsing from: " + str(start_epoch))
print(f"Getting the newest " + str(args.max_response) + " comments from: /r/" + str(args.subreddit) + " for the past " + str(args.lookback_days) + " days... (this will take a few mins)")
start_timestamp=int(start_epoch.timestamp())
gen = list(api.search_comments(after=start_timestamp,
                        subreddit=args.subreddit,
                        filter=['body'],
                        limit=args.max_response))
cache = []
for c in gen:
    cache.append(c)
    # Omit this test to actually return all results. Wouldn't recommend it though: could take a while, but you do you.
    if len(cache) >= args.max_response:
        break
# If you really want to: pick up where we left off to get the rest of the results.
if False:
    for c in gen:
        cache.append(c)

print(f"Total Comments Found: " + str(len(cache)))
df = pd.DataFrame([thing.body for thing in cache])

# Extract all comments with 3 or 4 capital letters
edf = pd.DataFrame(df[0].str.findall(r'\b[A-Z]{4}\b|\b[A-Z]{3}\b|\b[A-Z]{2}\b|\b[A-Z]{1}\b[.!?]?'))

# Cleanup dataframe
edf.columns = ['tickers']
edf = edf[edf['tickers'].map(lambda d: len(d)) > 0]
edf = edf.tickers.tolist()
tickers = list(itertools.chain.from_iterable(edf))

# Pretty print json
# pp = pprint.PrettyPrinter(indent=4)
ctr = collections.Counter(tickers)
for key, count in dropwhile(lambda key_count: key_count[1] >= args.count_threshold, ctr.most_common()):
    del ctr[key]

#Pull Top 10 Most Common Tickers
print(f"Retreving Trading Volume Data...")

dict_ = defaultdict(list)
for key in ctr:
    try:
        print(key)
        vol_data = yf.download(tickers=key,period=str(args.lookback_days)+"d",interval='1d',silent=True)['Volume']
        vol_chg = round(float(((vol_data[-1] - vol_data[0])/vol_data[0]) * 100),2)
        price_data = yf.download(tickers=key,period=str(args.lookback_days)+"d",interval='1d',silent=True)['Close']
        price_chg = round(float(((price_data[-1] - price_data[0])/price_data[0]) * 100),2)
        dict_[key].append(ctr[key])
        dict_[key].append(price_chg)
        dict_[key].append(vol_chg)
    except:
        dict_.pop(key, None)
pd.set_option('display.max_rows', None)
df = pd.DataFrame([(k, v[0], v[1], v[2]) for k, v in dict_.items()],  
                columns=['Ticker', 'Reddit Comments', 'Price Chg %', 'Vol Chg %'])
print(f"_____________________________________________________")
print(f"")
print(f"")
print(f"   SUMMARY   ")
print(f"Retrieved limit of " + str(len(cache)) + " comments (newest to oldest), starting from " + str(start_epoch))
print(df.sort_values(by=['Reddit Comments'],ascending=False))