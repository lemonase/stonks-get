import argparse
import os
import praw
import datetime as dt
from psaw import PushshiftAPI
import csv
import sqlite3

# TODO: look inside of these sub-reddits whith praw as well as psaw
sub_reddits = [
    "pennystocks",
    "RobinHoodPennyStocks",
    "Daytrading",
    "StockMarket",
    "stocks",
    "investing",
    "wallstreetbets",
]


def create_db():
    """
    Opens a connection, cursor and creates "stocks" table if it
    doesn't already exist
    """
    con = sqlite3.connect("wsb_tags.db")
    cur = con.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS stocks
        (
            dt TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            ticker TEXT NOT NULL,
            message TEXT NOT NULL,
            source TEXT NOT NULL,
            UNIQUE(source)
        )
        """)

    return (con, cur)


def gather_nasdaq_tickers():
    """ Returns a set of valid NYSE stock tickers from a csv """
    # create get stocks list
    tickers = set()

    with open(
            os.path.join(os.path.dirname(os.path.realpath(__file__)),
                         "data/nasdaq.csv")) as ticker_csv:
        t_reader = csv.reader(ticker_csv)
        for row in t_reader:
            tickers.add(row[0])

    return tickers


def get_subs_list(subreddit, count):
    """ Returns a list of <count> submissions from <subreddit> using PSAW """
    ps_api = PushshiftAPI()

    cur_year = int(dt.datetime.now().year)
    cur_month = int(dt.datetime.now().month)
    # cur_day = int(dt.datetime.now().day)

    start_epoch = int(dt.datetime(cur_year, cur_month, 1).timestamp())

    subs = list(
        ps_api.search_submissions(
            after=start_epoch,
            subreddit=subreddit,
            filter=["url", "author", "title", "subreddit"],
            limit=count,
        ))

    return subs


def add_subs_to_db(con, cur, subs):
    """ Takes a list of submissions and inserts them into the db """
    # get a list of valid ticker symbols
    ticker_set = gather_nasdaq_tickers()

    for sub in subs:
        words = sub.title.split()
        for word in words:
            if word in ticker_set:
                post_date = dt.datetime.fromtimestamp(sub.created_utc)
                try:
                    cur.execute(
                        """
                        INSERT OR IGNORE INTO stocks VALUES (?, ?, ? ,?)
                        """,
                        (post_date, word, sub.title, sub.url),
                    )
                    con.commit()
                except Exception as e:
                    print(e)
                    con.rollback()


def print_table(con, cur, table, pretty=False):
    if pretty:
        import pandas as pd
        print(pd.read_sql_query(f"SELECT * FROM {table}", con))
    else:
        for row in cur.execute(f"SELECT * FROM {table}"):
            print(row)


def get_args():
    parser = argparse.ArgumentParser(description="stonks-get.py")
    parser.add_argument("--fetch", action=argparse.BooleanOptionalAction)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--print", action=argparse.BooleanOptionalAction)
    parser.add_argument("--pprint", action=argparse.BooleanOptionalAction)

    return parser.parse_args()


def main():
    """ Main function """

    args = get_args()

    # get connection and cursor for db
    con, cur = create_db()

    # returns submissions from a subreddit
    if args.fetch:
        subs = get_subs_list("wallstreetbets", args.limit)
        add_subs_to_db(con, cur, subs)
        con.commit()
    if args.print:
        print_table(con, cur, "stocks")
    if args.pprint:
        print_table(con, cur, "stocks", pretty=True)

    con.close()


main()
