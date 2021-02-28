import praw
from psaw import PushshiftAPI

# More libraries to integrate
# finviz https://github.com/mariostoev/finviz
# yfinance https://github.com/ranaroussi/yfinance

# Goals:
# ------
# Get DD and watchlists from these subreddits
# filter out shitposts
# output a list of tickers to research

sub_reddits = [
    "pennystocks",
    "RobinHoodPennyStocks",
    "Daytrading",
    "StockMarket",
    "stocks",
    "investing",
    "wallstreetbets",
]


def main():
    for sub in sub_reddits:
        print(sub)


if __name__ == "__main__":
    main()
