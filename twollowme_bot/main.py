import argparse
from datetime import datetime
from typing import Tuple

import yaml

from database.sqlite import Database
from twitter.client import TwitterV1Api, parse_user


def parse_date(d: str):
    return datetime.strptime(d, "%Y-%m-%d")


def parse_args() -> Tuple:
    parser = argparse.ArgumentParser(
        description="Analyse Posts from a twitter search",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "-q",
        "--query",
        dest="query",
        default=None,
        help="twitter search query",
    )
    parser.add_argument(
        "-l",
        "--lang",
        dest="lang",
        default="en",
        help="Language (default: en)",
    )
    parser.add_argument(
        "-s",
        "--start",
        dest="start",
        default=None,
        type=parse_date,
        help="start date (format: yyyy-mm-dd)",
    )
    parser.add_argument(
        "-e",
        "--end",
        dest="end",
        default=None,
        type=parse_date,
        help="end date (format: yyyy-mm-dd)",
    )
    parser.add_argument(
        "-n",
        "--limit",
        dest="limit",
        default=None,
        type=int,
        help="limit number of top level tweets",
    )
    parser.add_argument(
        "-c",
        "--config",
        dest="config",
        default=".config.yaml",
        help="Path to config file (Default: config.yaml)",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    with open(args.config, encoding="utf8") as stream:
        config = yaml.safe_load(stream)
        twitter = TwitterV1Api(**config["auth"])

        with Database(**config["database"]) as db:
            db.setup()
            for page in twitter.get_friends():
                db.update_friends([parse_user(user) for user in page])
                db.conn.commit()


if __name__ == "__main__":
    main()
