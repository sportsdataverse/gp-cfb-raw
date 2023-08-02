import argparse
import concurrent.futures
import gc
import logging
import os
import time
from itertools import repeat
from pathlib import Path

import pandas as pd
import polars as pl
import pyreadr
import sportsdataverse as sdv

logging.basicConfig(level=logging.DEBUG, filename="gp_cfb_raw_logfile.txt")
logger = logging.getLogger(__name__)

path_to_schedules = "cfb/schedules"
final_file_name = "cfb/cfb_schedule_master.parquet"
MAX_THREADS = 30


def download_game_schedules(seasons, path_to_schedules):
    threads = min(MAX_THREADS, len(seasons))

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        return list(executor.map(download_schedule, seasons, repeat(path_to_schedules)))


def download_schedule(season, path_to_schedules=None):
    logger.info(f"Scraping CFB schedules for year {season}...")
    df = sdv.cfb.espn_cfb_calendar(season, ondays=True)
    calendar = df["dateURL"].str.replace("-", "").str.replace("-", "")
    ev = pl.DataFrame()
    for d in calendar:
        date_schedule = sdv.cfb.espn_cfb_schedule(dates=d)
        if date_schedule is not None:
            if "away_records" in date_schedule.columns:
                date_schedule = date_schedule.drop(["away_records"])
            if "home_records" in date_schedule.columns:
                date_schedule = date_schedule.drop(["home_records"])
            if "venue_capacity" in date_schedule.columns:
                date_schedule = date_schedule.with_columns(
                    venue_capacity=pl.col("venue_capacity").cast(pl.Int32),
                    week=pl.col("week").cast(pl.Int32),
                )
            logging.debug(f"Scraped {len(date_schedule)} games for {d}")
            ev = pl.concat([ev, date_schedule], how="diagonal")
    ev = ev.filter(pl.col("season_type").is_in([2, 3]))
    # ev = ev.drop("competitors", axis=1)
    # ev = ev.drop_duplicates(subset=["game_id"], ignore_index=True)

    Path(f"{path_to_schedules}/parquet").mkdir(parents=True, exist_ok=True)
    Path(f"{path_to_schedules}/rds").mkdir(parents=True, exist_ok=True)
    Path(f"{path_to_schedules}/csv").mkdir(parents=True, exist_ok=True)
    if path_to_schedules is not None:
        ev.to_parquet(f"{path_to_schedules}/parquet/cfb_schedule_{season}.parquet", index=False)
        pyreadr.write_rds(f"{path_to_schedules}/rds/cfb_schedule_{season}.rds", ev, compress="gzip")
        ev.to_csv(f"{path_to_schedules}/csv/cfb_schedule_{season}.csv", index=False)


def main():
    start_year = max(args.start_year, 2002)
    end_year = start_year if args.end_year is None else args.end_year
    years_arr = range(start_year, end_year + 1)
    print(f"Rescrape Schedule? {args.rescrape}")
    if args.rescrape == True:
        t0 = time.time()
        download_game_schedules(years_arr, path_to_schedules)
        t1 = time.time()
        logger.info(f"{(t1-t0)/60} minutes to download {len(years_arr)} years of season schedules.")

    parquet_files = [
        pos_parquet.replace(".parquet", "")
        for pos_parquet in os.listdir(path_to_schedules + "/parquet")
        if pos_parquet.endswith(".parquet")
    ]
    glued_data = pd.DataFrame()
    for index, js in enumerate(parquet_files):
        x = pd.read_csv(f"{path_to_schedules}/csv/{js}.csv")
        glued_data = pd.concat([glued_data, x], axis=0, ignore_index=True)
    glued_data["status_display_clock"] = glued_data["status_display_clock"].astype(str)
    glued_data.to_parquet(final_file_name, index=False)
    gc.collect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_year", "-s", type=int, required=True, help="Start year of CFB Schedule period (YYYY)")
    parser.add_argument("--end_year", "-e", type=int, help="End year of CFB Schedule period (YYYY)")
    parser.add_argument(
        "--rescrape",
        "-r",
        default=True,
        action="store_true",
        help="Rescrape all games in the schedule period",
    )
    args = parser.parse_args()

    main()
