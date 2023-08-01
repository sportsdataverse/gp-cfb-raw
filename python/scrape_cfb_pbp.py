import argparse
import concurrent.futures
import gc
import json
import logging
import os
import time
import traceback
import warnings
from itertools import repeat
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import pyreadr
from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

warnings.filterwarnings("ignore")

logging.basicConfig(level=logging.INFO, filename="gp_cfb_raw_logfile.txt")
logger = logging.getLogger(__name__)

path_to_final = "cfb/pbp/json/final"

path_to_errors = "cfb/errors"
path_to_schedules = "cfb/schedules"
final_file_name = "cfb/cfb_schedule_master.parquet"
github_url_prefix = "https://raw.githubusercontent.com/sportsdataverse/gp-cfb-raw/main/"
run_processing = True
rescrape_all = False
MAX_THREADS = 4


def download_game_pbps_for_gh_release(games, process, path_to_final):
    threads = min(MAX_THREADS, len(games))

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        return list(executor.map(download_game_for_gh_release, games, repeat(process), repeat(path_to_final)))


def download_game_for_gh_release(game, process, path_to_final):
    # this finds our json files
    path_to_final_json = f"{path_to_final}/"
    Path(path_to_final_json).mkdir(parents=True, exist_ok=True)

    try:
        result = cfb_pbp_processing(game_id=game)

        fp = f"{path_to_final_json}{game}.json"
        with open(fp, "w") as f:
            json.dump(result, f, indent=0, sort_keys=False)
    except FileNotFoundError:
        logger.exception(f"FileNotFoundError: game_id = {game}\n {traceback.format_exc()}")
    except TypeError:
        logger.exception(f"TypeError: game_id = {game}\n {traceback.format_exc()}")
    except IndexError:
        logger.exception(f"IndexError:  game_id = {game}\n {traceback.format_exc()}")
    except KeyError:
        logger.exception(f"KeyError: game_id =  game_id = {game}\n {traceback.format_exc()}")
    except ValueError:
        logger.exception(f"DecodeError: game_id = {game}\n {traceback.format_exc()}")
    except AttributeError:
        logger.exception(f"AttributeError: game_id = {game}\n {traceback.format_exc()}")

    time.sleep(0.5)


def add_game_to_schedule(schedule, year):
    game_files = [int(game_file.replace(".json", "")) for game_file in os.listdir(path_to_final)]
    schedule["game_json"] = schedule["game_id"].astype(int).isin(game_files)
    schedule["game_json_url"] = np.where(
        schedule["game_json"] == True,
        schedule["game_id"].apply(
            lambda x: f"https://raw.githubusercontent.com/sportsdataverse/gp-cfb-raw/main/cfb/json/final/{x}.json"
        ),
        None,
    )
    print(schedule)
    schedule.to_parquet(f"cfb/schedules/parquet/cfb_schedule_{year}.parquet", index=None)
    pyreadr.write_rds(f"cfb/schedules/rds/cfb_schedule_{year}.rds", schedule, compress="gzip")
    return


def cfb_pbp_processing(game_id):
    processed_data = CFBPlayProcess(
        gameId=game_id,
        return_keys=[
            "plays",
            "advBoxScore",
            "winprobability",
            "homeTeamSpread",
            "overUnder",
            "boxscore",
            "header",
            "broadcasts",
            "pickcenter",
            "gameInfo",
            "season",
        ],
    )
    pbp = processed_data.espn_cfb_pbp()
    logger.info(f"{game_id}: cfb_pbp processing pipeline")
    processed_data.run_processing_pipeline()
    pbp = processed_data.json
    return pl.DataFrame(pbp.get("plays"), infer_schema_length=400)


def main():
    start_year = max(args.start_year, 2004)
    end_year = start_year if args.end_year is None else args.end_year
    process = args.process
    years_arr = range(start_year, end_year + 1)

    for year in years_arr:
        schedule = pd.read_parquet(
            f"{path_to_schedules}/parquet/cfb_schedule_{year}.parquet", engine="auto", columns=None
        )
        schedule = schedule.sort_values(by=["season", "season_type"], ascending=True)
        schedule["game_id"] = schedule["game_id"].astype(int)
        schedule = schedule[schedule["status_type_completed"] == True]
        if args.rescrape == False:
            game_files = [int(game_file.replace(".json", "")) for game_file in os.listdir(path_to_final)]
            schedule = schedule[~schedule["game_id"].isin(game_files)]
        schedule = schedule[schedule["season"] >= 2004]
        logger.info(f"Scraping CFB PBP for {year}...")
        games = schedule[(schedule["season"] == year)].reset_index()["game_id"]

        if len(games) == 0:
            logger.info(f"{len(games)} Games to be scraped, skipping")
        elif len(games) > 0:
            logger.info(f"Number of Games: {len(games)}")
            t0 = time.time()
            download_game_pbps_for_gh_release(games, process, path_to_final)
            t1 = time.time()
            logger.info(f"{(t1-t0)/60} minutes to download {len(games)} game play-by-plays.")

        logger.info(f"Finished CFB PBP for {year}...")

        schedule = add_game_to_schedule(schedule, year)

    gc.collect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_year", "-s", type=int, required=True, help="Start year of CFB Schedule period (YYYY)")
    parser.add_argument("--end_year", "-e", type=int, help="End year of CFB Schedule period (YYYY)")
    parser.add_argument("--rescrape", "-r", type=bool, default=True, help="Rescrape all games in the schedule period")
    parser.add_argument(
        "--process", "-p", type=bool, default=True, help="Run processing pipeline for games in the schedule period"
    )
    args = parser.parse_args()

    main()
