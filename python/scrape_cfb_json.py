import argparse
import concurrent.futures
import gc
import json
import logging
import os
import time
import traceback
from itertools import repeat
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import pyreadr
from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

logging.basicConfig(level=logging.INFO, filename="gp_cfb_raw_logfile.txt", filemode="w")
logger = logging.getLogger(__name__)
np.set_printoptions(threshold=np.inf)
pd.set_option("display.max_rows", 100)

path_to_final = "cfb/json/final"
path_to_errors = "cfb/errors"
path_to_schedules = "cfb/schedules"
final_file_name = "cfb/cfb_schedule_master.parquet"
github_url_prefix = "https://raw.githubusercontent.com/sportsdataverse/gp-cfb-raw/main/"
run_processing = True
rescrape_all = False
MAX_THREADS = 30


def download_game_pbps(games, process, path_to_final):
    threads = min(MAX_THREADS, len(games))

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        return list(executor.map(download_game, games, repeat(process), repeat(path_to_final)))


def download_game(game, process, path_to_final):
    # this finds our json files
    path_to_final_json = f"{path_to_final}/"
    Path(path_to_final_json).mkdir(parents=True, exist_ok=True)

    try:
        result = postprocessing(game_id=game)

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
    schedule = schedule.astype({col: "int32" for col in schedule.select_dtypes("int64").columns})
    schedule = schedule.astype({col: "float32" for col in schedule.select_dtypes("float64").columns})
    schedule = schedule.fillna(value=np.nan)

    schedule.to_parquet(f"cfb/schedules/parquet/cfb_schedule_{year}.parquet", index=None)
    pyreadr.write_rds(f"cfb/schedules/rds/cfb_schedule_{year}.rds", schedule)
    schedule.to_csv(f"cfb/schedules/csv/cfb_schedule_{year}.csv", index=False)


def calculateGEI(plays, homeTeamId):
    plays = pd.DataFrame(plays)
    length = len(plays)
    avg_length = 179.01777401608126
    # Adjusting for game length
    normalize = avg_length / length if length > 0 else 0
    if length == 0:
        return 0.0
    # Get win probability differences for each play
    win_prob_change = abs(plays["home_wp_before"].diff())
    return normalize * win_prob_change.sum()


def postprocessing(game_id):
    processed_data = CFBPlayProcess(
        gameId=game_id,
    )
    pbp = processed_data.espn_cfb_pbp()
    logger.debug(f"{game_id}: processing pipeline {pbp.keys()}")
    if pbp.get("header").get("competitions")[0].get("playByPlaySource") == "none":
        logger.info(f"{game_id}: No plays found\n{[(k,type(v)) for k,v in pbp.items()]}")
        return pbp
    processed_data.run_processing_pipeline()
    pbp = processed_data.json

    jsonified_df = pl.DataFrame(pbp.get("plays", []), infer_schema_length=400).to_dicts()

    bad_cols = [
        "start.distance",
        "start.yardLine",
        "start.team.id",
        "start.down",
        "start.yardsToEndzone",
        "start.posTeamTimeouts",
        "start.defTeamTimeouts",
        "start.shortDownDistanceText",
        "start.possessionText",
        "start.downDistanceText",
        "start.pos_team_timeouts",
        "start.def_pos_team_timeouts",
        "clock.displayValue",
        "type.id",
        "type.text",
        "type.abbreviation",
        "end.distance",
        "end.yardLine",
        "end.team.id",
        "end.down",
        "end.yardsToEndzone",
        "end.posTeamTimeouts",
        "end.defTeamTimeouts",
        "end.shortDownDistanceText",
        "end.possessionText",
        "end.downDistanceText",
        "end.pos_team_timeouts",
        "end.def_pos_team_timeouts",
        "expectedPoints.before",
        "expectedPoints.after",
        "expectedPoints.added",
        "winProbability.before",
        "winProbability.after",
        "winProbability.added",
        "scoringType.displayName",
        "scoringType.name",
        "scoringType.abbreviation",
    ]
    # clean records back into ESPN format
    for record in jsonified_df:
        record["clock"] = {
            "displayValue": record["clock.displayValue"],
            "minutes": record["clock.minutes"],
            "seconds": record["clock.seconds"],
        }

        record["type"] = {
            "id": record["type.id"],
            "text": record["type.text"],
            "abbreviation": record["type.abbreviation"],
        }
        record["modelInputs"] = {
            "start": {
                "down": record["start.down"],
                "distance": record["start.distance"],
                "yardsToEndzone": record["start.yardsToEndzone"],
                "TimeSecsRem": record["start.TimeSecsRem"],
                "adj_TimeSecsRem": record["start.adj_TimeSecsRem"],
                "pos_score_diff": record["pos_score_diff_start"],
                "posTeamTimeouts": record["start.posTeamTimeouts"],
                "defTeamTimeouts": record["start.defPosTeamTimeouts"],
                "ExpScoreDiff": record["start.ExpScoreDiff"],
                "ExpScoreDiff_Time_Ratio": record["start.ExpScoreDiff_Time_Ratio"],
                "spread_time": record["start.spread_time"],
                "pos_team_receives_2H_kickoff": record["start.pos_team_receives_2H_kickoff"],
                "is_home": record["start.is_home"],
                "period": record["period"],
            },
            "end": {
                "down": record["end.down"],
                "distance": record["end.distance"],
                "yardsToEndzone": record["end.yardsToEndzone"],
                "TimeSecsRem": record["end.TimeSecsRem"],
                "adj_TimeSecsRem": record["end.adj_TimeSecsRem"],
                "posTeamTimeouts": record["end.posTeamTimeouts"],
                "defTeamTimeouts": record["end.defPosTeamTimeouts"],
                "pos_score_diff": record["pos_score_diff_end"],
                "ExpScoreDiff": record["end.ExpScoreDiff"],
                "ExpScoreDiff_Time_Ratio": record["end.ExpScoreDiff_Time_Ratio"],
                "spread_time": record["end.spread_time"],
                "pos_team_receives_2H_kickoff": record["end.pos_team_receives_2H_kickoff"],
                "is_home": record["end.is_home"],
                "period": record["period"],
            },
        }

        record["expectedPoints"] = {
            "before": record["EP_start"],
            "after": record["EP_end"],
            "added": record["EPA"],
        }

        record["winProbability"] = {
            "before": record["wp_before"],
            "after": record["wp_after"],
            "added": record["wpa"],
        }

        record["start"] = {
            "team": {
                "id": record["start.team.id"],
            },
            "pos_team": {"id": record["start.pos_team.id"], "name": record["start.pos_team.name"]},
            "def_pos_team": {
                "id": record["start.def_pos_team.id"],
                "name": record["start.def_pos_team.name"],
            },
            "distance": record["start.distance"],
            "yardLine": record["start.yardLine"],
            "down": record["start.down"],
            "yardsToEndzone": record["start.yardsToEndzone"],
            "homeScore": record["start.homeScore"],
            "awayScore": record["start.awayScore"],
            "pos_team_score": record["start.pos_team_score"],
            "def_pos_team_score": record["start.def_pos_team_score"],
            "pos_score_diff": record["pos_score_diff_start"],
            "posTeamTimeouts": record["start.posTeamTimeouts"],
            "defTeamTimeouts": record["start.defPosTeamTimeouts"],
            "ExpScoreDiff": record["start.ExpScoreDiff"],
            "ExpScoreDiff_Time_Ratio": record["start.ExpScoreDiff_Time_Ratio"],
            "shortDownDistanceText": record["start.shortDownDistanceText"],
            "possessionText": record["start.possessionText"],
            "downDistanceText": record["start.downDistanceText"],
            "posTeamSpread": record["start.pos_team_spread"],
        }

        record["end"] = {
            "team": {
                "id": record["end.team.id"],
            },
            "pos_team": {
                "id": record["end.pos_team.id"],
                "name": record["end.pos_team.name"],
            },
            "def_pos_team": {
                "id": record["end.def_pos_team.id"],
                "name": record["end.def_pos_team.name"],
            },
            "distance": record["end.distance"],
            "yardLine": record["end.yardLine"],
            "down": record["end.down"],
            "yardsToEndzone": record["end.yardsToEndzone"],
            "homeScore": record["end.homeScore"],
            "awayScore": record["end.awayScore"],
            "pos_team_score": record["end.pos_team_score"],
            "def_pos_team_score": record["end.def_pos_team_score"],
            "pos_score_diff": record["pos_score_diff_end"],
            "posTeamTimeouts": record["end.posTeamTimeouts"],
            "defPosTeamTimeouts": record["end.defPosTeamTimeouts"],
            "ExpScoreDiff": record["end.ExpScoreDiff"],
            "ExpScoreDiff_Time_Ratio": record["end.ExpScoreDiff_Time_Ratio"],
            "shortDownDistanceText": record["end.shortDownDistanceText"],
            "possessionText": record["end.possessionText"],
            "downDistanceText": record["end.downDistanceText"],
        }

        record["players"] = {
            "passer_player_name": record["passer_player_name"],
            "rusher_player_name": record["rusher_player_name"],
            "receiver_player_name": record["receiver_player_name"],
            "sack_player_name": record["sack_player_name"],
            "sack_player_name2": record["sack_player_name2"],
            "pass_breakup_player_name": record["pass_breakup_player_name"],
            "interception_player_name": record["interception_player_name"],
            "fg_kicker_player_name": record["fg_kicker_player_name"],
            "fg_block_player_name": record["fg_block_player_name"],
            "fg_return_player_name": record["fg_return_player_name"],
            "kickoff_player_name": record["kickoff_player_name"],
            "kickoff_return_player_name": record["kickoff_return_player_name"],
            "punter_player_name": record["punter_player_name"],
            "punt_block_player_name": record["punt_block_player_name"],
            "punt_return_player_name": record["punt_return_player_name"],
            "punt_block_return_player_name": record["punt_block_return_player_name"],
            "fumble_player_name": record["fumble_player_name"],
            "fumble_forced_player_name": record["fumble_forced_player_name"],
            "fumble_recovered_player_name": record["fumble_recovered_player_name"],
        }
        # remove added columns
        for col in bad_cols:
            record.pop(col, None)

    pbp["scoringPlays"] = [play for play in jsonified_df if play["scoringPlay"] is True]
    pbp["mostImportantPlays"] = [
        play
        for play in jsonified_df
        if sorted(jsonified_df, key=lambda x: abs(x["wpa"] if x["wpa"] is not None else 0.0), reverse=True).index(play)
        < 10
    ]
    pbp["bigPlays"] = [
        play
        for play in jsonified_df
        if sorted(jsonified_df, key=lambda x: abs(x["EPA"] if x["EPA"] is not None else 0.0), reverse=True).index(play)
        < 10
    ]

    if pbp["header"]["competitions"][0]["competitors"][0]["homeAway"] == "home":
        homeTeamId = pbp["header"]["competitions"][0]["competitors"][0]["team"]["id"]
        awayTeamId = pbp["header"]["competitions"][0]["competitors"][1]["team"]["id"]
    else:
        homeTeamId = pbp["header"]["competitions"][0]["competitors"][1]["team"]["id"]
        awayTeamId = pbp["header"]["competitions"][0]["competitors"][0]["team"]["id"]

    year = pbp["header"]["season"]["year"]
    gei = calculateGEI(jsonified_df, homeTeamId)
    percentiles = (
        pd.read_csv(f"cfb/team_summaries/{year}/percentiles.csv").to_dict(orient="records") if year > 2013 else []
    )
    return {
        "id": game_id,
        "count": len(jsonified_df),
        "gei": gei,
        "mostImportantPlays": pbp["mostImportantPlays"],
        "bigPlays": pbp["bigPlays"],
        "plays": jsonified_df,
        "percentiles": percentiles,
        "advBoxScore": pbp.get("advBoxScore"),
        "homeTeamId": homeTeamId,
        "awayTeamId": awayTeamId,
        # "drives" : pbp['drives'],
        "scoringPlays": pbp["scoringPlays"],
        "winprobability": np.array(pbp["winprobability"]).tolist(),
        "boxScore": pbp["boxscore"],
        "homeTeamSpread": np.array(pbp["homeTeamSpread"]).tolist(),
        "overUnder": np.array(pbp["overUnder"]).tolist(),
        "header": pbp["header"],
        "broadcasts": np.array(pbp["broadcasts"]).tolist(),
        "pickcenter": np.array(pbp["pickcenter"]).tolist(),
        "gameInfo": np.array(pbp["gameInfo"]).tolist(),
        "season": pbp["season"],
    }


def main():
    start_year = max(args.start_year, 2004)
    end_year = start_year if args.end_year is None else args.end_year
    process = args.process
    years_arr = range(start_year, end_year + 1)
    rescrape = args.rescrape
    for year in years_arr:
        schedule = pd.read_csv(f"{path_to_schedules}/csv/cfb_schedule_{year}.csv")
        schedule = schedule.sort_values(by=["season", "season_type"], ascending=True)
        schedule_df = schedule[schedule["status_type_completed"] == True]

        print(f"Rescrape? {rescrape}")
        if rescrape == False:
            game_files = [int(game_file.replace(".json", "")) for game_file in os.listdir(path_to_final)]
            schedule_df = schedule_df[~schedule_df["game_id"].isin(game_files)]

        schedule_df = schedule_df[schedule_df["season"] >= 2004]
        logger.info(f"Scraping CFB PBP for {year}...")
        games = schedule_df[(schedule_df["season"] == year)].reset_index()["game_id"]

        if len(games) == 0:
            logger.info(f"{len(games)} Games to be scraped, skipping")
        elif len(games) > 0:
            logger.info(f"Number of Games: {len(games)}")
            t0 = time.time()
            download_game_pbps(games, process, path_to_final)
            t1 = time.time()
            logger.info(f"{(t1-t0)/60} minutes to download {len(games)} game play-by-plays.")

        logger.info(f"Finished CFB PBP for {year}...")

        schedule = add_game_to_schedule(schedule, year)

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
    parser.add_argument(
        "--process", "-p", type=bool, default=True, help="Run processing pipeline for games in the schedule period"
    )
    args = parser.parse_args()

    main()
