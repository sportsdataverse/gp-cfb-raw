
import argparse
import concurrent.futures
import gc
import json
import http
import logging
import numpy as np
import os
import pyreadr
import pyarrow as pa
import pandas as pd
import re
import sportsdataverse as sdv
import time
import traceback
import urllib.request
from urllib.error import URLError, HTTPError, ContentTooShortError
from datetime import datetime
from itertools import chain, starmap, repeat
from pathlib import Path
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, filename = 'gp_cfb_raw_logfile.txt')
logger = logging.getLogger(__name__)

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

    with concurrent.futures.ThreadPoolExecutor(max_workers = threads) as executor:
        result = list(executor.map(download_game, games, repeat(process), repeat(path_to_final)))
        return result

def download_game(game, process, path_to_final):

    # this finds our json files
    path_to_final_json = f"{path_to_final}/"
    Path(path_to_final_json).mkdir(parents = True, exist_ok = True)

    try:
        result = postprocessing(game_id = game)

        fp = "{}{}.json".format(path_to_final_json, game)
        with open(fp,'w') as f:
            json.dump(result, f, indent = 0, sort_keys = False)
    except (FileNotFoundError) as e:
        logger.exception(f"FileNotFoundError: game_id = {game}\n {traceback.format_exc()}")
        pass
    except (TypeError) as e:
        logger.exception(f"TypeError: game_id = {game}\n {traceback.format_exc()}")
        pass
    except (IndexError) as e:
        logger.exception(f"IndexError:  game_id = {game}\n {traceback.format_exc()}")
        pass
    except (KeyError) as e:
        logger.exception(f"KeyError: game_id =  game_id = {game}\n {traceback.format_exc()}")
        pass
    except (ValueError) as e:
        logger.exception(f"DecodeError: game_id = {game}\n {traceback.format_exc()}")
        pass
    except (AttributeError) as e:
        logger.exception(f"AttributeError: game_id = {game}\n {traceback.format_exc()}")
        pass

    time.sleep(0.5)

def add_game_to_schedule(schedule, year):
    game_files = [int(game_file.replace(".json", "")) for game_file in os.listdir(path_to_final)]
    schedule["game_json"] = schedule["game_id"].astype(int).isin(game_files)
    schedule["game_json_url"] = np.where(
        schedule["game_json"] == True,
        schedule["game_id"].apply(lambda x: f"https://raw.githubusercontent.com/sportsdataverse/gp-cfb-raw/main/cfb/json/final/{x}.json"),
        None
    )
    schedule.to_parquet(f"cfb/schedules/parquet/cfb_schedule_{year}.parquet", index = None)
    pyreadr.write_rds(f"cfb/schedules/rds/cfb_schedule_{year}.rds", schedule, compress = "gzip")
    return

def postprocessing(game_id):
    processed_data = sdv.cfb.CFBPlayProcess(gameId = game_id)
    pbp = processed_data.espn_cfb_pbp()
    processed_data.run_processing_pipeline()
    tmp_json = processed_data.plays_json.to_json(orient="records")
    jsonified_df = json.loads(tmp_json)

    box = {}
    if pbp.get('header').get('competitions')[0].get('playByPlaySource', "none") != "none":
        box = processed_data.create_box_score()

    bad_cols = [
        'start.distance', 'start.yardLine', 'start.team.id', 'start.down', 'start.yardsToEndzone', 'start.posTeamTimeouts', 'start.defTeamTimeouts',
        'start.shortDownDistanceText', 'start.possessionText', 'start.downDistanceText', 'start.pos_team_timeouts', 'start.def_pos_team_timeouts',
        'clock.displayValue',
        'type.id', 'type.text', 'type.abbreviation',
        'end.distance', 'end.yardLine', 'end.team.id','end.down', 'end.yardsToEndzone', 'end.posTeamTimeouts','end.defTeamTimeouts',
        'end.shortDownDistanceText', 'end.possessionText', 'end.downDistanceText', 'end.pos_team_timeouts', 'end.def_pos_team_timeouts',
        'expectedPoints.before', 'expectedPoints.after', 'expectedPoints.added',
        'winProbability.before', 'winProbability.after', 'winProbability.added',
        'scoringType.displayName', 'scoringType.name', 'scoringType.abbreviation'
    ]
    # clean records back into ESPN format
    for record in jsonified_df:
        record["clock"] = {
            "displayValue" : record["clock.displayValue"],
            "minutes" : record["clock.minutes"],
            "seconds" : record["clock.seconds"]
        }

        record["type"] = {
            "id" : record["type.id"],
            "text" : record["type.text"],
            "abbreviation" : record["type.abbreviation"],
        }
        record["modelInputs"] = {
            "start" : {
                "down" : record["start.down"],
                "distance" : record["start.distance"],
                "yardsToEndzone" : record["start.yardsToEndzone"],
                "TimeSecsRem": record["start.TimeSecsRem"],
                "adj_TimeSecsRem" : record["start.adj_TimeSecsRem"],
                "pos_score_diff" : record["pos_score_diff_start"],
                "posTeamTimeouts" : record["start.posTeamTimeouts"],
                "defTeamTimeouts" : record["start.defPosTeamTimeouts"],
                "ExpScoreDiff" : record["start.ExpScoreDiff"],
                "ExpScoreDiff_Time_Ratio" : record["start.ExpScoreDiff_Time_Ratio"],
                "spread_time" : record['start.spread_time'],
                "pos_team_receives_2H_kickoff": record["start.pos_team_receives_2H_kickoff"],
                "is_home": record["start.is_home"],
                "period": record["period"]
            },
            "end" : {
                "down" : record["end.down"],
                "distance" : record["end.distance"],
                "yardsToEndzone" : record["end.yardsToEndzone"],
                "TimeSecsRem": record["end.TimeSecsRem"],
                "adj_TimeSecsRem" : record["end.adj_TimeSecsRem"],
                "posTeamTimeouts" : record["end.posTeamTimeouts"],
                "defTeamTimeouts" : record["end.defPosTeamTimeouts"],
                "pos_score_diff" : record["pos_score_diff_end"],
                "ExpScoreDiff" : record["end.ExpScoreDiff"],
                "ExpScoreDiff_Time_Ratio" : record["end.ExpScoreDiff_Time_Ratio"],
                "spread_time" : record['end.spread_time'],
                "pos_team_receives_2H_kickoff": record["end.pos_team_receives_2H_kickoff"],
                "is_home": record["end.is_home"],
                "period": record["period"]
            }
        }

        record["expectedPoints"] = {
            "before" : record["EP_start"],
            "after" : record["EP_end"],
            "added" : record["EPA"]
        }

        record["winProbability"] = {
            "before" : record["wp_before"],
            "after" : record["wp_after"],
            "added" : record["wpa"]
        }

        record["start"] = {
            "team" : {
                "id" : record["start.team.id"],
            },
            "pos_team": {
                "id" : record["start.pos_team.id"],
                "name" : record["start.pos_team.name"]
            },
            "def_pos_team": {
                "id" : record["start.def_pos_team.id"],
                "name" : record["start.def_pos_team.name"],
            },
            "distance" : record["start.distance"],
            "yardLine" : record["start.yardLine"],
            "down" : record["start.down"],
            "yardsToEndzone" : record["start.yardsToEndzone"],
            "homeScore" : record["start.homeScore"],
            "awayScore" : record["start.awayScore"],
            "pos_team_score" : record["start.pos_team_score"],
            "def_pos_team_score" : record["start.def_pos_team_score"],
            "pos_score_diff" : record["pos_score_diff_start"],
            "posTeamTimeouts" : record["start.posTeamTimeouts"],
            "defTeamTimeouts" : record["start.defPosTeamTimeouts"],
            "ExpScoreDiff" : record["start.ExpScoreDiff"],
            "ExpScoreDiff_Time_Ratio" : record["start.ExpScoreDiff_Time_Ratio"],
            "shortDownDistanceText" : record["start.shortDownDistanceText"],
            "possessionText" : record["start.possessionText"],
            "downDistanceText" : record["start.downDistanceText"],
            "posTeamSpread" : record["start.pos_team_spread"]
        }

        record["end"] = {
            "team" : {
                "id" : record["end.team.id"],
            },
            "pos_team": {
                "id" : record["end.pos_team.id"],
                "name" : record["end.pos_team.name"],
            },
            "def_pos_team": {
                "id" : record["end.def_pos_team.id"],
                "name" : record["end.def_pos_team.name"],
            },
            "distance" : record["end.distance"],
            "yardLine" : record["end.yardLine"],
            "down" : record["end.down"],
            "yardsToEndzone" : record["end.yardsToEndzone"],
            "homeScore" : record["end.homeScore"],
            "awayScore" : record["end.awayScore"],
            "pos_team_score" : record["end.pos_team_score"],
            "def_pos_team_score" : record["end.def_pos_team_score"],
            "pos_score_diff" : record["pos_score_diff_end"],
            "posTeamTimeouts" : record["end.posTeamTimeouts"],
            "defPosTeamTimeouts" : record["end.defPosTeamTimeouts"],
            "ExpScoreDiff" : record["end.ExpScoreDiff"],
            "ExpScoreDiff_Time_Ratio" : record["end.ExpScoreDiff_Time_Ratio"],
            "shortDownDistanceText" : record["end.shortDownDistanceText"],
            "possessionText" : record["end.possessionText"],
            "downDistanceText" : record["end.downDistanceText"]
        }

        record["players"] = {
            'passer_player_name' : record["passer_player_name"],
            'rusher_player_name' : record["rusher_player_name"],
            'receiver_player_name' : record["receiver_player_name"],
            'sack_player_name' : record["sack_player_name"],
            'sack_player_name2' : record["sack_player_name2"],
            'pass_breakup_player_name' : record["pass_breakup_player_name"],
            'interception_player_name' : record["interception_player_name"],
            'fg_kicker_player_name' : record["fg_kicker_player_name"],
            'fg_block_player_name' : record["fg_block_player_name"],
            'fg_return_player_name' : record["fg_return_player_name"],
            'kickoff_player_name' : record["kickoff_player_name"],
            'kickoff_return_player_name' : record["kickoff_return_player_name"],
            'punter_player_name' : record["punter_player_name"],
            'punt_block_player_name' : record["punt_block_player_name"],
            'punt_return_player_name' : record["punt_return_player_name"],
            'punt_block_return_player_name' : record["punt_block_return_player_name"],
            'fumble_player_name' : record["fumble_player_name"],
            'fumble_forced_player_name' : record["fumble_forced_player_name"],
            'fumble_recovered_player_name' : record["fumble_recovered_player_name"],
        }
        # remove added columns
        for col in bad_cols:
            record.pop(col, None)

    result = {
        "id": game_id,
        "count" : len(jsonified_df),
        "plays" : jsonified_df,
        "advBoxScore" : box,
        "homeTeamId": pbp['header']['competitions'][0]['competitors'][0]['team']['id'],
        "awayTeamId": pbp['header']['competitions'][0]['competitors'][1]['team']['id'],
        "drives" : pbp['drives'],
        "scoringPlays" : np.array(pbp['scoringPlays']).tolist(),
        "winprobability" : np.array(pbp['winprobability']).tolist(),
        "boxScore" : pbp['boxscore'],
        "homeTeamSpread" : np.array(pbp['homeTeamSpread']).tolist(),
        "overUnder" : np.array(pbp['overUnder']).tolist(),
        "header" : pbp['header'],
        "broadcasts" : np.array(pbp['broadcasts']).tolist(),
        "videos" : np.array(pbp['videos']).tolist(),
        "standings" : pbp['standings'],
        "pickcenter" : np.array(pbp['pickcenter']).tolist(),
        "espnWinProbability" : np.array(pbp['espnWP']).tolist(),
        "gameInfo" : np.array(pbp['gameInfo']).tolist(),
        "season" : np.array(pbp['season']).tolist()
    }
    return result

def main():

    if args.start_year < 2004:
        start_year = 2004
    else:
        start_year = args.start_year
    if args.end_year is None:
        end_year = start_year
    else:
        end_year = args.end_year
    process = args.process
    years_arr = range(start_year, end_year + 1)

    for year in years_arr:
        schedule = pd.read_parquet(f"{path_to_schedules}/parquet/cfb_schedules_{year}.parquet", engine = 'auto', columns = None)
        schedule = schedule.sort_values(by = ["season", "season_type"], ascending = True)
        schedule["game_id"] = schedule["game_id"].astype(int)
        schedule = schedule[schedule["status_type_completed"] == True]
        if args.rescrape == False:
            game_files = [int(game_file.replace(".json", "")) for game_file in os.listdir(path_to_final)]
            schedule = schedule[~schedule["game_id"].isin(game_files)]
        schedule = schedule[schedule['season']>=2004]
        logger.info(f"Scraping CFB PBP for {year}...")
        games = schedule[(schedule['season']==year)].reset_index()['game_id']

        if len(games) == 0:
            logger.info(f"{len(games)} Games to be scraped, skipping")
        elif len(games) > 0:
            logger.info(f"Number of Games: {len(games)}")
            bad_schedule_keys = pd.DataFrame()
            t0 = time.time()
            download_game_pbps(games, process, path_to_final)
            t1 = time.time()
            logger.info(f"{(t1-t0)/60} minutes to download {len(games)} game play-by-plays.")

        logger.info(f"Finished CFB PBP for {year}...")

        schedule = add_game_to_schedule(schedule, year)

    gc.collect()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_year", "-s", type = int, required = True, help = "Start year of CFB Schedule period (YYYY)")
    parser.add_argument("--end_year", "-e", type = int, help = "End year of CFB Schedule period (YYYY)")
    parser.add_argument("--rescrape", "-r", type = bool, default = True, help = "Rescrape all games in the schedule period")
    parser.add_argument("--process", "-p", type = bool, default = True, help = "Run processing pipeline for games in the schedule period")
    args = parser.parse_args()

    main()
