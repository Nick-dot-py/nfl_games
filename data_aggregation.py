
import os
import logging
import datetime
from warnings import warn
from typing import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy
import pandas
import appdirs
from urllib.error import HTTPError
import pickle

    
# Functions

def get_players(all_player_states=tuple, dist_players=list):
    player_list = []
    with open("pos_player.pkl", "rb") as f:
        pos_player = pickle.load(f)
    qbs = pos_player['QB']
    wrs = pos_player['WR']
    rbs = pos_player['RB']
    tes = pos_player['TE']
    if all_player_states[0]:
        player_list.extend(qbs)
    if all_player_states[1]:
        player_list.extend(wrs)
    if all_player_states[2]:
        player_list.extend(rbs)
    if all_player_states[3]:
        player_list.extend(tes)
    if len(dist_players) > 0:
        player_list.extend(dist_players)
    unique_player_list = list(set(player_list))
    unique_player_list.sort()
    return unique_player_list

def def_data_requested(standard=bool,select=bool,data_select=list):
    if standard:
        data_groups = ['Passing', 'Rushing', 'Receiving', 'Misc.']
    elif select:
        data_groups = data_select
    return data_groups

def get_weeks(dist=bool, start_end=bool,start_wk=str, end_wk=str, selections=list):
    # 2016 Season, Week 3
    szn_wk = {}
    if dist:
        for i in selections:
            szn = int(i[:4])
            wk = int(i[18:])
            if szn in szn_wk.keys():
                szn_wk[szn].append(wk)
            else:
                szn_wk[szn] = [wk]
    elif start_end:
        start_szn_wk = (start_wk[:4], start_wk[18:])
        end_szn_wk = (end_wk[:4], end_wk[18:])
        if start_szn_wk[0] == end_szn_wk[0]:
            szn = int(start_szn_wk[0])
            wk = list(range(int(start_szn_wk[1]), int(end_szn_wk[1] + 1)))
            szn_wk[szn] = [wk]
        else:
            szn = list(range(int(start_szn_wk[0]),int(end_szn_wk[0]) + 1))
            szn_wk = dict.fromkeys(szn)
            wk_s = list(range(int(start_szn_wk[1]),21))
            if int(end_szn_wk[1]) == 1:
                wk_e = [1]
            else:
                wk_e = list(range(1,int(end_szn_wk[1]) + 1))
            szn_wk[int(start_szn_wk[0])] = wk_s
            szn_wk[int(end_szn_wk[0])] = wk_e
            for key in szn_wk:
                if int(start_szn_wk[0]) < key < int(end_szn_wk[0]):
                    szn_wk[key] = list(range(1,21))
    return szn_wk

def get_seasons(dist=bool, start_end=bool, start_szn=str, end_szn=str, selections=list):
    if dist:
        szns = [int(s[:4]) for s in selections]
    elif start_end:
        start = start_szn[:4]
        end = end_szn[:4]
        szns = list(range(int(start), int(end) + 1))
    return szns

def import_weekly_data(
        years, 
        columns=None, 
        downcast=True,
        thread_requests=False
    ):
    """Imports weekly player data
    
    Args:
        years (List[int]): years to get weekly data for
        columns (List[str]): only return these columns
        downcast (bool): convert float64 to float32, default True
    Returns:
        DataFrame
    """
    
    # check variable types
    if not isinstance(years, (list, range)):
        raise ValueError('Input must be list or range.')
        
    if min(years) < 1999:
        raise ValueError('Data not available before 1999.')
    
    if not columns:
        columns = []

    url = r'https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_{0}.parquet'

    if thread_requests:
        with ThreadPoolExecutor() as executor:
            # Create a list of the same size as years, initialized with None
            data = [None]*len(years)
            # Create a mapping of futures to their corresponding index in the data
            futures_map = {
                executor.submit(
                    pandas.read_parquet,
                    path=url.format(year),
                    columns=columns if columns else None,
                    engine='auto'
                ): idx
                for idx, year in enumerate(years)
            }
            for future in as_completed(futures_map):
                data[futures_map[future]] = future.result()
            data = pandas.concat(data)
    else:
        # read weekly data
        data = pandas.concat([pandas.read_parquet(url.format(x), engine='auto') for x in years])        

    if columns:
        data = data[columns]

    # converts float64 to float32, saves ~30% memory
    if downcast:
        print('Downcasting floats.')
        cols = data.select_dtypes(include=[numpy.float64]).columns
        data[cols] = data[cols].astype(numpy.float32)

    return data


def import_seasonal_data(years, s_type='REG'):
    """Imports seasonal player data
    
    Args:
        years (List[int]): years to get seasonal data for
        s_type (str): season type to include in average ('ALL','REG','POST')
    Returns:
        DataFrame
    """
    
    # check variable types
    if not isinstance(years, (list, range)):
        raise ValueError('years input must be list or range.')
        
    if min(years) < 1999:
        raise ValueError('Data not available before 1999.')
        
    if s_type not in ('REG','ALL','POST'):
        raise ValueError('Only REG, ALL, POST allowed for s_type.')
    
    # import weekly data
    url = r'https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_{0}.parquet'
    data = pandas.concat([pandas.read_parquet(url.format(x), engine='auto') for x in years])
    
    # filter to appropriate season_type
    if s_type != 'ALL':
        data = data[(data['season_type'] == s_type)]

    # calc per game stats
    pgstats = data[['recent_team', 'season', 'week', 'attempts', 'completions', 'passing_yards', 'passing_tds',
                      'passing_air_yards', 'passing_yards_after_catch', 'passing_first_downs',
                      'fantasy_points_ppr']].groupby(
        ['recent_team', 'season', 'week']).sum().reset_index()
    pgstats.columns = ['recent_team', 'season', 'week', 'atts', 'comps', 'p_yds', 'p_tds', 'p_ayds', 'p_yac', 'p_fds',
                       'ppr_pts']
    all_stats = data[
        ['player_id', 'player_name', 'recent_team', 'season', 'week', 'carries', 'rushing_yards', 'rushing_tds',
         'rushing_first_downs', 'rushing_2pt_conversions', 'receptions', 'targets', 'receiving_yards', 'receiving_tds',
         'receiving_air_yards', 'receiving_yards_after_catch', 'receiving_first_downs', 'receiving_epa',
         'fantasy_points_ppr']].merge(pgstats, how='left', on=['recent_team', 'season', 'week']).fillna(0)
    season_stats = all_stats.drop(['recent_team', 'week'], axis=1).groupby(
        ['player_id', 'season']).sum(numeric_only=True).reset_index()

    # calc custom receiving stats
    season_stats['tgt_sh'] = season_stats['targets'] / season_stats['atts']
    season_stats['ay_sh'] = season_stats['receiving_air_yards'] / season_stats['p_ayds']
    season_stats['yac_sh'] = season_stats['receiving_yards_after_catch'] / season_stats['p_yac']
    season_stats['wopr'] = season_stats['tgt_sh'] * 1.5 + season_stats['ay_sh'] * 0.8
    season_stats['ry_sh'] = season_stats['receiving_yards'] / season_stats['p_yds']
    season_stats['rtd_sh'] = season_stats['receiving_tds'] / season_stats['p_tds']
    season_stats['rfd_sh'] = season_stats['receiving_first_downs'] / season_stats['p_fds']
    season_stats['rtdfd_sh'] = (season_stats['receiving_tds'] + season_stats['receiving_first_downs']) / (
                season_stats['p_tds'] + season_stats['p_fds'])
    season_stats['dom'] = (season_stats['ry_sh'] + season_stats['rtd_sh']) / 2
    season_stats['w8dom'] = season_stats['ry_sh'] * 0.8 + season_stats['rtd_sh'] * 0.2
    season_stats['yptmpa'] = season_stats['receiving_yards'] / season_stats['atts']
    season_stats['ppr_sh'] = season_stats['fantasy_points_ppr'] / season_stats['ppr_pts']

    data.drop(['recent_team', 'week'], axis=1, inplace=True)
    szn = data.groupby(['player_id', 'season', 'season_type']).sum(numeric_only=True).reset_index().merge(
        data[['player_id', 'season', 'season_type']].groupby(['player_id', 'season']).count().reset_index().rename(
            columns={'season_type': 'games'}), how='left', on=['player_id', 'season'])

    szn = szn.merge(season_stats[['player_id', 'season', 'tgt_sh', 'ay_sh', 'yac_sh', 'wopr', 'ry_sh', 'rtd_sh',
                                  'rfd_sh', 'rtdfd_sh', 'dom', 'w8dom', 'yptmpa', 'ppr_sh']], how='left',
                    on=['player_id', 'season'])

    return szn

def __validate_pfr_inputs(s_type, years=None):
    if s_type not in ('pass', 'rec', 'rush', 'def'):
        raise ValueError('s_type variable must be one of "pass", "rec","rush", or "def".')
    
    if years is None:
        return []
    
    if not isinstance(years, Iterable):
        raise ValueError("years must be an Iterable.")
    
    years = list(years)

    if not all(isinstance(x, int) for x in years):
        raise ValueError('years variable must only contain integers.')

    if years and min(years) < 2018:
        raise ValueError('Data not available before 2018.')

    return years

def import_seasonal_pfr(s_type, years=None):
    """Import PFR advanced season-level statistics
    
    Args:
        s_type (str): must be one of pass, rec, rush, def
        years (List[int]): years to return data for, optional
    Returns:
        DataFrame
    """
    
    years = __validate_pfr_inputs(s_type, years)

    url = f"https://github.com/nflverse/nflverse-data/releases/download/pfr_advstats/advstats_season_{s_type}.parquet"
    df = pandas.read_parquet(url)

    return df[df.season.isin(years)] if years else df

def import_weekly_pfr(s_type, years=None):
    """Import PFR advanced week-level statistics
    
    Args:
        s_type (str): must be one of pass, rec, rush, def
        years (List[int]): years to return data for, optional
    Returns:
        DataFrame
    """

    years = __validate_pfr_inputs(s_type, years)
    
    if len(years) == 0:
        years = list(import_seasonal_pfr(s_type).season.unique())
    
    url = "https://github.com/nflverse/nflverse-data/releases/download/pfr_advstats/advstats_week_{0}_{1}.parquet"
    df = pandas.concat([
        pandas.read_parquet(url.format(s_type, yr))
        for yr in years
    ])
    
    return df[df.season.isin(years)] if years else df

# Full function call
def generate_df(players, data_def, granularity, timeframe):
    # players = list of player names
    # data_def = list of data reqeusted
    # granularity = str in ["Week", "Season", "Cumulative"]
    seasonal = False
    weekly = False
    if type(timeframe) == list:
        seasonal = True
    elif type(timeframe) == dict:
        weekly = True
    player_names = players
    data_requested = data_def
    data_format = granularity
    player = str(player_names[0])
    data = str(data_requested[0])
    return f"Dataframe: players {player} data type {data} granularity {data_format}"
