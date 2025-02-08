# -*- coding: utf-8 -*-
"""
Created on Thu Jan 16 08:52:36 2025

@author: nick_
"""

import os
import logging
import datetime
from warnings import warn
from typing import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import numpy
import pandas
import appdirs
from urllib.error import HTTPError
import pickle
import time


start_time = time.perf_counter()

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

def transform_name(name):
    substring1 = " Jr."

    substring3 = "."
    name_T = name.replace(substring1, "")
    name_T = name_T.replace("'", "")
    name_T = name_T.replace(substring3, "")
    name_T = name_T.lower()
    return name_T

def transform_col(df, col, new_col):
    df[new_col] = df[col].str.replace("Jr.", "")
    df[new_col] = df[new_col].str.replace("'", "")
    df[new_col] = df[new_col].str.replace(".", "")
    df[new_col] = df[new_col].str.lower()
    return df

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

def remove_strings(list1, list2):
    """Removes strings found in list2 from list1."""
    return [item for item in list1 if item not in list2]

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

#Standard data def Passing | Rushing | Receiving | Misc.
# Granularity = Weekly
#Passing 
#seasons = [2024]

def get_weekly_passing_df(player_list, timeframe_dict):
    
    seasons = list(timeframe_dict.keys())
    
    name_col_basic_pfr = ('player_display_name','pfr_player_name')
    
    passing_cols_wk = ['player_display_name', 'position', 'recent_team', 'season', 'week', 'season_type', 'opponent_team', 'fantasy_points', 'fantasy_points_ppr', 'completions', 'attempts', 'passing_yards', 'passing_tds', 'interceptions', 'sacks', 'sack_yards', 'sack_fumbles', 'sack_fumbles_lost', 'passing_air_yards', 'passing_yards_after_catch', 'passing_first_downs', 'passing_epa', 'passing_2pt_conversions', 'pacr', 'dakota']
    adv_passing_cols_wk = ['player_name', 'season', 'week', 'passing_drops', 'passing_drop_pct', 'passing_bad_throws', 'passing_bad_throw_pct', 'times_sacked', 'times_blitzed', 'times_hurried', 'times_hit', 'times_pressured', 'times_pressured_pct']
    
    passing_df_adv = import_weekly_pfr(s_type='pass',years=seasons)
    
    passing_df_adv.rename(columns={name_col_basic_pfr[1]: 'player_name'}, inplace=True)
    
    passing_df_adv = passing_df_adv[adv_passing_cols_wk]
    
    passing_df_adv = passing_df_adv.reset_index(drop=True)
    player_loc = list(set(passing_df_adv['player_name'].tolist()))
    
    passing_df_basic = import_weekly_data(years=seasons,columns=passing_cols_wk)
    
    passing_df_basic = passing_df_basic.loc[passing_df_basic['player_display_name'].isin(player_loc)]
    
    passing_df_basic.rename(columns={name_col_basic_pfr[0]: 'player_name'}, inplace=True)
    passing_df_basic = passing_df_basic.reset_index(drop=True)
    
    passing_df = pandas.merge(passing_df_basic, passing_df_adv, on=['player_name', 'season', 'week'], how='left')
    passing_df = passing_df.loc[passing_df['player_name'].isin(player_list)]
    
    sub_df = pandas.DataFrame(columns=passing_df.columns.tolist())
    if len(passing_df) > 0:
        for key in timeframe_dict:
            sub_df = pandas.concat([sub_df, passing_df.loc[(passing_df['season']==key) & (passing_df['week'].isin(timeframe_dict[key]))]], ignore_index=True)
    
    return sub_df

def get_weekly_rushing_df(player_list, timeframe_dict):
    
    seasons = list(timeframe_dict.keys())
    alt_names = [transform_name(x) for x in player_list]
    
    name_col_basic_pfr = ('player_display_name','pfr_player_name')
    
    rushing_cols_wk = ['player_display_name', 'position', 'recent_team', 'season', 'week', 'season_type', 'opponent_team', 'fantasy_points', 'fantasy_points_ppr', 'carries', 'rushing_yards', 'rushing_tds', 'rushing_fumbles', 'rushing_fumbles_lost', 'rushing_first_downs', 'rushing_epa', 'rushing_2pt_conversions']
    adv_rushing_cols_wk = ['player_name', 'season', 'week', 'rushing_yards_before_contact', 'rushing_yards_before_contact_avg', 'rushing_yards_after_contact', 'rushing_yards_after_contact_avg', 'rushing_broken_tackles']
    
    rushing_df_adv = import_weekly_pfr(s_type='rush',years=seasons)
    
    rushing_df_adv.rename(columns={name_col_basic_pfr[1]: 'player_name'}, inplace=True)
    
    rushing_df_adv = rushing_df_adv[adv_rushing_cols_wk]
    rushing_df_adv = rushing_df_adv.reset_index(drop=True)
    rushing_df_adv = transform_col(rushing_df_adv,'player_name', 'player_name_alt')
    player_loc = list(set(rushing_df_adv['player_name_alt'].tolist()))
    
    rushing_df_basic = import_weekly_data(years=seasons,columns=rushing_cols_wk)
    rushing_df_basic = transform_col(rushing_df_basic,'player_display_name', 'player_name_alt')
    rushing_df_basic = rushing_df_basic.loc[rushing_df_basic['player_name_alt'].isin(player_loc)]

    rushing_df_basic.rename(columns={name_col_basic_pfr[0]: 'player_name'}, inplace=True)
    rushing_df_basic = rushing_df_basic.reset_index(drop=True)
    
    rushing_df = pandas.merge(rushing_df_basic, rushing_df_adv, on=['player_name_alt', 'season', 'week'], how='left')
    rushing_df = rushing_df.loc[rushing_df['player_name_alt'].isin(alt_names)]
    rushing_df.rename(columns={'player_name_y': 'player_name'}, inplace=True)
    rushing_df.drop(columns=['player_name_x'], inplace=True)
    rushing_df = rushing_df[['player_name', 'position', 'recent_team', 'season', 'week',
       'season_type', 'opponent_team', 'fantasy_points', 'fantasy_points_ppr',
       'carries', 'rushing_yards', 'rushing_tds', 'rushing_fumbles',
       'rushing_fumbles_lost', 'rushing_first_downs', 'rushing_epa',
       'rushing_2pt_conversions', 'rushing_yards_before_contact', 'rushing_yards_before_contact_avg',
       'rushing_yards_after_contact', 'rushing_yards_after_contact_avg',
       'rushing_broken_tackles']]

    sub_df = pandas.DataFrame(columns=rushing_df.columns.tolist())
    if len(rushing_df) > 0:
        for key in timeframe_dict:
            sub_df = pandas.concat([sub_df, rushing_df.loc[(rushing_df['season']==key) & (rushing_df['week'].isin(timeframe_dict[key]))]], ignore_index=True)
    
    return sub_df

def get_weekly_receiving_df(player_list, timeframe_dict):
    seasons = list(timeframe_dict.keys())
    alt_names = [transform_name(x) for x in player_list]
    name_col_basic_pfr = ('player_display_name','pfr_player_name')
    
    rec_cols_wk = ['player_display_name', 'position', 'recent_team', 'season', 'week', 'season_type', 'opponent_team', 'fantasy_points', 'fantasy_points_ppr', 'receptions', 'targets', 'receiving_yards', 'receiving_tds', 'receiving_fumbles', 'receiving_fumbles_lost', 'receiving_air_yards', 'receiving_yards_after_catch', 'receiving_first_downs', 'receiving_epa', 'receiving_2pt_conversions', 'racr', 'target_share', 'air_yards_share', 'wopr']
    adv_rec_cols_wk = ['player_name', 'season', 'week', 'receiving_broken_tackles', 'receiving_drop', 'receiving_drop_pct', 'receiving_int', 'receiving_rat']
    
    rec_df_adv = import_weekly_pfr(s_type='rec',years=seasons)
    
    rec_df_adv.rename(columns={name_col_basic_pfr[1]: 'player_name'}, inplace=True)
    
    rec_df_adv = rec_df_adv[adv_rec_cols_wk]
    rec_df_adv = rec_df_adv.reset_index(drop=True)
    rec_df_adv = transform_col(rec_df_adv,'player_name', 'player_name_alt')
    player_loc = list(set(rec_df_adv['player_name_alt'].tolist()))
    
    rec_df_basic = import_weekly_data(years=seasons,columns=rec_cols_wk)
    rec_df_basic = transform_col(rec_df_basic,'player_display_name', 'player_name_alt')
    rec_df_basic = rec_df_basic.loc[rec_df_basic['player_name_alt'].isin(player_loc)]
    rec_df_basic.rename(columns={name_col_basic_pfr[0]: 'player_name'}, inplace=True)
    rec_df_basic = rec_df_basic.reset_index(drop=True)
    
    rec_df = pandas.merge(rec_df_basic, rec_df_adv, on=['player_name_alt', 'season', 'week'], how='left')
    rec_df = rec_df.loc[rec_df['player_name_alt'].isin(alt_names)]
    
    rec_df.rename(columns={'player_name_y': 'player_name'}, inplace=True)
    rec_df.drop(columns=['player_name_x'], inplace=True)

    rec_df = rec_df[['player_name', 'position', 'recent_team', 'season', 'week', 'season_type',
       'opponent_team', 'fantasy_points', 'fantasy_points_ppr', 'receptions',
       'targets', 'receiving_yards', 'receiving_tds', 'receiving_fumbles',
       'receiving_fumbles_lost', 'receiving_air_yards',
       'receiving_yards_after_catch', 'receiving_first_downs', 'receiving_epa',
       'receiving_2pt_conversions', 'racr', 'target_share', 'air_yards_share',
       'wopr', 'receiving_broken_tackles',
       'receiving_drop', 'receiving_drop_pct', 'receiving_int',
       'receiving_rat']]
    sub_df = pandas.DataFrame(columns=rec_df.columns.tolist())
    if len(rec_df) > 0:
        for key in timeframe_dict:
            sub_df = pandas.concat([sub_df, rec_df.loc[(rec_df['season']==key) & (rec_df['week'].isin(timeframe_dict[key]))]], ignore_index=True)
    
    sub_df['adot'] = (sub_df['receiving_air_yards'] / sub_df['targets'].replace(0, np.nan))
    return sub_df


def get_weekly_misc_df(player_list, timeframe_dict):
    pass

def get_seasonal_passing_df(player_list, timeframe_list):
    
    seasons = timeframe_list
    
    player_list_T = [transform_name(x) for x in player_list]
    
    passing_cols_ssn = ['player_id', 'season', 'season_type', 'fantasy_points', 'fantasy_points_ppr', 'games', 'completions', 'attempts', 'passing_yards', 'passing_tds', 'interceptions', 'sacks', 'sack_yards', 'sack_fumbles', 'sack_fumbles_lost', 'passing_air_yards', 'passing_yards_after_catch', 'passing_first_downs', 'passing_epa', 'passing_2pt_conversions', 'pacr', 'dakota']
    adv_passing_cols_ssn = ['player', 'team', 'pass_attempts', 'throwaways', 'spikes', 'drops', 'drop_pct', 'bad_throws', 'bad_throw_pct', 'season', 'pocket_time', 'times_blitzed', 'times_hurried', 'times_hit', 'times_pressured', 'pressure_pct', 'batted_balls', 'on_tgt_throws', 'on_tgt_pct', 'rpo_plays', 'rpo_yards', 'rpo_pass_att', 'rpo_pass_yards', 'rpo_rush_att', 'rpo_rush_yards', 'pa_pass_att', 'pa_pass_yards']
    
    passing_df_adv = import_seasonal_pfr(s_type='pass',years=seasons)
    passing_df_basic = import_seasonal_data(seasons)
    
    passing_df_adv.loc[passing_df_adv['pfr_id']=='MinsGa00', 'player'] = 'Gardner Minshew'
    passing_df_adv.loc[passing_df_adv['pfr_id']=='PeniMi00', 'player'] = 'Michael Penix Jr.'
    
    passing_df_adv = passing_df_adv[adv_passing_cols_ssn]
    passing_df_basic = passing_df_basic[passing_cols_ssn]
    
    with open("player_id.pkl", "rb") as f:
        player_id = pickle.load(f)
    
    with open("player_id_T.pkl", "rb") as f:
        player_id_T = pickle.load(f)
        
    id_list = []
    alt_name = []
    for index, row in passing_df_adv.iterrows():
        alt_name.append(transform_name(row['player']))
        if row['player'] in player_id.keys():
            id_list.append(player_id[row['player']])
        elif transform_name(row['player']) in player_id_T.keys():
            id_list.append(player_id_T[transform_name(row['player'])])
        else:
            id_list.append('no_id')
        
    passing_df_adv['player_id'] = id_list
    passing_df_adv['name_alt'] = alt_name
    
    passing_df_adv = passing_df_adv[passing_df_adv['player_id'] != 'no_id']
    passing_df_adv.rename(columns={'player' : 'player_name'}, inplace=True)
    passing_df_adv = passing_df_adv.reset_index(drop=True)
    
    player_loc = list(set(passing_df_adv['player_id'].tolist()))
    passing_df_basic = passing_df_basic.loc[passing_df_basic['player_id'].isin(player_loc)]
    passing_df_basic = passing_df_basic.reset_index(drop=True)
    
    passing_df = pandas.merge(passing_df_basic, passing_df_adv, on=['player_id', 'season'], how='left')
    passing_df = passing_df.loc[(passing_df['player_name'].isin(player_list)) | (passing_df['name_alt'].isin(player_list_T))]
    passing_df.drop('name_alt', axis=1, inplace=True)
    passing_df = passing_df.reindex(columns=['player_name', 'team', 'games', 'season', 'season_type', 'fantasy_points', 'fantasy_points_ppr', 'player_id', 'completions', 'attempts', 'passing_yards', 'passing_tds', 'interceptions', 'sacks', 'sack_yards', 'sack_fumbles', 'sack_fumbles_lost', 'passing_air_yards', 'passing_yards_after_catch', 'passing_first_downs', 'passing_epa', 'passing_2pt_conversions', 'pacr', 'dakota', 'pass_attempts', 'throwaways', 'spikes', 'drops', 'drop_pct', 'bad_throws', 'bad_throw_pct', 'pocket_time', 'times_blitzed', 'times_hurried', 'times_hit', 'times_pressured', 'pressure_pct', 'batted_balls', 'on_tgt_throws', 'on_tgt_pct', 'rpo_plays', 'rpo_yards', 'rpo_pass_att', 'rpo_pass_yards', 'rpo_rush_att', 'rpo_rush_yards', 'pa_pass_att', 'pa_pass_yards'])
    return passing_df

def get_seasonal_rushing_df(player_list, timeframe_list):
    seasons = timeframe_list
    
    player_list_T = [transform_name(x) for x in player_list]
    
    rushing_cols_ssn = ['player_id', 'season', 'season_type', 'games', 'fantasy_points', 'fantasy_points_ppr', 'carries', 'rushing_yards', 'rushing_tds', 'rushing_fumbles', 'rushing_fumbles_lost', 'rushing_first_downs', 'rushing_epa', 'rushing_2pt_conversions']
    adv_rushing_cols_ssn = ['player', 'season', 'tm', 'age', 'pos', 'att', 'ybc', 'ybc_att', 'yac', 'yac_att', 'brk_tkl', 'att_br']
    
    rushing_df_adv = import_seasonal_pfr(s_type='rush',years=seasons)
    rushing_df_basic = import_seasonal_data(seasons)
    
    rushing_df_adv = rushing_df_adv[adv_rushing_cols_ssn]
    rushing_df_basic = rushing_df_basic[rushing_cols_ssn]
    
    with open("player_id.pkl", "rb") as f:
        player_id = pickle.load(f)
    
    with open("player_id_T.pkl", "rb") as f:
        player_id_T = pickle.load(f)
    
    id_list = []
    alt_name = []
    for index, row in rushing_df_adv.iterrows():
        alt_name.append(transform_name(row['player']))
        if row['player'] in player_id.keys():
            id_list.append(player_id[row['player']])
        elif transform_name(row['player']) in player_id_T.keys():
            id_list.append(player_id_T[transform_name(row['player'])])
        else:
            id_list.append('no_id')
            
    rushing_df_adv['player_id'] = id_list
    rushing_df_adv['name_alt'] = alt_name
    rushing_df_adv = rushing_df_adv[rushing_df_adv['player_id'] != 'no_id']
    rushing_df_adv.rename(columns={'player' : 'player_name', 'tm' : 'team', 'ybc_att' : 'ybc_per_att', 'yac_att' : 'yac_per_att', 'att_br' : 'carry_per_brk_tkl'}, inplace=True)
    rushing_df_adv = rushing_df_adv.reset_index(drop=True)
    
    player_loc = list(set(rushing_df_adv['player_id'].tolist()))
    rushing_df_basic = rushing_df_basic.loc[rushing_df_basic['player_id'].isin(player_loc)]
    rushing_df_basic = rushing_df_basic.reset_index(drop=True)
    
    rushing_df = pandas.merge(rushing_df_basic, rushing_df_adv, on=['player_id', 'season'], how='left')
    rushing_df = rushing_df.loc[(rushing_df['player_name'].isin(player_list)) | (rushing_df['name_alt'].isin(player_list_T))]
    rushing_df.drop('name_alt', axis=1, inplace=True)
    rushing_df = rushing_df.reindex(columns=['player_name', 'team', 'age', 'pos', 'player_id', 'season', 'season_type', 'games', 'fantasy_points', 'fantasy_points_ppr', 'carries', 'rushing_yards', 'rushing_tds', 'rushing_fumbles', 'rushing_fumbles_lost', 'rushing_first_downs', 'rushing_epa', 'rushing_2pt_conversions', 'att', 'ybc', 'ybc_per_att', 'yac', 'yac_per_att', 'brk_tkl', 'carry_per_brk_tkl'])
    return rushing_df

def get_seasonal_rec_df(player_list, timeframe_list):
    seasons = timeframe_list
    
    player_list_T = [transform_name(x) for x in player_list]
    
    rec_cols_ssn = ['player_id', 'season', 'season_type', 'games', 'fantasy_points', 'fantasy_points_ppr', 'receptions', 'targets', 'receiving_yards', 'receiving_tds', 'receiving_fumbles', 'receiving_fumbles_lost', 'receiving_air_yards', 'receiving_yards_after_catch', 'receiving_first_downs', 'receiving_epa', 'receiving_2pt_conversions', 'racr', 'tgt_sh', 'ay_sh', 'wopr_y',]
    adv_rec_cols_ssn = ['player', 'season',  'tm', 'age', 'pos', 'ybc', 'ybc_r', 'yac', 'yac_r', 'adot', 'brk_tkl', 'rec_br', 'drop', 'drop_percent', 'int']
    
    rec_df_adv = import_seasonal_pfr(s_type='rec',years=seasons)
    rec_df_basic = import_seasonal_data(seasons)
    
    rec_df_adv = rec_df_adv[adv_rec_cols_ssn]
    rec_df_basic = rec_df_basic[rec_cols_ssn]
    
    with open("player_id.pkl", "rb") as f:
        player_id = pickle.load(f)
        
    with open("player_id_T.pkl", "rb") as f:
        player_id_T = pickle.load(f)
    
    id_list = []
    alt_name = []
    for index, row in rec_df_adv.iterrows():
        alt_name.append(transform_name(row['player']))
        if row['player'] in player_id.keys():
            id_list.append(player_id[row['player']])
        elif transform_name(row['player']) in player_id_T.keys():
            id_list.append(player_id_T[transform_name(row['player'])])
        else:
            id_list.append('no_id')
            
    rec_df_adv['player_id'] = id_list
    rec_df_adv['name_alt'] = alt_name
    rec_df_adv = rec_df_adv[rec_df_adv['player_id'] != 'no_id']
    rec_df_adv.rename(columns={'player' : 'player_name', 'tm' : 'team', 'pos' : 'position', 'ybc' : 'ybcatch', 'ybc_r' : 'ybcatch_per_rec', 'yac' : 'yacatch', 'yac_r' : 'yacatch_per_rec', 'rec_br' : 'rec_per_brk_tkl'}, inplace=True)
    rec_df_adv = rec_df_adv.reset_index(drop=True)
    
    rec_df_basic.rename(columns={'tgt_sh' : 'target_share', 'ay_sh' : 'air_yards_share', 'wopr_y' : 'wopr'}, inplace=True)
    
    player_loc = list(set(rec_df_adv['player_id'].tolist()))
    rec_df_basic = rec_df_basic.loc[rec_df_basic['player_id'].isin(player_loc)]
    rec_df_basic = rec_df_basic.reset_index(drop=True)
    
    rec_df = pandas.merge(rec_df_basic, rec_df_adv, on=['player_id', 'season'], how='left')
    rec_df = rec_df.loc[(rec_df['player_name'].isin(player_list)) | (rec_df['name_alt'].isin(player_list_T))]
    rec_df.drop('name_alt', axis=1, inplace=True)
    rec_df = rec_df.reindex(columns=['player_name', 'team', 'age', 'position', 'player_id', 'season', 'season_type', 'games', 'fantasy_points', 'fantasy_points_ppr', 'receptions', 'targets', 'receiving_yards', 'receiving_tds', 'receiving_fumbles', 'receiving_fumbles_lost', 'receiving_air_yards', 'receiving_yards_after_catch', 'receiving_first_downs', 'receiving_epa', 'receiving_2pt_conversions', 'racr', 'target_share', 'air_yards_share', 'wopr', 'ybcatch', 'ybcatch_per_rec', 'yacatch', 'yacatch_per_rec', 'adot', 'brk_tkl', 'rec_per_brk_tkl', 'drop', 'drop_percent', 'int'])
    return rec_df

def get_cumulative_weekly_passing_df(player_list, timeframe_dict):
    df_weekly = get_weekly_passing_df(player_list, timeframe_dict)
    df_weekly = df_weekly[['player_name', 'position', 'fantasy_points', 'fantasy_points_ppr', 'completions', 'attempts', 'passing_yards', 'passing_tds', 'interceptions', 'sacks', 'sack_yards', 'sack_fumbles', 'sack_fumbles_lost', 'passing_air_yards', 'passing_yards_after_catch', 'passing_first_downs', 'passing_2pt_conversions', 'passing_drops', 'passing_bad_throws', 'passing_bad_throw_pct', 'times_blitzed', 'times_hurried', 'times_hit', 'times_pressured', 'times_pressured_pct', 'passing_epa', 'pacr', 'dakota', 'passing_drop_pct']]
    player_names = list(set(df_weekly['player_name'].tolist()))

    df_weekly['time_pressured_numer'] = df_weekly['times_pressured'] / df_weekly['times_pressured_pct']
    df_weekly['passing_drop_numer'] = df_weekly['passing_drops'] / df_weekly['passing_drop_pct']
    df_weekly['passing_bad_throws_numer'] = df_weekly['passing_bad_throws'] / df_weekly['passing_bad_throw_pct']
    
    sum_df = pandas.DataFrame(columns=['player_name', 'position', 'games', 'fantasy_points', 'fantasy_points_ppr', 'completions', 'attempts', 'passing_yards', 'passing_tds', 'interceptions', 'sacks', 'sack_yards', 'sack_fumbles', 'sack_fumbles_lost', 'passing_air_yards', 'passing_yards_after_catch', 'passing_first_downs', 'passing_2pt_conversions', 'passing_drops', 'passing_bad_throws', 'passing_bad_throw_pct', 'times_blitzed', 'times_hurried', 'times_hit', 'times_pressured', 'times_pressured_pct', 'passing_epa_avg', 'pacr', 'dakota_avg', 'passing_drop_pct'])
    for i in player_names:
        df_weekly_sub = df_weekly.loc[df_weekly['player_name']==i]
        df_weekly_sub.reset_index(inplace=True)
        player = i
        position = df_weekly_sub['position'][0]
        fpts = df_weekly_sub['fantasy_points'].sum()
        fpts_ppr = df_weekly_sub['fantasy_points_ppr'].sum()
        games = len(df_weekly_sub)
        completions = df_weekly_sub['completions'].sum()
        attempts = df_weekly_sub['attempts'].sum()
        passing_yards = df_weekly_sub['passing_yards'].sum()
        passing_tds = df_weekly_sub['passing_tds'].sum()
        interceptions = df_weekly_sub['interceptions'].sum()
        sacks = df_weekly_sub['sacks'].sum()
        sack_yards = df_weekly_sub['sack_yards'].sum()
        sack_fumbles = df_weekly_sub['sack_fumbles'].sum()
        sack_fumbles_lost = df_weekly_sub['sack_fumbles_lost'].sum()
        passing_air_yards = df_weekly_sub['passing_air_yards'].sum()
        passing_yards_after_catch = df_weekly_sub['passing_yards_after_catch'].sum()
        passing_first_downs = df_weekly_sub['passing_first_downs'].sum()
        passing_2pt_conversions = df_weekly_sub['passing_2pt_conversions'].sum()
        passing_drops = df_weekly_sub['passing_drops'].sum()
        passing_bad_throws = df_weekly_sub['passing_bad_throws'].sum()
        times_blitzed = df_weekly_sub['times_blitzed'].sum()
        times_hurried = df_weekly_sub['times_hurried'].sum()
        times_hit = df_weekly_sub['times_hit'].sum()
        times_pressured = df_weekly_sub['times_pressured'].sum()
        
        time_pressured_numer = df_weekly_sub['time_pressured_numer'].sum()
        passing_drop_numer = df_weekly_sub['passing_drop_numer'].sum()
        passing_bad_throws_numer = df_weekly['passing_bad_throws_numer'].sum()
        
        times_pressured_pct = times_pressured/time_pressured_numer
        passing_drop_pct = passing_drops/passing_drop_numer
        passing_bad_throw_pct = passing_bad_throws/passing_bad_throws_numer
        
        pacr = passing_yards/passing_air_yards
        
        passing_epa_avg = df_weekly_sub['passing_epa'].mean()
        dakota_avg = df_weekly_sub['dakota'].mean()
        
        new_row = pandas.DataFrame({'player_name' : [player], 'position' : [position], 'games' : [games], 'fantasy_points' : [fpts], 'fantasy_points_ppr' : [fpts_ppr], 'completions' : [completions], 'attempts' : [attempts], 'passing_yards' : [passing_yards], 'passing_tds' : [passing_tds], 'interceptions' : [interceptions], 'sacks' : [sacks], 'sack_yards' : [sack_yards], 'sack_fumbles' : [sack_fumbles], 'sack_fumbles_lost' : [sack_fumbles_lost], 'passing_air_yards' : [passing_air_yards], 'passing_yards_after_catch' : [passing_yards_after_catch], 'passing_first_downs' : [passing_first_downs], 'passing_2pt_conversions' : [passing_2pt_conversions], 'passing_drops' : [passing_drops], 'passing_bad_throws' : [passing_bad_throws], 'passing_bad_throw_pct' : [passing_bad_throw_pct], 'times_blitzed' : [times_blitzed], 'times_hurried' : [times_hurried], 'times_hit' : [times_hit], 'times_pressured' : [times_pressured], 'times_pressured_pct' : [times_pressured_pct], 'passing_epa_avg' : [passing_epa_avg], 'pacr' : [pacr], 'dakota_avg' : [dakota_avg], 'passing_drop_pct' : [passing_drop_pct]})
        sum_df = pandas.concat([sum_df, new_row], ignore_index=True)
    return sum_df

def get_cumulative_weekly_rushing_df(player_list, timeframe_dict):
    df_weekly = get_weekly_rushing_df(player_list, timeframe_dict)
    df_weekly = df_weekly[['player_name', 'position', 'fantasy_points', 'fantasy_points_ppr', 'carries', 'rushing_yards', 'rushing_tds', 'rushing_fumbles', 'rushing_fumbles_lost', 'rushing_first_downs', 'rushing_epa', 'rushing_2pt_conversions', 'rushing_yards_before_contact', 'rushing_yards_before_contact_avg', 'rushing_yards_after_contact', 'rushing_yards_after_contact_avg', 'rushing_broken_tackles']]
    player_names = list(set(df_weekly['player_name'].tolist()))
    
    sum_df = pandas.DataFrame(columns=['player_name', 'position', 'games','fantasy_points', 'fantasy_points_ppr', 'carries', 'rushing_yards', 'rushing_tds', 'rushing_fumbles', 'rushing_fumbles_lost', 'rushing_first_downs', 'rushing_epa_avg', 'rushing_2pt_conversions', 'rushing_yards_before_contact', 'rushing_yards_before_contact_avg', 'rushing_yards_after_contact', 'rushing_yards_after_contact_avg', 'rushing_broken_tackles'])
    for i in player_names:
        df_weekly_sub = df_weekly.loc[df_weekly['player_name']==i]
        df_weekly_sub.reset_index(inplace=True)
        player = i
        position = df_weekly_sub['position'][0]
        games = len(df_weekly_sub)
        fantasy_points = df_weekly_sub['fantasy_points'].sum()
        fantasy_points_ppr = df_weekly_sub['fantasy_points_ppr'].sum()
        carries = df_weekly_sub['carries'].sum()
        rushing_yards = df_weekly_sub['rushing_yards'].sum()
        rushing_tds = df_weekly_sub['rushing_tds'].sum()
        rushing_fumbles = df_weekly_sub['rushing_fumbles'].sum()
        rushing_fumbles_lost = df_weekly_sub['rushing_fumbles_lost'].sum()
        rushing_first_downs = df_weekly_sub['rushing_first_downs'].sum()
        rushing_2pt_conversions = df_weekly_sub['rushing_2pt_conversions'].sum()
        rushing_yards_before_contact = df_weekly_sub['rushing_yards_before_contact'].sum()
        rushing_yards_after_contact = df_weekly_sub['rushing_yards_after_contact'].sum()
        rushing_broken_tackles = df_weekly_sub['rushing_broken_tackles'].sum()
        
        rushing_epa_avg = df_weekly_sub['rushing_epa'].mean()
     
        rushing_yards_before_contact_avg = rushing_yards_before_contact/carries
        rushing_yards_after_contact_avg = rushing_yards_after_contact/carries
        new_row = pandas.DataFrame({'player_name' : [player], 'position' : [position], 'games' : [games], 'fantasy_points' : [fantasy_points], 'fantasy_points_ppr' : [fantasy_points_ppr], 'carries' : [carries], 'rushing_yards' : [rushing_yards], 'rushing_tds' : [rushing_tds], 'rushing_fumbles' : [rushing_fumbles], 'rushing_fumbles_lost' : [rushing_fumbles_lost], 'rushing_first_downs' : [rushing_first_downs], 'rushing_epa_avg' : [rushing_epa_avg], 'rushing_2pt_conversions' : [rushing_2pt_conversions], 'rushing_yards_before_contact' : [rushing_yards_before_contact], 'rushing_yards_before_contact_avg' : [rushing_yards_before_contact_avg], 'rushing_yards_after_contact' : [rushing_yards_after_contact], 'rushing_yards_after_contact_avg' : [rushing_yards_after_contact_avg], 'rushing_broken_tackles' : [rushing_broken_tackles]})
        sum_df = pandas.concat([sum_df, new_row], ignore_index=True)
    return sum_df

def get_cumulative_weekly_receiving_df(player_list, timeframe_dict):
    df_weekly = get_weekly_receiving_df(player_list, timeframe_dict)
    df_weekly = df_weekly[['player_name', 'position', 'fantasy_points', 'fantasy_points_ppr', 'receptions', 'targets', 'receiving_yards', 'receiving_tds', 'receiving_fumbles', 'receiving_fumbles_lost', 'receiving_air_yards', 'receiving_yards_after_catch', 'receiving_first_downs', 'receiving_epa', 'receiving_2pt_conversions', 'racr', 'target_share', 'air_yards_share', 'wopr', 'receiving_broken_tackles', 'receiving_drop', 'receiving_drop_pct', 'receiving_int', 'receiving_rat', 'adot']]
    player_names = list(set(df_weekly['player_name'].tolist()))
    
    df_weekly['tgt_sh_numer'] = df_weekly['targets'] / df_weekly['target_share']
    df_weekly['ay_sh_numer'] = df_weekly['receiving_air_yards'] / df_weekly['air_yards_share']
    
    sum_df = pandas.DataFrame(columns=['player_name', 'position', 'games','fantasy_points', 'fantasy_points_ppr', 'receptions', 'targets', 'receiving_yards', 'receiving_tds', 'receiving_fumbles', 'receiving_fumbles_lost', 'receiving_air_yards', 'receiving_yards_after_catch', 'receiving_first_downs', 'receiving_epa_avg', 'receiving_2pt_conversions', 'racr', 'target_share', 'air_yards_share', 'wopr', 'receiving_broken_tackles', 'receiving_drop', 'receiving_drop_pct', 'receiving_int', 'receiving_rat_avg', 'adot'])
    for i in player_names:
        df_weekly_sub = df_weekly.loc[df_weekly['player_name']==i]
        df_weekly_sub.reset_index(inplace=True)
        player = i
        position = df_weekly_sub['position'][0]
        games = len(df_weekly_sub)
        fantasy_points = df_weekly_sub['fantasy_points'].sum()
        fantasy_points_ppr = df_weekly_sub['fantasy_points_ppr'].sum()
        receptions = df_weekly_sub['receptions'].sum()
        targets = df_weekly_sub['targets'].sum()
        receiving_yards = df_weekly_sub['receiving_yards'].sum()
        receiving_tds = df_weekly_sub['receiving_tds'].sum()
        receiving_fumbles = df_weekly_sub['receiving_fumbles'].sum()
        receiving_fumbles_lost = df_weekly_sub['receiving_fumbles_lost'].sum()
        receiving_air_yards = df_weekly_sub['receiving_air_yards'].sum()
        receiving_yards_after_catch = df_weekly_sub['receiving_yards_after_catch'].sum()
        receiving_first_downs = df_weekly_sub['receiving_first_downs'].sum()
        receiving_2pt_conversions = df_weekly_sub['receiving_2pt_conversions'].sum()
        receiving_broken_tackles = df_weekly_sub['receiving_broken_tackles'].sum()
        receiving_drop = df_weekly_sub['receiving_drop'].sum()
        receiving_drop_pct = df_weekly_sub['receiving_drop_pct'].sum()
        receiving_int = df_weekly_sub['receiving_int'].sum()
        
        tgt_sh_numer = df_weekly_sub['tgt_sh_numer'].sum()
        ay_sh_numer = df_weekly_sub['ay_sh_numer'].sum()
        
        target_share = targets/tgt_sh_numer
        air_yards_share = receiving_air_yards/ay_sh_numer
        
        receiving_rat_avg = df_weekly_sub['receiving_rat'].mean()
        receiving_epa_avg = df_weekly_sub['receiving_epa'].mean()
        
        racr = receiving_yards/receiving_air_yards
        wopr = (1.5*target_share) + (0.7*air_yards_share)
        adot = receiving_air_yards / targets
        new_row = pandas.DataFrame({'player_name' : [player], 'position' : [position], 'games' : [games], 'fantasy_points' : [fantasy_points], 'fantasy_points_ppr' : [fantasy_points_ppr], 'receptions' : [receptions], 'targets' : [targets], 'receiving_yards' : [receiving_yards], 'receiving_tds' : [receiving_tds], 'receiving_fumbles' : [receiving_fumbles], 'receiving_fumbles_lost' : [receiving_fumbles_lost], 'receiving_air_yards' : [receiving_air_yards], 'receiving_yards_after_catch' : [receiving_yards_after_catch], 'receiving_first_downs' : [receiving_first_downs], 'receiving_epa_avg' : [receiving_epa_avg], 'receiving_2pt_conversions' : [receiving_2pt_conversions], 'racr' : [racr], 'target_share' : [target_share], 'air_yards_share' : [air_yards_share], 'wopr' : [wopr], 'receiving_broken_tackles' : [receiving_broken_tackles], 'receiving_drop' : [receiving_drop], 'receiving_drop_pct' : [receiving_drop_pct], 'receiving_int' : [receiving_int], 'receiving_rat_avg' : [receiving_rat_avg], 'adot' : [adot]})
        sum_df = pandas.concat([sum_df, new_row], ignore_index=True)

    return sum_df

def get_cumulative_seasonal_passing_df(player_list, timeframe_list):
    df_seasonal = get_seasonal_passing_df(player_list, timeframe_list)

    player_names = list(set(df_seasonal['player_name'].tolist()))
    if 2018 in timeframe_list:
        df_seasonal = df_seasonal[['player_name', 'games', 'fantasy_points', 'fantasy_points_ppr', 'completions', 'attempts', 'passing_yards', 'passing_tds', 'interceptions', 'sacks', 'sack_yards', 'sack_fumbles', 'sack_fumbles_lost', 'passing_air_yards', 'passing_yards_after_catch', 'passing_first_downs', 'passing_epa', 'passing_2pt_conversions', 'pacr', 'dakota', 'pass_attempts', 'throwaways', 'spikes', 'drops', 'drop_pct', 'bad_throws', 'bad_throw_pct', 'pocket_time', 'times_blitzed', 'times_hurried', 'times_hit', 'times_pressured', 'pressure_pct']]
    
        df_seasonal['time_pressured_numer'] = df_seasonal['times_pressured'] / df_seasonal['pressure_pct']
        df_seasonal['passing_drop_numer'] = df_seasonal['drops'] / df_seasonal['drop_pct']
        df_seasonal['passing_bad_throws_numer'] = df_seasonal['bad_throws'] / df_seasonal['bad_throw_pct']
        
        sum_df = pandas.DataFrame(columns=['player_name', 'games', 'fantasy_points', 'fantasy_points_ppr', 'completions', 'attempts', 'passing_yards', 'passing_tds', 'interceptions', 'sacks', 'sack_yards', 'sack_fumbles', 'sack_fumbles_lost', 'passing_air_yards', 'passing_yards_after_catch', 'passing_first_downs', 'passing_epa_avg', 'passing_2pt_conversions', 'pacr', 'dakota_avg', 'pass_attempts', 'throwaways', 'spikes', 'drops', 'passing_drop_pct', 'bad_throws', 'passing_bad_throw_pct', 'pocket_time', 'times_blitzed', 'times_hurried', 'times_hit', 'times_pressured', 'times_pressured_pct'])
        for i in player_names:
            df_season_sub = df_seasonal.loc[df_seasonal['player_name']==i]
            df_season_sub.reset_index(inplace=True)
            player = i
            games = df_season_sub['games'].sum()
            fantasy_points = df_season_sub['fantasy_points'].sum()
            fantasy_points_ppr = df_season_sub['fantasy_points_ppr'].sum()
            completions = df_season_sub['completions'].sum()
            attempts = df_season_sub['attempts'].sum()
            passing_yards = df_season_sub['passing_yards'].sum()
            passing_tds = df_season_sub['passing_tds'].sum()
            interceptions = df_season_sub['interceptions'].sum()
            sacks = df_season_sub['sacks'].sum()
            sack_yards = df_season_sub['sack_yards'].sum()
            sack_fumbles = df_season_sub['sack_fumbles'].sum()
            sack_fumbles_lost = df_season_sub['sack_fumbles_lost'].sum()
            passing_air_yards = df_season_sub['passing_air_yards'].sum()
            passing_yards_after_catch = df_season_sub['passing_yards_after_catch'].sum()
            passing_first_downs = df_season_sub['passing_first_downs'].sum()
            passing_2pt_conversions = df_season_sub['passing_2pt_conversions'].sum()
            pass_attempts = df_season_sub['pass_attempts'].sum()
            throwaways = df_season_sub['throwaways'].sum()
            spikes = df_season_sub['spikes'].sum()
            drops = df_season_sub['drops'].sum()
            bad_throws = df_season_sub['bad_throws'].sum()
            pocket_time = df_season_sub['pocket_time'].sum()
            times_blitzed = df_season_sub['times_blitzed'].sum()
            times_hurried = df_season_sub['times_hurried'].sum()
            times_hit = df_season_sub['times_hit'].sum()
            times_pressured = df_season_sub['times_pressured'].sum()
            
            time_pressured_numer = df_season_sub['time_pressured_numer'].sum()
            passing_drop_numer = df_season_sub['passing_drop_numer'].sum()
            passing_bad_throws_numer = df_season_sub['passing_bad_throws_numer'].sum()
            
            times_pressured_pct = times_pressured/time_pressured_numer
            passing_drop_pct = drops/passing_drop_numer
            passing_bad_throw_pct = bad_throws/passing_bad_throws_numer
            
            pacr = passing_yards/passing_air_yards
        
            passing_epa_avg = df_season_sub['passing_epa'].mean()
            dakota_avg = df_season_sub['dakota'].mean()
            
            new_row = pandas.DataFrame({'player_name' : [player], 'games' : [games], 'fantasy_points' : [fantasy_points], 'fantasy_points_ppr' : [fantasy_points_ppr], 'completions' : [completions], 'attempts' : [attempts], 'passing_yards' : [passing_yards], 'passing_tds' : [passing_tds], 'interceptions' : [interceptions], 'sacks' : [sacks], 'sack_yards' : [sack_yards], 'sack_fumbles' : [sack_fumbles], 'sack_fumbles_lost' : [sack_fumbles_lost], 'passing_air_yards' : [passing_air_yards], 'passing_yards_after_catch' : [passing_yards_after_catch], 'passing_first_downs' : [passing_first_downs], 'passing_epa_avg' : [passing_epa_avg], 'passing_2pt_conversions' : [passing_2pt_conversions], 'pacr' : [pacr], 'dakota_avg' : [dakota_avg], 'pass_attempts' : [pass_attempts], 'throwaways' : [throwaways], 'spikes' : [spikes], 'drops' : [drops], 'passing_drop_pct' : [passing_drop_pct], 'bad_throws' : [bad_throws], 'passing_bad_throw_pct' : [passing_bad_throw_pct], 'pocket_time' : [pocket_time], 'times_blitzed' : [times_blitzed], 'times_hurried' : [times_hurried], 'times_hit' : [times_hit], 'times_pressured' : [times_pressured], 'times_pressured_pct' : [times_pressured_pct]})
            sum_df = pandas.concat([sum_df, new_row], ignore_index=True)

    else:
        df_seasonal = df_seasonal[['player_name', 'games', 'fantasy_points', 'fantasy_points_ppr', 'completions', 'attempts', 'passing_yards', 'passing_tds', 'interceptions', 'sacks', 'sack_yards', 'sack_fumbles', 'sack_fumbles_lost', 'passing_air_yards', 'passing_yards_after_catch', 'passing_first_downs', 'passing_epa', 'passing_2pt_conversions', 'pacr', 'dakota', 'pass_attempts', 'throwaways', 'spikes', 'drops', 'drop_pct', 'bad_throws', 'bad_throw_pct', 'pocket_time', 'times_blitzed', 'times_hurried', 'times_hit', 'times_pressured', 'pressure_pct', 'batted_balls', 'on_tgt_throws', 'on_tgt_pct', 'rpo_plays', 'rpo_yards', 'rpo_pass_att', 'rpo_pass_yards', 'rpo_rush_att', 'rpo_rush_yards', 'pa_pass_att', 'pa_pass_yards']]
        
        df_seasonal['time_pressured_numer'] = df_seasonal['times_pressured'] / df_seasonal['pressure_pct']
        df_seasonal['passing_drop_numer'] = df_seasonal['drops'] / df_seasonal['drop_pct']
        df_seasonal['passing_bad_throws_numer'] = df_seasonal['bad_throws'] / df_seasonal['bad_throw_pct']
        df_seasonal['on_tgt_numer'] = df_seasonal['on_tgt_throws'] / df_seasonal['on_tgt_pct']
        
        sum_df = pandas.DataFrame(columns=['player_name', 'games', 'fantasy_points', 'fantasy_points_ppr', 'completions', 'attempts', 'passing_yards', 'passing_tds', 'interceptions', 'sacks', 'sack_yards', 'sack_fumbles', 'sack_fumbles_lost', 'passing_air_yards', 'passing_yards_after_catch', 'passing_first_downs', 'passing_epa_avg', 'passing_2pt_conversions', 'pacr', 'dakota_avg', 'pass_attempts', 'throwaways', 'spikes', 'drops', 'passing_drop_pct', 'bad_throws', 'passing_bad_throw_pct', 'pocket_time', 'times_blitzed', 'times_hurried', 'times_hit', 'times_pressured', 'times_pressured_pct', 'batted_balls', 'on_tgt_throws', 'on_tgt_pct', 'rpo_plays', 'rpo_yards', 'rpo_pass_att', 'rpo_pass_yards', 'rpo_rush_att', 'rpo_rush_yards', 'pa_pass_att', 'pa_pass_yards'])
        for i in player_names:
            df_season_sub = df_seasonal.loc[df_seasonal['player_name']==i]
            df_season_sub.reset_index(inplace=True)
            player = i
            games = df_season_sub['games'].sum()
            fantasy_points = df_season_sub['fantasy_points'].sum()
            fantasy_points_ppr = df_season_sub['fantasy_points_ppr'].sum()
            completions = df_season_sub['completions'].sum()
            attempts = df_season_sub['attempts'].sum()
            passing_yards = df_season_sub['passing_yards'].sum()
            passing_tds = df_season_sub['passing_tds'].sum()
            interceptions = df_season_sub['interceptions'].sum()
            sacks = df_season_sub['sacks'].sum()
            sack_yards = df_season_sub['sack_yards'].sum()
            sack_fumbles = df_season_sub['sack_fumbles'].sum()
            sack_fumbles_lost = df_season_sub['sack_fumbles_lost'].sum()
            passing_air_yards = df_season_sub['passing_air_yards'].sum()
            passing_yards_after_catch = df_season_sub['passing_yards_after_catch'].sum()
            passing_first_downs = df_season_sub['passing_first_downs'].sum()
            passing_2pt_conversions = df_season_sub['passing_2pt_conversions'].sum()
            pass_attempts = df_season_sub['pass_attempts'].sum()
            throwaways = df_season_sub['throwaways'].sum()
            spikes = df_season_sub['spikes'].sum()
            drops = df_season_sub['drops'].sum()
            bad_throws = df_season_sub['bad_throws'].sum()
            pocket_time = df_season_sub['pocket_time'].sum()
            times_blitzed = df_season_sub['times_blitzed'].sum()
            times_hurried = df_season_sub['times_hurried'].sum()
            times_hit = df_season_sub['times_hit'].sum()
            times_pressured = df_season_sub['times_pressured'].sum()
            batted_balls = df_season_sub['batted_balls'].sum()
            on_tgt_throws = df_season_sub['on_tgt_throws'].sum()
            rpo_plays = df_season_sub['rpo_plays'].sum()
            rpo_yards = df_season_sub['rpo_yards'].sum()
            rpo_pass_att = df_season_sub['rpo_pass_att'].sum()
            rpo_pass_yards = df_season_sub['rpo_pass_yards'].sum()
            rpo_rush_att = df_season_sub['rpo_rush_att'].sum()
            rpo_rush_yards = df_season_sub['rpo_rush_yards'].sum()
            pa_pass_att = df_season_sub['pa_pass_att'].sum()
            pa_pass_yards = df_season_sub['pa_pass_yards'].sum()
            
            time_pressured_numer = df_season_sub['time_pressured_numer'].sum()
            passing_drop_numer = df_season_sub['passing_drop_numer'].sum()
            passing_bad_throws_numer = df_season_sub['passing_bad_throws_numer'].sum()
            on_tgt_numer = df_season_sub['on_tgt_numer'].sum()
            
            times_pressured_pct = times_pressured/time_pressured_numer
            passing_drop_pct = drops/passing_drop_numer
            passing_bad_throw_pct = bad_throws/passing_bad_throws_numer
            on_tgt_pct = on_tgt_throws/on_tgt_numer
            
            pacr = passing_yards/passing_air_yards
        
            passing_epa_avg = df_season_sub['passing_epa'].mean()
            dakota_avg = df_season_sub['dakota'].mean()
            
            new_row = pandas.DataFrame({'player_name' : [player], 'games' : [games], 'fantasy_points' : [fantasy_points], 'fantasy_points_ppr' : [fantasy_points_ppr], 'completions' : [completions], 'attempts' : [attempts], 'passing_yards' : [passing_yards], 'passing_tds' : [passing_tds], 'interceptions' : [interceptions], 'sacks' : [sacks], 'sack_yards' : [sack_yards], 'sack_fumbles' : [sack_fumbles], 'sack_fumbles_lost' : [sack_fumbles_lost], 'passing_air_yards' : [passing_air_yards], 'passing_yards_after_catch' : [passing_yards_after_catch], 'passing_first_downs' : [passing_first_downs], 'passing_epa_avg' : [passing_epa_avg], 'passing_2pt_conversions' : [passing_2pt_conversions], 'pacr' : [pacr], 'dakota_avg' : [dakota_avg], 'pass_attempts' : [pass_attempts], 'throwaways' : [throwaways], 'spikes' : [spikes], 'drops' : [drops], 'passing_drop_pct' : [passing_drop_pct], 'bad_throws' : [bad_throws], 'passing_bad_throw_pct' : [passing_bad_throw_pct], 'pocket_time' : [pocket_time], 'times_blitzed' : [times_blitzed], 'times_hurried' : [times_hurried], 'times_hit' : [times_hit], 'times_pressured' : [times_pressured], 'times_pressured_pct' : [times_pressured_pct], 'batted_balls' : [batted_balls], 'on_tgt_throws' : [on_tgt_throws], 'on_tgt_pct' : [on_tgt_pct], 'rpo_plays' : [rpo_plays], 'rpo_yards' : [rpo_yards], 'rpo_pass_att' : [rpo_pass_att], 'rpo_pass_yards' : [rpo_pass_yards], 'rpo_rush_att' : [rpo_rush_att], 'rpo_rush_yards' : [rpo_rush_yards], 'pa_pass_att' : [pa_pass_att], 'pa_pass_yards' : [pa_pass_yards]})
            sum_df = pandas.concat([sum_df, new_row], ignore_index=True)
            
    return sum_df

def get_cumulative_seasonal_rushing_df(player_list, timeframe_list):
    df_seasonal = get_seasonal_rushing_df(player_list, timeframe_list)
    df_seasonal = df_seasonal[['player_name', 'pos', 'games', 'fantasy_points', 'fantasy_points_ppr', 'carries', 'rushing_yards', 'rushing_tds', 'rushing_fumbles', 'rushing_fumbles_lost', 'rushing_first_downs', 'rushing_epa', 'rushing_2pt_conversions', 'ybc', 'yac', 'brk_tkl']]
    player_names = list(set(df_seasonal['player_name'].tolist()))
    
    sum_df = pandas.DataFrame(columns=['player_name', 'position', 'games', 'fantasy_points', 'fantasy_points_ppr', 'carries', 'rushing_yards', 'rushing_tds', 'rushing_fumbles', 'rushing_fumbles_lost', 'rushing_first_downs', 'rushing_epa_avg', 'rushing_2pt_conversions', 'ybc', 'ybc_per_att', 'yac', 'yac_per_att', 'brk_tkl', 'carry_per_brk_tkl'])
    for i in player_names:
            df_season_sub = df_seasonal.loc[df_seasonal['player_name']==i]
            df_season_sub.reset_index(inplace=True)
            position = df_season_sub['pos'][0]
            player = i
            games = df_season_sub['games'].sum()
            fantasy_points = df_season_sub['fantasy_points'].sum()
            fantasy_points_ppr = df_season_sub['fantasy_points_ppr'].sum()
            carries = df_season_sub['carries'].sum()
            rushing_yards = df_season_sub['rushing_yards'].sum()
            rushing_tds = df_season_sub['rushing_tds'].sum()
            rushing_fumbles = df_season_sub['rushing_fumbles'].sum()
            rushing_fumbles_lost = df_season_sub['rushing_fumbles_lost'].sum()
            rushing_first_downs = df_season_sub['rushing_first_downs'].sum()
            rushing_epa_avg = df_season_sub['rushing_epa'].mean()/games
            rushing_2pt_conversions = df_season_sub['rushing_2pt_conversions'].sum()
            ybc = df_season_sub['ybc'].sum()
            ybc_per_att = ybc/carries
            yac = df_season_sub['yac'].sum()
            yac_per_att = yac/carries
            brk_tkl = df_season_sub['brk_tkl'].sum()
            carry_per_brk_tkl = carries/brk_tkl
            
            new_row = pandas.DataFrame({'player_name' : [player], 'position' : [position], 'games' : [games], 'fantasy_points' : [fantasy_points], 'fantasy_points_ppr' : [fantasy_points_ppr], 'carries' : [carries], 'rushing_yards' : [rushing_yards], 'rushing_tds' : [rushing_tds], 'rushing_fumbles' : [rushing_fumbles], 'rushing_fumbles_lost' : [rushing_fumbles_lost], 'rushing_first_downs' : [rushing_first_downs], 'rushing_epa_avg' : [rushing_epa_avg], 'rushing_2pt_conversions' : [rushing_2pt_conversions], 'ybc' : [ybc], 'ybc_per_att' : [ybc_per_att], 'yac' : [yac], 'yac_per_att' : [yac_per_att], 'brk_tkl' : [brk_tkl], 'carry_per_brk_tkl' : [carry_per_brk_tkl]})
            sum_df = pandas.concat([sum_df, new_row], ignore_index=True)
    return sum_df

def get_cumulative_seasonal_receiving_df(player_list, timeframe_list):
    df_seasonal = get_seasonal_rec_df(player_list, timeframe_list)
    df_seasonal = df_seasonal[['player_name', 'position', 'games', 'fantasy_points', 'fantasy_points_ppr', 'receptions', 'targets', 'receiving_yards', 'receiving_tds', 'receiving_fumbles', 'receiving_fumbles_lost', 'receiving_air_yards', 'receiving_yards_after_catch', 'receiving_first_downs', 'receiving_epa', 'receiving_2pt_conversions', 'racr', 'target_share', 'air_yards_share', 'wopr', 'ybcatch', 'ybcatch_per_rec', 'yacatch', 'yacatch_per_rec', 'adot', 'brk_tkl', 'rec_per_brk_tkl', 'drop', 'drop_percent', 'int']]
    player_names = list(set(df_seasonal['player_name'].tolist()))
    
    df_seasonal['total_tgt'] = df_seasonal['targets']/df_seasonal['target_share']
    df_seasonal['total_ay'] = df_seasonal['receiving_air_yards']/df_seasonal['air_yards_share']

    
    sum_df = pandas.DataFrame(columns=['player', 'position', 'games', 'fantasy_points', 'fantasy_points_ppr', 'receptions', 'targets', 'receiving_yards', 'receiving_tds', 'receiving_fumbles', 'receiving_fumbles_lost', 'receiving_air_yards', 'receiving_yards_after_catch', 'receiving_first_downs', 'receiving_epa_avg', 'receiving_2pt_conversions', 'racr', 'target_share', 'air_yards_share', 'wopr', 'ybcatch', 'ybcatch_per_rec', 'yacatch', 'yacatch_per_rec', 'adot', 'brk_tkl', 'rec_per_brk_tkl', 'drop', 'drop_percent', 'interceptions'])
    for i in player_names:
            df_season_sub = df_seasonal.loc[df_seasonal['player_name']==i]
            df_season_sub.reset_index(inplace=True)
            position = df_season_sub['position'][0]
            player = i
            games = df_season_sub['games'].sum()
            fantasy_points = df_season_sub['fantasy_points'].sum()
            fantasy_points_ppr = df_season_sub['fantasy_points_ppr'].sum()
            receptions = df_season_sub['receptions'].sum()
            targets = df_season_sub['targets'].sum()
            receiving_yards = df_season_sub['receiving_yards'].sum()
            receiving_tds = df_season_sub['receiving_tds'].sum()
            receiving_fumbles = df_season_sub['receiving_fumbles'].sum()
            receiving_fumbles_lost = df_season_sub['receiving_fumbles_lost'].sum()
            receiving_air_yards = df_season_sub['receiving_air_yards'].sum()
            receiving_yards_after_catch = df_season_sub['receiving_yards_after_catch'].sum()
            receiving_first_downs = df_season_sub['receiving_first_downs'].sum()
            receiving_2pt_conversions = df_season_sub['receiving_2pt_conversions'].sum()
            ybcatch = df_season_sub['ybcatch'].sum()
            yacatch = df_season_sub['yacatch'].sum()
            brk_tkl = df_season_sub['brk_tkl'].sum()
            drop = df_season_sub['drop'].sum()
            interceptions = df_season_sub['int'].sum()
            
            total_tgt = df_seasonal['total_tgt'].sum()
            total_ay = df_seasonal['total_ay'].sum()
            
            target_share = targets/total_tgt
            air_yards_share = receiving_air_yards/total_ay
            
            drop_percent = drop/targets
            receiving_epa_avg = df_seasonal['receiving_epa'].mean()/games
            racr = receiving_yards/receiving_air_yards
            wopr = (1.5*target_share) + (0.7*air_yards_share)
            ybcatch_per_rec = ybcatch/receptions
            yacatch_per_rec = yacatch/receptions
            adot = receiving_air_yards/targets
            rec_per_brk_tkl = receptions/brk_tkl

            new_row = pandas.DataFrame({'player' : [player], 'position' : [position], 'games' : [games], 'fantasy_points' : [fantasy_points], 'fantasy_points_ppr' : [fantasy_points_ppr], 'receptions' : [receptions], 'targets' : [targets], 'receiving_yards' : [receiving_yards], 'receiving_tds' : [receiving_tds], 'receiving_fumbles' : [receiving_fumbles], 'receiving_fumbles_lost' : [receiving_fumbles_lost], 'receiving_air_yards' : [receiving_air_yards], 'receiving_yards_after_catch' : [receiving_yards_after_catch], 'receiving_first_downs' : [receiving_first_downs], 'receiving_epa_avg' : [receiving_epa_avg], 'receiving_2pt_conversions' : [receiving_2pt_conversions], 'racr' : [racr], 'target_share' : [target_share], 'air_yards_share' : [air_yards_share], 'wopr' : [wopr], 'ybcatch' : [ybcatch], 'ybcatch_per_rec' : [ybcatch_per_rec], 'yacatch' : [yacatch], 'yacatch_per_rec' : [yacatch_per_rec], 'adot' : [adot], 'brk_tkl' : [brk_tkl], 'rec_per_brk_tkl' : [rec_per_brk_tkl], 'drop' : [drop], 'drop_percent' : [drop_percent], 'interceptions' : [interceptions]})
            sum_df = pandas.concat([sum_df, new_row], ignore_index=True)
    return sum_df

def get_comp(player_list, timeframe_list):
    comp_file = r"Top_Competition_CORE.xlsx"
    comp_df = pandas.read_excel(comp_file)
    player_list_T = [transform_name(x) for x in player_list]
    seasons = timeframe_list
    df = comp_df.loc[(comp_df['alt_name'].isin(player_list_T)) & (comp_df['season'].isin(seasons))]
    if len(df) > 0:
        df_final = df[['player_name', 'season', 'top_competitor_name']]
        df_final.reset_index(drop=True, inplace=True)
    else:
        df_final = pandas.DataFrame(columns=['player_name', 'season', 'top_competitor_name'])
    return df_final

def get_nonqb_qbr_weekly(player_list, timeframe_dict):
    with open("team_qb_pass_rating_dict.pkl", "rb") as f:
        team_qbr_dic = pickle.load(f)
    seasons = list(timeframe_dict.keys())
    
    misc_cols_wk = ['player_display_name', 'position', 'recent_team', 'season', 'week']
    player_loc = [transform_name(x) for x in player_list]
    player_name_dict = dict(zip(player_loc, player_list))
    misc_df_basic = import_weekly_data(years=seasons,columns=misc_cols_wk)
    misc_df_basic = transform_col(misc_df_basic,'player_display_name', 'player_name_alt')
    #misc_df_basic.rename(columns={'player_display_name' : 'player_name'}, inplace=True)
    misc_df_basic = misc_df_basic.loc[(misc_df_basic['player_name_alt'].isin(player_loc)) & (misc_df_basic['position'] != 'QB')]
    player_name_list = []
    for index, row in misc_df_basic.iterrows():
        player_name_list.append(player_name_dict[row['player_name_alt']])
        
    misc_df_basic['player_name'] = player_name_list
    misc_df = pandas.DataFrame(columns=['player_name', 'season', 'week', 'team_qbr'])

    if len(misc_df_basic) > 0:
        for key in timeframe_dict:
            sub_df = misc_df_basic.loc[(misc_df_basic['season']==key) & (misc_df_basic['week'].isin(timeframe_dict[key]))]
            qbr = []
            if len(sub_df) > 0:
                for index, row in sub_df.iterrows():
                    key = (row['season'], row['recent_team'], row['week'])
                    if key in team_qbr_dic:
                        qbr.append(team_qbr_dic[key])
                    else:
                        qbr.append(None)
                sub_df['team_qbr'] = qbr
                sub_df = sub_df[['player_name', 'season', 'week', 'team_qbr']]
                misc_df = pandas.concat([misc_df, sub_df], ignore_index=True)
    return misc_df

def get_qb_qbr_weekly(player_list, timeframe_dict):
    with open("qb_pass_rating_dict.pkl", "rb") as f:
        player_qbr_dic = pickle.load(f)
    seasons = list(timeframe_dict.keys())
    
    misc_cols_wk = ['player_display_name', 'position', 'recent_team', 'season', 'week']
    player_loc = player_list
    
    misc_df_basic = import_weekly_data(years=seasons,columns=misc_cols_wk)
    misc_df_basic.rename(columns={'player_display_name' : 'player_name'}, inplace=True)
    misc_df_basic = misc_df_basic.loc[(misc_df_basic['player_name'].isin(player_loc)) & (misc_df_basic['position'] == 'QB')]
    
    misc_df = pandas.DataFrame(columns=['player_name', 'season', 'week', 'player_qbr'])

    if len(misc_df_basic) > 0:
        for key in timeframe_dict:
            sub_df = misc_df_basic.loc[(misc_df_basic['season']==key) & (misc_df_basic['week'].isin(timeframe_dict[key]))]
            qbr = []
            if len(sub_df) > 0:
                for index, row in sub_df.iterrows():
                    key = (row['season'], row['recent_team'], row['week'], row['player_name'])
                    if key in player_qbr_dic:
                        qbr.append(player_qbr_dic[key])
                    else:
                        qbr.append(None)
                sub_df['player_qbr'] = qbr
                sub_df = sub_df[['player_name', 'season', 'week', 'player_qbr']]
                misc_df = pandas.concat([misc_df, sub_df], ignore_index=True)
    return misc_df


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
    data_format = granularity
    passing = False
    rushing = False
    receiving = False
    misc = False
    df_list = []
    if "Passing" in data_def:
        passing = True
        if weekly and data_format == "Week":
            df1 = get_weekly_passing_df(player_names, timeframe)
        elif seasonal and data_format == "Season":
            df1 = get_seasonal_passing_df(player_names, timeframe)
        elif weekly and data_format == 'Cumulative':
            df1 = get_cumulative_weekly_passing_df(player_names, timeframe)
        elif seasonal and data_format == 'Cumulative':
            df1 = get_cumulative_seasonal_passing_df(player_names, timeframe)
        df_list.append(df1)
    if "Rushing" in data_def:
        rushing = True
        if weekly and data_format == "Week":
            df2 = get_weekly_rushing_df(player_names, timeframe)
        elif seasonal and data_format == "Season":
            df2 = get_seasonal_rushing_df(player_names, timeframe)
        elif weekly and data_format == 'Cumulative':
            df2 = get_cumulative_weekly_rushing_df(player_names, timeframe)
        elif seasonal and data_format == 'Cumulative':
            df2 = get_cumulative_seasonal_rushing_df(player_names, timeframe)
        df_list.append(df2)
        
    if "Receiving" in data_def:
        receiving = True
        if weekly and data_format == "Week":
            df3 = get_weekly_receiving_df(player_names, timeframe)
        elif seasonal and data_format == "Season":
            df3 = get_seasonal_rec_df(player_names, timeframe)
        elif weekly and data_format == 'Cumulative':
            df3 = get_cumulative_weekly_receiving_df(player_names, timeframe)
        elif seasonal and data_format == 'Cumulative':
            df3 = get_cumulative_seasonal_receiving_df(player_names, timeframe)
        df_list.append(df3)
    if "Misc." in data_def:
        if weekly and data_format == "Week":
            if rushing or receiving:
                misc = True
                df_qbr = get_nonqb_qbr_weekly(player_names, timeframe)
                df_list.append(df_qbr)
            if passing:
                misc = True
                df_qb_qbr = get_qb_qbr_weekly(player_names, timeframe)
                df_list.append(df_qb_qbr)
        elif seasonal and data_format == "Season":
            if rushing or receiving:
                misc = True
                df_comp = get_comp(player_names, timeframe)
                df_list.append(df_comp)
        elif weekly and data_format == 'Cumulative':
            pass
        elif seasonal and data_format == 'Cumulative':
            pass
    
    merged_df = df_list[0]
    if data_format == "Week":           # Outter Joins to 1 DF
        root_cols = ['player_name', 'season', 'week']
        overlap_cols = ['position', 'recent_team', 'season_type', 'opponent_team', 'fantasy_points', 'fantasy_points_ppr']
        for i in range(1, len(df_list)):
            merged_df = pandas.merge(merged_df, df_list[i], on=root_cols, how='outer')
        for col in merged_df.columns:
            overlaps = {}
            if col in overlap_cols:
                z = merged_df[col].count()
                overlaps[col] = z
                if col+str("_x") in merged_df.columns:
                    x = merged_df[col+str("_x")].count()
                    overlaps[col+str("_x")] = x
                if col+str("_y") in merged_df.columns:
                    y = merged_df[col+str("_y")].count()
                    overlaps[col+str("_y")] = y
                if z == len(merged_df):
                    core_col = col
                else:
                    core_col = max(overlaps, key=overlaps.get)
                if len(overlaps) > 1:
                    overlaps.pop(core_col)
                    for key in overlaps:
                        merged_df[core_col] = merged_df[core_col].fillna(merged_df[key])
                    merged_df = merged_df.drop(columns=list(overlaps.keys()))                        
        all_cols = merged_df.columns.tolist()
        left_cols = root_cols + overlap_cols
        right_cols = remove_strings(all_cols, left_cols)
        merged_df = merged_df[left_cols + right_cols]
        merged_df = merged_df.replace(0, np.nan)
        merged_df = merged_df.sort_values(by=root_cols)
        meta_cols = overlap_cols
        return merged_df
    if data_format == "Season":
        merged_df['player_name'] = merged_df['player_name'].str.replace("Jr.", "")
        merged_df['player_name'] = merged_df['player_name'].str.replace("'", "")
        merged_df['player_name'] = merged_df['player_name'].str.replace(".", "")
        merged_df['player_name'] = merged_df['player_name'].str.lower()
        root_cols = ['player_name', 'season']
        overlap_cols = ['team', 'games', 'season_type', 'fantasy_points', 'fantasy_points_ppr', 'player_id', 'age_x', 'brk_tkl_x']
        for i in range(1, len(df_list)):
            df_list[i]['player_name'] = df_list[i]['player_name'].str.replace("Jr.", "")
            df_list[i]['player_name'] = df_list[i]['player_name'].str.replace("'", "")
            df_list[i]['player_name'] = df_list[i]['player_name'].str.replace(".", "")
            df_list[i]['player_name'] = df_list[i]['player_name'].str.lower()
            merged_df = pandas.merge(merged_df, df_list[i], on=root_cols, how='outer')
        for col in merged_df.columns:
            overlaps = {}
            if col in overlap_cols:
                z = merged_df[col].count()
                overlaps[col] = z
                if col+str("_x") in merged_df.columns:
                    x = merged_df[col+str("_x")].count()
                    overlaps[col+str("_x")] = x
                if col+str("_y") in merged_df.columns:
                    y = merged_df[col+str("_y")].count()
                    overlaps[col+str("_y")] = y
                if col.replace("_x", "_y") in merged_df.columns:
                    xy = merged_df[col.replace("_x", "_y")].count()
                    overlaps[col.replace("_x", "_y")] = xy
                if z == len(merged_df):
                    core_col = col
                else:
                    core_col = max(overlaps, key=overlaps.get)
                if len(overlaps) > 1:
                    overlaps.pop(core_col)
                    merged_df = merged_df.drop(columns=list(overlaps.keys()))                        
        all_cols = merged_df.columns.tolist()
        left_cols = root_cols + overlap_cols
        right_cols = remove_strings(all_cols, left_cols)
        merged_df = merged_df[left_cols + right_cols]
        merged_df = merged_df.replace(0, np.nan)
        merged_df = merged_df.sort_values(by=root_cols)
        merged_df.rename(columns={'age_x' : 'age', 'brk_tkl_x' : 'brk_tkl'}, inplace=True)
        merged_df = merged_df.drop(columns=['player_id', 'season_type'])
        return merged_df
    if data_format == "Cumulative":
        root_cols = ['player_name']
        for i in range(1, len(df_list)):
            merged_df = pandas.merge(merged_df, df_list[i], on=root_cols, how='outer')
        
        merged_df = merged_df.replace(0, np.nan)
        merged_df = merged_df.sort_values(by=root_cols)
        return merged_df
    
