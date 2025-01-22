# -*- coding: utf-8 -*-
"""
Created on Thu Jan 16 22:26:10 2025

@author: nick_
"""
import pygame
import math
import sys
import os
from warnings import warn
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy
import pandas
import appdirs
from urllib.error import HTTPError

pygame.init()

info = pygame.display.Info()

############################################################################## Get screen width and height
screen_width = info.current_w
screen_height = info.current_h

RED = (255, 0, 0)
GREEN = (0, 255, 0)
BLUE = (0, 0, 255)
WHITE = (255, 255, 255)
BLACK = (0, 0, 0)
YELLOW = (255, 255, 0)
ORANGE = (255, 140, 0)
PURPLE = (207, 159, 255)
TURQUOISE = (64, 224, 208)
GREY = (128, 128, 128)
DARK_GREEN = (0, 100, 0)
DARK_RED = (100, 0, 0)
SLATE_GREY = (47,79,79)
LIGHT_BLUE = (0, 191, 255)

field_ratio = 0.444
FPS = 300

WIDTH, HEIGHT = round(screen_width/1.6), round(screen_height/1.2)
WIN = pygame.display.set_mode((WIDTH,HEIGHT))

spacer_unit = int(WIDTH/24)
spacer_unit_small = int(WIDTH/120)
font_size = int(spacer_unit_small)

clock = pygame.time.Clock()

field_x = spacer_unit
field_y = spacer_unit
field_width = WIDTH - (12*spacer_unit_small)
field_height = round(field_ratio*field_width)
field_size = (field_x, field_y, field_width, field_height)
yard = field_width/(12*spacer_unit_small)
end_z_1 = pygame.Rect(spacer_unit, spacer_unit, yard*spacer_unit_small, field_height)
end_z_2 = pygame.Rect(field_x + field_width - (yard*spacer_unit_small), field_y, yard*spacer_unit_small, field_height)

chains = pygame.Rect(end_z_1.right, spacer_unit, yard, field_height)

pos_yard_line = pygame.Rect(end_z_1.right, spacer_unit, yard, field_height)

text_box = pygame.Rect(field_x, field_y + field_height, field_width, round(0.5*field_height))
env_box = pygame.Rect(field_x + field_width/4, field_y - (3*spacer_unit_small), field_width/2, (3*spacer_unit_small))
coach_box = pygame.Rect(field_x, field_y + field_height + text_box.height, field_width, (HEIGHT - text_box.bottom)/2)

font = pygame.font.SysFont(r"arial", round(1.2*font_size), True, False)
font2 = pygame.font.SysFont(r"arial", round(1.2*font_size), True, True)
font3 = pygame.font.SysFont(r"arial", 2*font_size)

season_options = list(range(2000, 2025))
team_options =  ('WAS', 'TEN', 'TB', 'SF', 'SEA', 'PIT', 'PHI', 'NYJ', 'NYG', 'NO', 'NE', 'MIN', 'MIA', 'LV', 'LAC', 'LA', 'KC', 'JAX', 'IND', 'HOU', 'GB', 'DET', 'DEN', 'DAL', 'CLE', 'CIN', 'CHI', 'CAR', 'BUF', 'BAL', 'ATL', 'ARI')
#cache_path = r"C:\Users\nick_\OneDrive\Desktop\Python\Projects\Fantasy\replay_test\cache"

if getattr(sys, 'frozen', False):
    # Executable is running in PyInstaller bundle
    current_file_path = sys._MEIPASS
else:
    # Executable is running in normal Python environment
    current_file_path = os.path.dirname(os.path.abspath(__file__))

cache_path_g = os.path.dirname(current_file_path) + r"\cache"

x_pic = pygame.image.load(r"pics\x.png")
check_pic = pygame.image.load(r"pics\check.png")

x_pic = pygame.transform.scale(x_pic, (spacer_unit, spacer_unit))
check_pic = pygame.transform.scale(check_pic, (spacer_unit, spacer_unit))

class Yard_Lines:
    def __init__(self, field):
        self.lines = []
        self.color = BLACK
        self.height = field[3]
        self.position = (field[0] + (yard*spacer_unit_small), field[1])
    def get_lines(self):
        for i in range(0, 11):
            self.lines.append(pygame.Rect((self.position[0] + i*(yard*spacer_unit_small)), self.position[1], yard, self.height))

class game_text:
    def __init__(self, surf_x, surf_y, surf_width, surf_height):
        self.surf_x = surf_x
        self.surf_y = surf_y
        self.x = surf_x + spacer_unit_small
        self.y = surf_y + spacer_unit_small
        self.surf_width = surf_width
        self.surf_height = surf_height
        self.text = ''
    def write(self, text):
        self.text = text
        
    def draw(self, win):
        title_text = font2.render(r"Play by Play", 1, BLACK)
        win.blit(title_text, (self.x, self.y - int(spacer_unit/2)))
        if len(self.text) > 0:
            raw_text = font.render(self.text, 1, WHITE)
            measure_stick = pygame.Rect(self.surf_x, self.surf_y, self.surf_width, self.surf_height)
            pygame.draw.rect(win, SLATE_GREY, measure_stick)
            if raw_text.get_width() < self.surf_width - spacer_unit_small:
                win.blit(raw_text, (self.x, self.y))
            else:
                words = self.text.split(' ')
                overflow = []
                for i in range(len(words)):
                    overflow.append(words[-1])
                    words.pop()
                    line1 = " ".join(words)
                    raw_text1 = font.render(line1, 1, WHITE)
                    if raw_text1.get_width() < self.surf_width - spacer_unit_small:
                        overflow.reverse()
                        line2 = " ".join(overflow)
                        break
                raw_text2 = font.render(line2, 1, WHITE)
                win.blit(raw_text1, (self.x, self.y))
                if raw_text2.get_width() < self.surf_width - spacer_unit_small:
                    win.blit(raw_text2, (self.x, self.y + spacer_unit_small + 10))
                else:
                    words = overflow
                    overflow = []
                    for i in range(len(words)):
                        overflow.append(words[-1])
                        words.pop()
                        line1 = " ".join(words)
                        raw_text2 = font.render(line1, 1, WHITE)
                        if raw_text2.get_width() < self.surf_width - spacer_unit_small:
                            overflow.reverse()
                            line3 = " ".join(overflow)
                            break
                    raw_text3 = font.render(line3, 1, WHITE)
                    win.blit(raw_text2, (self.x, self.y + spacer_unit_small + 10))
                    if raw_text3.get_width() < self.surf_width - spacer_unit_small:
                        win.blit(raw_text3, (self.x, self.y + 2*(spacer_unit_small + 10)))
                    else:
                        words = overflow
                        overflow = []
                        for i in range(len(words)):
                            overflow.append(words[-1])
                            words.pop()
                            line1 = " ".join(words)
                            raw_text3 = font.render(line1, 1, WHITE)
                            if raw_text3.get_width() < self.surf_width - spacer_unit_small:
                                overflow.reverse()
                                line4 = " ".join(overflow)
                                break
                        raw_text4 = font.render(line4, 1, WHITE)
                        win.blit(raw_text3, (self.x, self.y + 2*(spacer_unit_small + 10)))
                        if raw_text4.get_width() < self.surf_width - spacer_unit_small:
                            win.blit(raw_text4, (self.x, self.y + 3*(spacer_unit_small + 10)))
            
############################################################################## Football data
def import_pbp_data(
        years, 
        columns=None, 
        include_participation=True, 
        downcast=True, 
        cache=False, 
        alt_path=None,
        thread_requests=False
    ):
    """Imports play-by-play data
    
    Args:
        years (List[int]): years to get PBP data for
        columns (List[str]): only return these columns
        include_participation (bool): whether to include participation stats or not
        downcast (bool): convert float64 to float32, default True
        cache (bool): whether to use local cache as source of pbp data
        alt_path (str): path for cache if not nfl_data_py default
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

    columns = [x for x in columns if x not in ['season']]
    
    if all([include_participation, len(columns) != 0]):
        columns = columns + [x for x in ['play_id','old_game_id'] if x not in columns]
       
    # potential sources for pbp data
    url1 = r'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_'
    url2 = r'.parquet'
    appname = 'nfl_data_py'
    appauthor = 'cooper_dff'
    pbp_data = []
    
    if cache:
        if not alt_path:
            dpath = os.path.join(appdirs.user_cache_dir(appname, appauthor), 'pbp')
        else:
            dpath = alt_path

    if thread_requests and not cache:
        with ThreadPoolExecutor() as executor:
            # Create a list of the same size as years, initialized with None
            pbp_data = [None]*len(years)
            # Create a mapping of futures to their corresponding index in the pbp_data
            futures_map = {
                executor.submit(
                    pandas.read_parquet,
                    path=url1 + str(year) + url2,
                    columns=columns if columns else None, 
                    engine='auto'
                ): idx 
                for idx, year in enumerate(years)
            }
            for future in as_completed(futures_map):
                pbp_data[futures_map[future]] = future.result()
    else:
        # read in pbp data
        for year in years:
            if cache:
                seasonStr = f'season={year}'
                if not os.path.isdir(os.path.join(dpath, seasonStr)):
                    print(dpath)
                    raise ValueError(f'{year} cache file does not exist.')
                for fname in filter(lambda x: seasonStr in x, os.listdir(dpath)):
                    folder = os.path.join(dpath, fname)
                    print(dpath)
                    for file in os.listdir(folder):
                        if file.endswith(".parquet"):
                            fpath = os.path.join(folder, file)
                
            # define path based on cache and alt_path variables
                path = fpath
            else:
                path = url1 + str(year) + url2

            # load data
            try:

                data = pandas.read_parquet(path, columns=columns if columns else None)


                raw = pandas.DataFrame(data)
                raw['season'] = year
                

                if include_participation and not cache:
                    path = r'https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/pbp_participation_{}.parquet'.format(year)

                    try:
                        partic = pandas.read_parquet(path)
                        raw = raw.merge(
                            partic,
                            how='left',
                            left_on=['play_id','game_id'],
                            right_on=['play_id','nflverse_game_id']
                        )
                    except HTTPError:
                        pass
                
                pbp_data.append(raw)
                print(str(year) + ' done.')

            except Exception as e:
                print(e)
                print('Data not available for ' + str(year))
    
    if not pbp_data:
        return pandas.DataFrame()
    
    plays = pandas.concat(pbp_data, ignore_index=True)

    # converts float64 to float32, saves ~30% memory
    if downcast:
        print('Downcasting floats.')
        cols = plays.select_dtypes(include=[numpy.float64]).columns
        plays[cols] = plays[cols].astype(numpy.float32)
            
    return plays


def cache_pbp(years, downcast=True, alt_path=None):
    """Cache pbp data in local location to allow for faster loading

    Args:
        years (List[int]): years to cache PBP data for
        downcast (bool): convert float64 to float32, default True
        alt_path (str): path for cache if not nfl_data_py default
    Returns:
        DataFrame
    """

    if not isinstance(years, (list, range)):
        raise ValueError('Input must be list or range.')

    if min(years) < 1999:
        raise ValueError('Data not available before 1999.')

    url1 = r'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_'
    url2 = r'.parquet'
    appname = 'nfl_data_py'
    appauthor = 'nflverse'

    # define path for caching
    if alt_path is not None:
        path = str(alt_path)
    else:
        path = os.path.join(appdirs.user_cache_dir(appname, appauthor), 'pbp')

    # check if drectory exists already
    if not os.path.isdir(path):
        os.makedirs(path)

    # delete seasons to be replaced
    for folder in [os.path.join(path, x) for x in os.listdir(path) for y in years if ('season='+str(y)) in x]:
        for file in os.listdir(folder):
            if file.endswith(".parquet"):
                os.remove(os.path.join(folder, file))

    # read in pbp data
    for year in years:

        try:

            data = pandas.read_parquet(url1 + str(year) + url2, engine='auto')

            raw = pandas.DataFrame(data)
            raw['season'] = year

            if year >= 2016:
                path2 = r'https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/pbp_participation_{}.parquet'.format(year)
                part = pandas.read_parquet(path2)
                raw = raw.merge(part, how='left', on=['play_id','old_game_id'])

            if downcast:
                cols = raw.select_dtypes(include=[numpy.float64]).columns
                raw[cols] = raw[cols].astype(numpy.float32)

            # write parquet to path, partitioned by season
            raw.to_parquet(path, partition_cols='season')

            print(str(year) + ' done.')

        except Exception as e:
            warn(
                f"Caching failed for {year}, skipping.\n"
                "In nfl_data_py 1.0, this will raise an exception.\n"
                f"Failure: {e}",
                DeprecationWarning,
                stacklevel=2
            )

            next

def get_football_data(game_season=int, game_team=str, game_week=int, cache=str):
    cache_path = cache
    seasons = [game_season]
    team = game_team
    week = game_week
    season_str = f'season={game_season}'
    if seasons[0] == 2024:
        game_df = import_pbp_data([2024],downcast=True)
    
    else:
        if os.path.isdir(os.path.join(cache_path, season_str)):
            game_df = import_pbp_data(seasons, cache=True,alt_path=cache_path)
        else:
            cache_pbp(seasons, downcast=True, alt_path = cache_path)
            game_df = import_pbp_data(seasons, cache=True,alt_path=cache_path)
    
    game_df = game_df.loc[game_df['week'] == week]
    game_df = game_df.loc[(game_df['home_team']==team) | (game_df['away_team']==team)]
    game_df.reset_index(inplace=True)
    return game_df

#cache_path = r"C:\Users\nick_\OneDrive\Desktop\Python\Projects\Fantasy\replay_test\cache"
#df = get_football_data(2024, 'CIN', 1, cache_path)

#Works
#df = pd.read_excel(r"C:\Users\nick_\OneDrive\Desktop\Python\Projects\Fantasy\replay_test\game_data.xlsx")
#playcount = len(df)

############################################################################## Intro Loop

def game_intro():
    intro = True
    user_text = ''
    year = ''
    team = ''
    week = ''
    instruct_text = 'Hi'
    color_passive = GREY
    color_active = WHITE
    box_color = color_passive
    active = False
    year_selected = False
    week_selected = False
    team_selected = False
    error = False
    data_error = False
    instruct_text_year = r"Please enter season (like '2021') and press enter"
    instruct_text_team = r"Please enter team (like 'BUF', 'CIN') and press enter"
    instruct_text_week = r"Please enter week (like '7') and press enter"
    loading_text = r"Loading..." + cache_path_g
    error_text = r"Retry? (click)"
    teams_avail_text = r"WAS TEN TB SF SEA PIT PHI NYJ NYG NO NE MIN MIA LV LAC LA KC JAX IND HOU GB DET DEN DAL CLE CIN CHI CAR BUF BAL ATL ARI"
    input_rect = pygame.Rect(WIDTH/2, HEIGHT/2, 10*font_size, 3*font_size)
    error_rect = pygame.Rect(WIDTH/2, HEIGHT/2, 20*font_size, 5*font_size)
    while intro:
        clock.tick(FPS)
        WIN.fill(BLACK)
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                intro = False
                print(year, team, week)

                pygame.quit()
                sys.exit()

            if event.type == pygame.MOUSEBUTTONDOWN:
                if input_rect.collidepoint(event.pos):
                    active = True
                else:
                    active = False
                if error and error_rect.collidepoint(event.pos):
                        user_text = ''
                        year = ''
                        team = ''
                        week = ''
                        instruct_text = 'Hi'
                        color_passive = GREY
                        color_active = WHITE
                        box_color = color_passive
                        active = False
                        year_selected = False
                        week_selected = False
                        team_selected = False
                        error = False
                        data_error = False
            if event.type == pygame.KEYDOWN:
                if active == True:
                    if event.key == pygame.K_BACKSPACE:
                        user_text = user_text[:-1]
                    elif event.key == pygame.K_RETURN:
                        if not year_selected:
                            year += user_text
                            user_text = ''
                            year_selected = True
                        elif not team_selected:
                            team += user_text.upper()
                            user_text = ''
                            team_selected = True
                        else:
                            week += user_text
                            user_text = ''
                            week_selected = True
                    else:
                        user_text += event.unicode
        
        if active:
            box_color = color_active
        else:
            box_color = color_passive
            
        if not year_selected:
            instruct_text = instruct_text_year
        elif not team_selected:
            instruct_text = instruct_text_team
            avail_team_surface =font.render(r" Teams: " + teams_avail_text, True, WHITE)
            WIN.blit(avail_team_surface, (5, HEIGHT - spacer_unit))
        elif not week_selected:
            instruct_text = instruct_text_week
        else:
            #instruct_text = loading_text
            #inst_text_surface = font3.render(instruct_text, True, WHITE)
            #WIN.blit(inst_text_surface, (WIDTH/10, (HEIGHT/2) - spacer_unit))
            try:
                year_selected = int(year)
                if year_selected not in season_options:
                    error = True
                    instruct_text = 'Year not available'
            except ValueError:
                instruct_text = 'Invalid year format'
                error = True
            try:
                week_selected = int(week)
            except ValueError:
                instruct_text = 'Invalid week format'
                error = True
            if team not in team_options:
                error = True
                instruct_text = 'Team not available'
            if not error:
                instruct_text = loading_text
                inst_text_surface = font3.render(instruct_text, True, WHITE)
                WIN.blit(inst_text_surface, (WIDTH/10, (HEIGHT/2) - spacer_unit))
                pygame.display.flip()
                
                local_df = get_football_data(year_selected, team, week_selected, cache_path_g)

                if len(local_df) == 0:
                    error = True
                    data_error = True
                else:
                    intro = False
                    return local_df
                
        if not week_selected:
            pygame.draw.rect(WIN,box_color,input_rect,2)
        if error:
            pygame.draw.rect(WIN,box_color,error_rect)
            error_text_surface = font3.render(error_text, True, GREY)
            WIN.blit(error_text_surface, (error_rect.x + 5, error_rect.y + 5))
            
        user_text_surface = font3.render(user_text, True, WHITE)
        
        if data_error:
            inst_text_surface = font3.render("Data Not Available", True, WHITE)
            WIN.blit(inst_text_surface, (WIDTH/2, (HEIGHT/2) - spacer_unit))
        else:
            inst_text_surface = font3.render(instruct_text, True, WHITE)
            WIN.blit(inst_text_surface, (WIDTH/2, (HEIGHT/2) - spacer_unit))
        
        #WIN.blit(inst_text_surface, (WIDTH/10, (HEIGHT/2) - spacer_unit))
        WIN.blit(user_text_surface, (input_rect.x + 10, input_rect.y + 5))
        
        input_rect.w = max(10*font_size, user_text_surface.get_width() + 10)
        
        pygame.display.flip()



def main():
    yardlines = Yard_Lines(field_size)
    yardlines.get_lines()
    announcer = game_text(text_box.centerx, text_box.centery - spacer_unit, ((text_box.width/2) - spacer_unit_small), (text_box.height/3)+(2*spacer_unit_small))
    run = True
    play = 0
    pygame.display.set_caption("Play by Play")
    playcount = len(df)
    
    rush = False
    reception = False
    td = False
    first_down = False
    sack = False
    penalty = False
    fg = False
    extra_point = False
    two_point_conv = False
    timeout = False

    
    home_team = df['home_team'][0]
    away_team = df['away_team'][0]
    home_coach = df['home_coach'][0]
    away_coach = df['away_coach'][0]
    
    stadium = str(df['stadium'][1])
    weather = str(df['weather'][1])
    roof = str(df['roof'][1])

    while run:
        clock.tick(FPS)
        
        ###################################################################### Get User Input
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                run = False
            
            if event.type == pygame.KEYDOWN:
                if event.key == pygame.K_RIGHT:
                    if play < playcount - 1:
                        play += 1
                    if math.isnan(df['yardline_100'][play]):
                        continue
                    else:
                        chains.centerx = field_x + (yard*spacer_unit_small) +yard*(df['yardline_100'][play] - df['ydstogo'][play])
                        pos_yard_line.centerx = field_x + (yard*spacer_unit_small) +yard*(df['yardline_100'][play])
                if event.key == pygame.K_LEFT:
                    if play > 0:
                        play -=1
                    if math.isnan(df['yardline_100'][play]):
                        continue
                    else:
                        chains.centerx = field_x + (yard*spacer_unit_small) + yard*(df['yardline_100'][play] - df['ydstogo'][play])
                        pos_yard_line.centerx = field_x + (yard*spacer_unit_small) +yard*(df['yardline_100'][play])
        
        ###################################################################### Get play variables *
        posteam = str(df['posteam'][play])
        defteam = str(df['defteam'][play])
        play_type = str(df['play_type_nfl'][play])
        yardline_100 = str(df['yardline_100'][play])
        down = str(df['down'][play])
        ydstogo = str(df['ydstogo'][play])
        yards_gained = str(df['yards_gained'][play])
        home_score = str(df['total_home_score'][play])
        away_score = str(df['total_away_score'][play])
        touchdown = df['touchdown'][play]
        f_down = df['first_down'][play]
        play_result = df['yards_gained'][play]
        td_chance = df['td_prob'][play]
        qb_scramble = df['qb_scramble'][play]
        home_to = str(df['home_timeouts_remaining'][play])
        away_to = str(df['away_timeouts_remaining'][play])
        announcer_text = str(df['desc'][play])
        game_time = str(df['time'][play])
        quarter = str(df['qtr'][play])
        o_form = str(df['offense_formation'][play])
        o_pers = str(df['offense_personnel'][play])
        d_pers = str(df['defense_personnel'][play])
        d_box = str(df['defenders_in_box'][play])
        
        pass_rushers = str(df['number_of_pass_rushers'][play])
        time_tt = str(df['time_to_throw'][play])
        was_press = df['was_pressure'][play]
        route = str(df['route'][play])
        man_zone = str(df['defense_man_zone_type'][play])
        cover = str(df['defense_coverage_type'][play])
        ###################################################################### Define Play *
        if play_type == 'RUSH':
            run_location = str(df['run_location'][play])
            run_gap = str(df['run_gap'][play])
            rusher = str(df['rusher'][play])
            rush = True
        else:
            rush = False
        if play_type == 'PASS':
            receiver = str(df['receiver'][play])
            air_yards = str(df['air_yards'][play])
            yacatch = str(df['yards_after_catch'][play])
            
            pass_loc = df['pass_location'][play]
            passer = df['passer_player_name'][play]
            
            qb_hit = df['qb_hit'][play]
            pass_complete = df['complete_pass'][play]
            
            reception = True
        else:
            reception = False
        if play_type == 'FIELD_GOAL':
            fg_result = str(df['field_goal_result'][play])
            kicker = str(df['kicker_player_name'][play])
            kick_dist = str(df['kick_distance'][play])
            fg_chance = format(df['fg_prob'][play],".2%")
            fg = True
        else:
            fg = False
        if play_type == 'SACK':
            sacker = str(df['sack_player_name'][play])
            sack = True
        else:
            sack = False
        if play_type == 'PENALTY':
            pen_type = str(df['penalty_type'][play])
            pen_team = str(df['penalty_team'][play])
            pen_player = str(df['penalty_player_name'][play])
            ped_yds = str(df['penalty_yards'][play])
            penalty = True
        else:
            penalty = False
        if touchdown > 0:
            td = True
        else:
            td = False
        if f_down > 0:
            first_down = True
        else:
            first_down = False
        if play_type == 'XP_KICK':
            extra_point = True
            extra_point_res = str(df['extra_point_result'][play])
            kicker = str(df['kicker_player_name'][play])
        else:
            extra_point = False
            
        if play_type == 'PAT2':
            two_point_conv = True
            two_point_conv_res = str(df['two_point_conv_result'][play])
        else:
            two_point_conv = False

        if play_type == 'TIMEOUT':
            timeout_team = str(df['timeout_team'][play])
            timeout = True
        else:
            timeout = False
        
        ###################################################################### Draw field and indicators and text boxes
        pygame.draw.rect(WIN, GREEN, field_size)
        pygame.draw.rect(WIN, GREY, end_z_1)
        pygame.draw.rect(WIN, GREY, end_z_2)
        for i in yardlines.lines:
            pygame.draw.rect(WIN, yardlines.color, i)
            
        pygame.draw.rect(WIN, ORANGE, chains)
        pygame.draw.rect(WIN, BLUE, pos_yard_line)
        
        if play_result > 0:
            result_line = (pos_yard_line.centerx - (play_result*yard), field_y + field_height - yard, (play_result*yard), yard)
            pygame.draw.rect(WIN, DARK_GREEN, result_line)
        elif play_result < 0:
            result_line = (pos_yard_line.centerx, field_y + field_height - yard, abs(play_result*yard), yard)
            pygame.draw.rect(WIN, DARK_RED, result_line)
        else:
            pass

                
        pygame.draw.rect(WIN, WHITE, text_box)
        pygame.draw.rect(WIN, WHITE, env_box)
        pygame.draw.rect(WIN, GREY, coach_box)
        announcer.write(announcer_text)
        
        ###################################################################### Define text static
        text1 = font.render("Offense: " + posteam, 1, GREY)
        text2 = font.render("Defense: " + defteam, 1, GREY)
        text3 = font.render("Play Type: " + play_type, 1, GREY)
        text4 = font.render("Yardline: " + yardline_100, 1, GREY)
        text5 = font.render("Down: " + down, 1, GREY)
        text6 = font.render("Yards to Go: " + ydstogo, 1, GREY)
        text7 = font.render("Yards gained: " + yards_gained, 1, GREY)
        text8 = font.render(home_team + " Score: " + home_score, 1, BLACK)
        text9 = font.render(away_team + " Score: " + away_score, 1, BLACK)
        text10 = font.render("End Zone", 1, BLACK)
        text11 = font2.render("Chains", 1, ORANGE)
        pos_team_text = font2.render(posteam, 1, BLUE)
        weather_text = font.render("Weather: " + weather, 1, BLACK)
        stadium_text = font.render("Stadium: " + stadium, 1, BLACK)
        roof_text = font.render(roof, 1, BLACK)
        
        home_team_text = font.render(home_team, 1, BLACK)
        away_team_text = font.render(away_team, 1, BLACK)
        home_coach_text = font.render(f"{home_team} Coach: " + home_coach, 1, BLACK)
        away_coach_text = font.render(f"{away_team} Coach: " + away_coach, 1, BLACK)
        home_to_text = font.render("Timeouts Remaining: " + home_to, 1, BLACK)
        away_to_text = font.render("Timeouts Remaining: " + away_to, 1, BLACK)
        
        ###################################################################### Define and blit Text Dynamic
        if td:
            pygame.draw.rect(WIN, YELLOW, end_z_1)
            text_td = font2.render("TOUCHDOWN", 1, DARK_GREEN)
            WIN.blit(text_td, (text_box.centerx + (text_box.centerx - text_box.left)/2, text_box.bottom - spacer_unit))
        if first_down and not td:
            pygame.draw.rect(WIN, YELLOW, chains)
            text_fd = font2.render("First Down", 1, DARK_GREEN)
            WIN.blit(text_fd, (text_box.centerx + (text_box.centerx - text_box.left)/2, text_box.bottom - spacer_unit))
        if rush:
            text_run_location = font2.render("Run Location: " + run_location, 1, GREY)
            text_run_gap = font2.render("Run Gap: " + run_gap, 1, GREY)
            text_rusher = font2.render("Rusher: " + rusher, 1, GREY)
            WIN.blit(text_rusher, (text_box.left + spacer_unit_small, text_box.bottom - spacer_unit))
            WIN.blit(text_run_location, (text_box.centerx, text_box.bottom - spacer_unit))
            WIN.blit(text_run_gap, (text_box.centerx - (text_box.centerx - text_box.left)/2, text_box.bottom - spacer_unit))
        if reception:
            text_receiver = font2.render("Receiver: " + receiver, 1, GREY)
            text_air_yards = font2.render("Air Yards: " + air_yards, 1, GREY)
            text_yacatch = font2.render("YAC: " + yacatch, 1, GREY)
            text_pass_loc = font.render("Pass Location: " + pass_loc, 1, GREY)
            text_passer = font2.render("Passer: " + passer, 1, GREY)
            text_pass_rushers = font.render("Pass Rushers: " + pass_rushers, 1, RED)
            text_time_tt = font.render("Time to throw: " + time_tt + " sec", 1, PURPLE)
            text_route = font.render("Route: " + route, 1, PURPLE)
            text_man_zone = font.render(man_zone, 1, RED)
            text_cover = font.render(cover, 1, RED)
            
            WIN.blit(text_man_zone, (pos_yard_line.left - (2*spacer_unit) + spacer_unit_small, pos_yard_line.centery + 5*spacer_unit_small))
            WIN.blit(text_cover, (pos_yard_line.left - (2*spacer_unit) + spacer_unit_small, pos_yard_line.centery + 7*spacer_unit_small))
            WIN.blit(text_time_tt, (pos_yard_line.right + (2*spacer_unit_small), pos_yard_line.centery + 5*spacer_unit_small))
            WIN.blit(text_pass_rushers, (pos_yard_line.left - (2*spacer_unit) + spacer_unit_small, pos_yard_line.centery + 3*spacer_unit_small))
            WIN.blit(text_receiver, (text_box.left + spacer_unit_small, text_box.bottom - spacer_unit))
            WIN.blit(text_passer, (text_box.centerx - 3*(text_box.centerx - text_box.left)/4, text_box.bottom - spacer_unit))
            WIN.blit(text_yacatch, (text_box.centerx - (text_box.centerx - text_box.left)/4, text_box.bottom - spacer_unit))
            WIN.blit(text_air_yards, (text_box.centerx - (text_box.centerx - text_box.left)/2, text_box.bottom - spacer_unit))
            WIN.blit(text_pass_loc, (text_box.centerx, text_box.bottom - spacer_unit))
            
            if pass_complete > 0:
                text_pass_completion = font2.render("Pass Completed", 1, DARK_GREEN)
            else:
                text_pass_completion = font2.render("Pass Incomplete", 1, DARK_RED)
            WIN.blit(text_pass_completion, (text_box.centerx + (text_box.centerx - text_box.left)/4, text_box.bottom - spacer_unit))
            if qb_hit > 0:
                text_qb_hit = font2.render("QB Hit", 1, DARK_RED)
                WIN.blit(text_qb_hit, (text_box.left + spacer_unit_small, text_box.bottom - (spacer_unit/2)))
            if was_press > 0:
                text_was_press = font.render("QB Pressured", 1, PURPLE)
            else:
                text_was_press = font.render("No QB Pressure", 1, PURPLE)
            WIN.blit(text_was_press, (pos_yard_line.right + (2*spacer_unit_small), pos_yard_line.centery + 3*spacer_unit_small))

        if reception:
            if pass_loc == 'left':
                pass_loc_ind = (pos_yard_line.centerx - int((yard*df['air_yards'][play])) - int(spacer_unit/2), pos_yard_line.centery + (pos_yard_line.height/3))

            if pass_loc == 'middle':
                pass_loc_ind = (pos_yard_line.centerx - int((yard*df['air_yards'][play])) - int(spacer_unit/2), pos_yard_line.centery - int(spacer_unit/2))

            if pass_loc == 'right':
                pass_loc_ind = (pos_yard_line.centerx - int((yard*df['air_yards'][play])) - int(spacer_unit/2), pos_yard_line.centery - (pos_yard_line.height/3))
            WIN.blit(text_route, (pass_loc_ind[0], pass_loc_ind[1] - (2*spacer_unit_small)))
            if pass_complete == 0:
                WIN.blit(x_pic, pass_loc_ind)
            
            else:
                WIN.blit(check_pic, pass_loc_ind)

        if sack:
            text_sacker = font2.render("Sacked by: " + sacker, 1, DARK_RED)
            WIN.blit(text_sacker, (text_box.centerx + (text_box.centerx - text_box.left)/2, text_box.bottom - spacer_unit))  
        if penalty:
            text_pen = font2.render(f"{pen_type} against {pen_team} on {pen_player} for {ped_yds} yards", 1, DARK_RED)
            WIN.blit(text_pen, (text_box.centerx - (text_box.centerx - text_box.left)/2, text_box.bottom - spacer_unit))
        if fg:
            text_fg = font2.render("Field Goal " + fg_result, 1, GREY)
            text_kicker = font2.render("Kicker: " + kicker, 1, GREY)
            text_kick_dist = font2.render("Distance: " + kick_dist, 1, GREY)
            text_fg_chance = font.render("Field Goal Probability: " + fg_chance, 1, GREY)
            WIN.blit(text_fg, (text_box.centerx - (text_box.centerx - text_box.left)/2, text_box.bottom - spacer_unit))
            WIN.blit(text_kicker, (text_box.centerx - (text_box.centerx - text_box.left)/4, text_box.bottom - spacer_unit))
            WIN.blit(text_kick_dist, (text_box.centerx, text_box.bottom - spacer_unit))
            WIN.blit(text_fg_chance, (text_box.centerx + (text_box.centerx - text_box.left)/4, text_box.bottom - spacer_unit))
        if extra_point:
            text_extra_point_res = font2.render("Kick is " + extra_point_res, 1, GREY)
            text_kicker = font2.render("Kicker: " + kicker, 1, GREY)
            WIN.blit(text_extra_point_res, (text_box.centerx - (text_box.centerx - text_box.left)/2, text_box.bottom - spacer_unit))
            WIN.blit(text_kicker, (text_box.centerx, text_box.bottom - spacer_unit))
        if two_point_conv:
            text_two_point = font2.render("Result: " + two_point_conv_res, 1, GREY)
            WIN.blit(text_two_point, (text_box.centerx, text_box.bottom - spacer_unit))
        if td_chance > 0 and not penalty:
            td_prob = format(td_chance, ".2%")
            text_td_prob = font.render("Touchdown Probability: " + td_prob, 1, GREY)
            WIN.blit(text_td_prob, (text_box.centerx - text_box.width/4, text_box.top + spacer_unit_small))
        if reception or rush:
            text_o_form = font.render(o_form, 1, PURPLE)
            text_o_pers = font.render(o_pers, 1, PURPLE)
            text_d_pers = font.render(d_pers, 1, RED)
            text_d_box = font.render(d_box + " in box", 1, RED)
            WIN.blit(text_o_form, (pos_yard_line.right + (2*spacer_unit_small), pos_yard_line.centery - spacer_unit_small))
            WIN.blit(text_o_pers, (pos_yard_line.right + (2*spacer_unit_small), pos_yard_line.centery + spacer_unit_small))
            WIN.blit(text_d_box, (pos_yard_line.left - (2*spacer_unit) + spacer_unit_small, pos_yard_line.centery - spacer_unit_small))
            WIN.blit(text_d_pers, (pos_yard_line.left - (2*spacer_unit) + spacer_unit_small, pos_yard_line.centery + spacer_unit_small))
        if qb_scramble > 0:
            text_qb_scramble = font.render("QB Scramble", 1, BLUE)
            WIN.blit(text_qb_scramble, (pos_yard_line.right + (7*spacer_unit_small), pos_yard_line.centery + spacer_unit))
        if timeout:
            timeout_team_text = font2.render("Timeout Called by " + timeout_team, 1, GREY)
            WIN.blit(timeout_team_text, (text_box.left + spacer_unit_small, text_box.bottom - spacer_unit))
        
        announcer.draw(WIN)
        time_text = font3.render("Game Clock: " + game_time, 1, BLACK)
        quarter_text = font3.render("QTR: " + quarter, 1, BLACK)
        WIN.blit(quarter_text, (text_box.centerx, text_box.top + spacer_unit_small))
        WIN.blit(time_text, (text_box.centerx + int(text_box.width/6), text_box.top + spacer_unit_small))
        
        ###################################################################### Blit Text Static
        
        WIN.blit(text1, (text_box.left + spacer_unit_small, text_box.top + spacer_unit_small))
        WIN.blit(text2, (text_box.left + spacer_unit_small, text_box.top + (3*spacer_unit_small)))
        WIN.blit(text3, (text_box.left + spacer_unit_small, text_box.top + spacer_unit))
        WIN.blit(text4, (text_box.left + spacer_unit_small, text_box.top + (7*spacer_unit_small)))
        WIN.blit(text5, (text_box.left + spacer_unit_small, text_box.top + (9*spacer_unit_small)))
        WIN.blit(text6, (text_box.left + spacer_unit_small, text_box.top + (11*spacer_unit_small)))
        WIN.blit(text7, (text_box.left + spacer_unit_small, text_box.top + (13*spacer_unit_small)))
        WIN.blit(text8, (text_box.right - (12*spacer_unit_small), text_box.top + spacer_unit_small))
        WIN.blit(text9, (text_box.right - (12*spacer_unit_small), text_box.top + (3*spacer_unit_small)))
        WIN.blit(text10, (end_z_1.left + (end_z_1.width/4), end_z_1.top + spacer_unit))
        WIN.blit(weather_text, (env_box.left + spacer_unit_small, env_box.top + (1.7*spacer_unit_small)))
        WIN.blit(stadium_text, (env_box.left + spacer_unit_small, env_box.top + (0.3*spacer_unit_small)))
        WIN.blit(roof_text, (env_box.centerx + int(env_box.width/4), env_box.top + (0.3*spacer_unit_small)))
        WIN.blit(home_team_text, (coach_box.left + spacer_unit_small, coach_box.top + spacer_unit_small))
        WIN.blit(away_team_text, (coach_box.centerx + spacer_unit_small, coach_box.top + spacer_unit_small))
        WIN.blit(home_coach_text, (coach_box.left + spacer_unit_small, coach_box.centery))
        WIN.blit(away_coach_text, (coach_box.centerx + spacer_unit_small, coach_box.centery))
        WIN.blit(home_to_text, (coach_box.left + (coach_box.width/4), coach_box.centery))
        WIN.blit(away_to_text, (coach_box.centerx + (coach_box.width/4), coach_box.centery))
        ###################################################################### Blit Field Text
        WIN.blit(pos_team_text, (pos_yard_line.right + (7*spacer_unit_small), pos_yard_line.centery - (10*spacer_unit_small)))
        WIN.blit(text11, (chains.left - (7*spacer_unit_small), chains.centery - (10*spacer_unit_small)))

        
        ###################################################################### Update Display
        pygame.display.flip()
        
    pygame.quit()


if __name__ == "__main__":
    df = game_intro()
    main()
