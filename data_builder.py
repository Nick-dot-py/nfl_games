# -*- coding: utf-8 -*-
"""
Created on Wed Jan 29 23:38:03 2025

@author: nick_
"""
from tkinter import *
from tkinter import ttk
import pickle
import pandas as pd
import view_df
import data_aggregation

# windows = serves as a container to hold or contain these widgets
# widgets = GUI elements: buttons, textboxes, labels, images
# label = an area widget that holds text and/or an image within a window
# radio button = can only select 1 from a group
# frame = rectangular container to group and hold widgets
# listbox = A listing of selectable text items within it's own container

window = Tk() #instantiate an instance of a window

# Global Variables
screen_width = window.winfo_screenwidth()
screen_height = window.winfo_screenheight()
WIDTH, HEIGHT = round(screen_width/1.6), round(screen_height/1.6)
spacer_unit = round(WIDTH/100)
spacer_unit_small = round(spacer_unit/6)

selection_complete = False

x = IntVar()
y = IntVar()
z = IntVar()
dist_wk_select = ""
dist_szn_select = ""

dist_qb_select = ""
dist_wr_select = ""
dist_rb_select = ""
dist_te_select = ""

qb_check = IntVar()
wr_check = IntVar()
rb_check = IntVar()
te_check = IntVar()

#with open("player_id.pkl", "rb") as f:
#    player_id = pickle.load(f)

with open("player_pos.pkl", "rb") as f:
    player_pos = pickle.load(f)
    
with open("pos_player.pkl", "rb") as f:
    pos_player = pickle.load(f)

player_names = list(player_pos.keys())

qbs = pos_player['QB']
rbs = pos_player['RB']
wrs = pos_player['WR']
tes = pos_player['TE']

qbs.sort()
rbs.sort()
wrs.sort()
tes.sort()

seasons = list(range(2016,2025))
weeks = list(range(1,21))
by_week_options = []
by_season_options = []

# Global Variables to determine output

data_granularity = ""
data_slct = []              #select data options
dist_week_selections = []   #if dist_weeks_mode
dist_season_selections = [] #if dist_szn_mode
start_week = ""
end_week = ""
start_szn = ""
end_szn = ""

dist_weeks_mode = False     #how to slice data
dist_szn_mode = False
start_end_wk_mode = False
start_end_szn_mode = False

what_data_std = False       #standard data
what_data_slct = False      #select data

all_qbs = False             #select players
all_wrs = False
all_rbs = False
all_tes = False

dist_player_selections = []

# 180 selections
for i in seasons:
    for j in weeks:
        by_week_options.append(str(i)+" Season, Week "+str(j))
    by_season_options.append(str(i)+" Season")


# Define Window Dimensions
window.geometry(f"{WIDTH}x{HEIGHT}")
window.title("Data Builder")

# Create Icon
icon = PhotoImage(file=r"pics\icon.png")
window.iconphoto(True,icon)

# Define Window Color
window.config(background="#d0d0d2")

# Functions
def click():
    error_txt = "Please Select "
    if not any([all_qbs, all_wrs, all_rbs, all_tes]) and len(dist_player_selections) == 0:
        error_txt += "Player(s)"
        warn_label.config(text=error_txt)
    elif not what_data_std and len(data_slct) == 0:
        error_txt += "Standard or Select Data"
        warn_label.config(text=error_txt)
    elif data_granularity not in ["Week", "Season", "Cumulative"]:
        error_txt += "Data Granularity"
        warn_label.config(text=error_txt)
    elif data_granularity in ["Week", "Season", "Cumulative"] and not any([dist_weeks_mode, dist_szn_mode, start_end_wk_mode, start_end_szn_mode]):
        error_txt = "Please Choose Timeframe"
        warn_label.config(text=error_txt)
    elif dist_weeks_mode and len(dist_week_selections) == 0:
        error_txt = "Please Add Weeks"
        warn_label.config(text=error_txt)
    elif dist_szn_mode and len(dist_season_selections) == 0:
        error_txt = "Please Add Seasons"
        warn_label.config(text=error_txt)
    elif start_end_wk_mode and len(start_week) == 0:
        error_txt = "Please Select Start Week"
        warn_label.config(text=error_txt)
    elif start_end_wk_mode and len(end_week) == 0:
        error_txt = "Please Select End Week"
        warn_label.config(text=error_txt)
    elif start_end_szn_mode and len(start_szn) == 0:
        error_txt = "Please Select Start Season"
        warn_label.config(text=error_txt)
    elif start_end_szn_mode and len(end_szn) == 0:
        error_txt = "Please Select End Season"
        warn_label.config(text=error_txt)
    else:
        warn_label.config(text="")
        
        selected_player_states = (all_qbs, all_wrs, all_rbs, all_tes)
        selected_players = data_aggregation.get_players(selected_player_states,dist_player_selections) #player list
        
        data_definition = data_aggregation.def_data_requested(what_data_std, what_data_slct, data_slct) #data requested
        
        if dist_szn_mode or start_end_szn_mode: # timeframe
            season_list = data_aggregation.get_seasons(dist_szn_mode, start_end_szn_mode, start_szn, end_szn, dist_season_selections)
            df = data_aggregation.generate_df(selected_players, data_definition, data_granularity, season_list)
        elif start_end_wk_mode or dist_weeks_mode:
            season_week_dict = data_aggregation.get_weeks(dist_weeks_mode, start_end_wk_mode, start_week, end_week, dist_week_selections)
            df = data_aggregation.generate_df(selected_players, data_definition, data_granularity, season_week_dict)
        print(df)
        #view_df.display_data(WIDTH,HEIGHT,test_df)

def what_data():
    global what_data_std
    global what_data_slct
    if(x.get()==1):
        what_data_std = True
        what_data_slct = False
        perf_std_label1.config(fg='WHITE')
        perf_std_label2.config(text="Selected:",fg='#a2a2a5')
        radbutton_perf_gran_week.config(state=NORMAL)
        radbutton_perf_gran_szn.config(state=NORMAL)
        radbutton_perf_gran_cum.config(state=NORMAL)
        slct_dataButton.config(state=DISABLED)
    elif(x.get()==2):
        perf_std_label1.config(fg='#a2a2a5')
        what_data_std = False
        what_data_slct = True
        global data_slct
        data_slct = []
        slct_data_listbox.config(state=NORMAL)
        radbutton_perf_gran_week.config(state=DISABLED)
        radbutton_perf_gran_szn.config(state=DISABLED)
        radbutton_perf_gran_cum.config(state=DISABLED)
        slct_dataButton.config(state=ACTIVE)


def submitSlct():
    global data_slct

    display_string = "Selected:"
    for index in slct_data_listbox.curselection():
        data_slct.insert(index,slct_data_listbox.get(index))
        string_add = str(slct_data_listbox.get(index))
        display_string += f"\n {string_add}"
    perf_std_label2.config(text=display_string, fg="WHITE")
    slct_data_listbox.config(state=DISABLED)
    radbutton_perf_gran_week.config(state=NORMAL)
    radbutton_perf_gran_szn.config(state=NORMAL)
    radbutton_perf_gran_cum.config(state=NORMAL)

def data_gran():
    global data_granularity
    if(y.get()==1):
        data_granularity = "Week"
        radbutton_perf_dist_weeks.config(state=NORMAL)
        radbutton_perf_dist_szns.config(state=DISABLED)
        radbutton_perf_startend_week.config(state=NORMAL)
        radbutton_perf_startend_szns.config(state=DISABLED)
    elif(y.get()==2):
        data_granularity = "Season"
        radbutton_perf_dist_weeks.config(state=DISABLED)
        radbutton_perf_dist_szns.config(state=NORMAL)
        radbutton_perf_startend_week.config(state=DISABLED)
        radbutton_perf_startend_szns.config(state=NORMAL)
    elif(y.get()==3):
        data_granularity = "Cumulative"
        radbutton_perf_dist_weeks.config(state=NORMAL)
        radbutton_perf_dist_szns.config(state=NORMAL)
        radbutton_perf_startend_week.config(state=NORMAL)
        radbutton_perf_startend_szns.config(state=NORMAL)

def timeframeSlct():
    global dist_weeks_mode
    global dist_szn_mode
    global start_end_wk_mode
    global start_end_szn_mode
    if(z.get()==1):
        dist_weeks_mode = True
        dist_szn_mode = False
        start_end_wk_mode = False
        start_end_szn_mode = False
        drop_week_dist.config(state=NORMAL)
        drop_szn_dist.config(state=DISABLED)
        drop_week_start.config(state=DISABLED)
        drop_szn_start.config(state=DISABLED)
        drop_szn_end.config(state=DISABLED)
        drop_week_end.config(state=DISABLED)
        add_dist_sznButton.config(state=DISABLED)
        rem_dist_sznButton.config(state=DISABLED)
        drop_szn_end.config(value=[" "],state=DISABLED)
        drop_szn_end.current(0)
    elif(z.get()==2):
        dist_szn_mode = True
        dist_weeks_mode = False
        start_end_wk_mode = False
        start_end_szn_mode = False
        drop_week_dist.config(state=DISABLED)
        drop_szn_dist.config(state=NORMAL)
        drop_week_start.config(state=DISABLED)
        drop_szn_start.config(state=DISABLED)
        drop_szn_end.config(state=DISABLED)
        drop_week_end.config(state=DISABLED)
        add_dist_weekButton.config(state=DISABLED)
        rem_dist_weekButton.config(state=DISABLED)
        drop_week_end.config(value=[" "],state=DISABLED)
        drop_week_end.current(0)
    elif(z.get()==3):
        start_end_wk_mode = True
        dist_weeks_mode = False
        dist_szn_mode = False
        start_end_szn_mode = False
        drop_week_dist.config(state=DISABLED)
        drop_szn_dist.config(state=DISABLED)
        drop_week_start.config(state=NORMAL)
        drop_szn_start.config(state=DISABLED)
        drop_szn_end.config(value=[" "],state=DISABLED)
        drop_szn_end.current(0)
        dist_label.config(fg="#a2a2a5")
    elif(z.get()==4):
        start_end_szn_mode = True
        dist_weeks_mode = False
        dist_szn_mode = False
        start_end_wk_mode = False
        drop_week_dist.config(state=DISABLED)
        drop_szn_dist.config(state=DISABLED)
        drop_week_start.config(state=DISABLED)
        drop_week_end.config(value=[" "],state=DISABLED)
        drop_week_end.current(0)
        drop_szn_start.config(state=NORMAL)
        dist_label.config(fg="#a2a2a5")

def add_week():
    global dist_week_selections
    global dist_wk_select
    if dist_wk_select not in dist_week_selections:
        dist_week_selections.append(dist_wk_select)
    rem_dist_weekButton.config(state=ACTIVE)
    display_string = "Selected: "
    for i in dist_week_selections:
        if dist_week_selections.index(i) == len(dist_week_selections) - 1:
            display_string += str(i)
        else:
            display_string += str(i)+" + "
    dist_label.config(text=display_string, fg="WHITE")

def add_season():
    global dist_season_selections
    global dist_szn_select
    if dist_szn_select not in dist_season_selections:
        dist_season_selections.append(dist_szn_select)
    rem_dist_sznButton.config(state=ACTIVE)
    display_string = "Selected: "
    for i in dist_season_selections:
        if dist_season_selections.index(i) == len(dist_season_selections) - 1:
            display_string += str(i)
        else:
            display_string += str(i)+" + "
    dist_label.config(text=display_string, fg="WHITE")

def rem_week():
    global dist_week_selections
    global dist_wk_select
    if dist_wk_select in dist_week_selections:
        dist_week_selections.pop(dist_week_selections.index(dist_wk_select))
    display_string = "Selected: "
    for i in dist_week_selections:
        if dist_week_selections.index(i) == len(dist_week_selections) - 1:
            display_string += str(i)
        else:
            display_string += str(i)+" + "
    dist_label.config(text=display_string, fg="WHITE")


def rem_season():
    global dist_season_selections
    global dist_szn_select
    if dist_szn_select in dist_season_selections:
        dist_season_selections.pop(dist_season_selections.index(dist_szn_select))
    display_string = "Selected: "
    for i in dist_season_selections:
        if dist_season_selections.index(i) == len(dist_season_selections) - 1:
            display_string += str(i)
        else:
            display_string += str(i)+" + "
    dist_label.config(text=display_string, fg="WHITE")


def pick_week(e):
    global dist_wk_select
    dist_wk_select = drop_week_dist.get()
    add_dist_weekButton.config(state=ACTIVE)


def pick_season(e):
    global dist_szn_select
    dist_szn_select = drop_szn_dist.get()
    add_dist_sznButton.config(state=ACTIVE)


def pick_start_week(e):
    global start_week
    s_week = drop_week_start.get()
    drop_week_end.config(value=by_week_options[by_week_options.index(s_week)+1:],state=NORMAL)
    start_week = s_week

def pick_end_week(e):
    global end_week
    end_week = drop_week_end.get()


def pick_start_season(e):
    global start_szn
    s_szn = drop_szn_start.get()
    drop_szn_end.config(value=by_season_options[by_season_options.index(s_szn)+1:],state=NORMAL)
    start_szn = s_szn

def pick_end_season(e):
    global end_szn
    end_szn = drop_szn_end.get()

def qb_select_all():
    global all_qbs
    if(qb_check.get()==1):
        all_qbs = True
        drop_qb_dist.config(state=DISABLED)
    else:
        all_qbs = False
        drop_qb_dist.config(state=NORMAL)

def wr_select_all():
    global all_wrs
    if(wr_check.get()==1):
        all_wrs = True
        drop_wr_dist.config(state=DISABLED)
    else:
        all_wrs = False
        drop_wr_dist.config(state=NORMAL)

def rb_select_all():
    global all_rbs
    if(rb_check.get()==1):
        all_rbs = True
        drop_rb_dist.config(state=DISABLED)
    else:
        all_rbs = False
        drop_rb_dist.config(state=NORMAL)
    
def te_select_all():
    global all_tes
    if(te_check.get()==1):
        all_tes = True
        drop_te_dist.config(state=DISABLED)
    else:
        all_tes = False
        drop_te_dist.config(state=NORMAL)

def pick_qb(e):
    global dist_qb_select
    dist_qb_select = drop_qb_dist.get()
    add_dist_qbButton.config(state=NORMAL)
    
def pick_wr(e):
    global dist_wr_select
    dist_wr_select = drop_wr_dist.get()
    add_dist_wrButton.config(state=NORMAL)

def pick_rb(e):
    global dist_rb_select
    dist_rb_select = drop_rb_dist.get()
    add_dist_rbButton.config(state=NORMAL)

def pick_te(e):
    global dist_te_select
    dist_te_select = drop_te_dist.get()
    add_dist_teButton.config(state=NORMAL)

def add_qb():
    global dist_qb_select
    global dist_player_selections
    if dist_qb_select not in dist_player_selections:
        dist_player_selections.append(dist_qb_select)
    rem_dist_qbButton.config(state=ACTIVE)
    display_string = "Selected: "
    for i in dist_player_selections:
        if dist_player_selections.index(i) == len(dist_player_selections) - 1:
            display_string += str(i)
        else:
            display_string += str(i)+" + "
    dist_player_label.config(text=display_string, fg="WHITE")
    
def rem_qb():
    global dist_qb_select
    global dist_player_selections
    if dist_qb_select in dist_player_selections:
        dist_player_selections.pop(dist_player_selections.index(dist_qb_select))
    display_string = "Selected: "
    for i in dist_player_selections:
        if dist_player_selections.index(i) == len(dist_player_selections) - 1:
            display_string += str(i)
        else:
            display_string += str(i)+" + "
    dist_player_label.config(text=display_string, fg="WHITE")
    
def add_wr():
    global dist_wr_select
    global dist_player_selections
    if dist_wr_select not in dist_player_selections:
        dist_player_selections.append(dist_wr_select)
    rem_dist_wrButton.config(state=ACTIVE)
    display_string = "Selected: "
    for i in dist_player_selections:
        if dist_player_selections.index(i) == len(dist_player_selections) - 1:
            display_string += str(i)
        else:
            display_string += str(i)+" + "
    dist_player_label.config(text=display_string, fg="WHITE")
    
def rem_wr():
    global dist_wr_select
    global dist_player_selections
    if dist_wr_select in dist_player_selections:
        dist_player_selections.pop(dist_player_selections.index(dist_wr_select))
    display_string = "Selected: "
    for i in dist_player_selections:
        if dist_player_selections.index(i) == len(dist_player_selections) - 1:
            display_string += str(i)
        else:
            display_string += str(i)+" + "
    dist_player_label.config(text=display_string, fg="WHITE")

def add_rb():
    global dist_rb_select
    global dist_player_selections
    if dist_rb_select not in dist_player_selections:
        dist_player_selections.append(dist_rb_select)
    rem_dist_rbButton.config(state=ACTIVE)
    display_string = "Selected: "
    for i in dist_player_selections:
        if dist_player_selections.index(i) == len(dist_player_selections) - 1:
            display_string += str(i)
        else:
            display_string += str(i)+" + "
    dist_player_label.config(text=display_string, fg="WHITE")
    
def rem_rb():
    global dist_rb_select
    global dist_player_selections
    if dist_rb_select in dist_player_selections:
        dist_player_selections.pop(dist_player_selections.index(dist_rb_select))
    display_string = "Selected: "
    for i in dist_player_selections:
        if dist_player_selections.index(i) == len(dist_player_selections) - 1:
            display_string += str(i)
        else:
            display_string += str(i)+" + "
    dist_player_label.config(text=display_string, fg="WHITE")

def add_te():
    global dist_te_select
    global dist_player_selections
    if dist_te_select not in dist_player_selections:
        dist_player_selections.append(dist_te_select)
    rem_dist_teButton.config(state=ACTIVE)
    display_string = "Selected: "
    for i in dist_player_selections:
        if dist_player_selections.index(i) == len(dist_player_selections) - 1:
            display_string += str(i)
        else:
            display_string += str(i)+" + "
    dist_player_label.config(text=display_string, fg="WHITE")
    
def rem_te():
    global dist_te_select
    global dist_player_selections
    if dist_te_select in dist_player_selections:
        dist_player_selections.pop(dist_player_selections.index(dist_te_select))
    display_string = "Selected: "
    for i in dist_player_selections:
        if dist_player_selections.index(i) == len(dist_player_selections) - 1:
            display_string += str(i)
        else:
            display_string += str(i)+" + "
    dist_player_label.config(text=display_string, fg="WHITE")
    
    
# Window Tabs

notebook = ttk.Notebook(window)         #widget that manages a colection of windows/displays
notebook.pack(padx=spacer_unit,pady=spacer_unit,expand=True,fill="both")  #expand to fill any space not otherwise used
                                        #fill = fill space on x and y axis
                                        
perftab = Frame(notebook, bg="#16161d")                  #new frame for tab 1
statictab = Frame(notebook)                  #new frame for tab 1
contracttab = Frame(notebook) 
injurytab = Frame(notebook) 
rostertab = Frame(notebook) 


statictab.pack(fill="both", expand=True)
contracttab.pack(fill="both", expand=True)
injurytab.pack(fill="both", expand=True)
rostertab.pack(fill="both", expand=True)

notebook.add(perftab,text="Performance Data")
notebook.add(statictab,text="Static Data")
notebook.add(contracttab,text="Contract Data")
notebook.add(injurytab,text="Injury Data")
notebook.add(rostertab,text="Roster Data")

# Sub frames
perf_sub1 = Frame(perftab, borderwidth=1, relief="groove", bg="#16161d")
perf_sub1.place(relx=0,rely=1/4,relwidth=1,relheight=1/4)
Label(perf_sub1, text="Define Data Points", bg="#a2a2a5").pack(side="left")

perf_sub2 = Frame(perftab, borderwidth=1, relief="groove", bg="#45454a")
perf_sub2.place(relx=0,rely=1/2,relwidth=1,relheight=1/4)
Label(perf_sub2, text="Granularity", bg="#a2a2a5").pack(side="left")

perf_sub3 = Frame(perftab, borderwidth=1, relief="groove", bg="#16161d")
perf_sub3.place(relx=0,rely=3/4,relwidth=1,relheight=1/4)
Label(perf_sub3, text="Time Frame", bg="#a2a2a5").pack(side="left")

perf_sub4 = Frame(perftab, borderwidth=1, relief="groove", bg="#45454a")
perf_sub4.place(relx=0,rely=0,relwidth=1,relheight=1/4)
Label(perf_sub4, text="Players", bg="#a2a2a5").pack(side="left")

# Labels
perf_std_label1 = Label(perf_sub1,
              text="Passing | Rushing | Receiving | Misc.",
              font=('Arial',spacer_unit, 'bold'),
              fg='WHITE',
              bg='#45454a',
              bd=spacer_unit_small,
              padx=spacer_unit_small,
              pady=spacer_unit_small)

perf_std_label1.place(relx=1/5,rely=1/4)

perf_std_label2 = Label(perf_sub1,
              text="Selected:",
              font=('Arial',spacer_unit, 'bold'),
              fg='WHITE',
              bg='#45454a',
              bd=spacer_unit_small,
              padx=spacer_unit_small,
              pady=spacer_unit_small)

perf_std_label2.place(relx=1/2,rely=1/4)

dist_label = Label(perf_sub3,
              text="Selected:",
              font=('Arial',round(2*spacer_unit/3), 'bold'),
              fg='#a2a2a5',
              bg='#45454a',
              bd=spacer_unit_small,
              padx=spacer_unit_small,
              pady=spacer_unit_small)

dist_label.place(relx=1/12,rely=0)

warn_label = Label(perf_sub3,
              text="",
              font=('Arial',round(2*spacer_unit/3), 'bold'),
              fg='#a2a2a5',
              bg='#16161d',
              bd=spacer_unit_small,
              padx=spacer_unit_small,
              pady=spacer_unit_small)

warn_label.place(relx=0,rely=3/4)

players_label1 = Label(perf_sub4,
              text="QBs",
              font=('Arial',spacer_unit, 'bold'),
              fg='WHITE',
              bg='#45454a',
              bd=spacer_unit_small,
              padx=spacer_unit_small,
              pady=spacer_unit_small)

players_label2 = Label(perf_sub4,
              text="WRs",
              font=('Arial',spacer_unit, 'bold'),
              fg='WHITE',
              bg='#45454a',
              bd=spacer_unit_small,
              padx=spacer_unit_small,
              pady=spacer_unit_small)

players_label3 = Label(perf_sub4,
              text="RBs",
              font=('Arial',spacer_unit, 'bold'),
              fg='WHITE',
              bg='#45454a',
              bd=spacer_unit_small,
              padx=spacer_unit_small,
              pady=spacer_unit_small)

players_label4 = Label(perf_sub4,
              text="TEs",
              font=('Arial',spacer_unit, 'bold'),
              fg='WHITE',
              bg='#45454a',
              bd=spacer_unit_small,
              padx=spacer_unit_small,
              pady=spacer_unit_small)

players_label1.place(relx=1/5,rely=1/10)
players_label2.place(relx=2/5,rely=1/10)
players_label3.place(relx=3/5,rely=1/10)
players_label4.place(relx=4/5,rely=1/10)

dist_player_label = Label(perf_sub4,
              text="Selected:",
              font=('Arial',round(2*spacer_unit/3), 'bold'),
              fg='#a2a2a5',
              bg='#16161d',
              bd=spacer_unit_small,
              padx=spacer_unit_small,
              pady=spacer_unit_small)

dist_player_label.place(relx=1/12,rely=0)


# Buttons
get_data_button = Button(window, text='Get Data')

get_data_button.config(command=click) # performs call back of function
get_data_button.config(font=('Arial',spacer_unit,'bold'))
get_data_button.config(bg='#737377')
get_data_button.config(fg='WHITE')


#get_data_button.config(state=DISABLED) # disables button (ACTIVE/DISABLED)

get_data_button.place(x=spacer_unit,y=(HEIGHT - 3*spacer_unit))

slct_dataButton = Button(perf_sub1, text="Submit", command=submitSlct)
slct_dataButton.place(relx=2/3,rely=5/6)
slct_dataButton.config(state=DISABLED)

add_dist_weekButton = Button(perf_sub3, text="Add...", command=add_week)
add_dist_weekButton.place(relx=1/6,rely=3/8)
add_dist_weekButton.config(state=DISABLED)

rem_dist_weekButton = Button(perf_sub3, text="Remove", command=rem_week)
rem_dist_weekButton.place(relx=3/12,rely=3/8)
rem_dist_weekButton.config(state=DISABLED)

add_dist_sznButton = Button(perf_sub3, text="Add...", command=add_season)
add_dist_sznButton.place(relx=1/2,rely=3/8)
add_dist_sznButton.config(state=DISABLED)

rem_dist_sznButton = Button(perf_sub3, text="Remove", command=rem_season)
rem_dist_sznButton.place(relx=7/12,rely=3/8)
rem_dist_sznButton.config(state=DISABLED)

add_dist_qbButton = Button(perf_sub4, text="Add...", command=add_qb)
add_dist_qbButton.place(relx=1/5,rely=4/5)
add_dist_qbButton.config(state=DISABLED)

rem_dist_qbButton = Button(perf_sub4, text="Remove", command=rem_qb)
rem_dist_qbButton.place(relx=3/10,rely=4/5)
rem_dist_qbButton.config(state=DISABLED)

add_dist_wrButton = Button(perf_sub4, text="Add...", command=add_wr)
add_dist_wrButton.place(relx=2/5,rely=4/5)
add_dist_wrButton.config(state=DISABLED)

rem_dist_wrButton = Button(perf_sub4, text="Remove", command=rem_wr)
rem_dist_wrButton.place(relx=1/2,rely=4/5)
rem_dist_wrButton.config(state=DISABLED)

add_dist_rbButton = Button(perf_sub4, text="Add...", command=add_rb)
add_dist_rbButton.place(relx=3/5,rely=4/5)
add_dist_rbButton.config(state=DISABLED)

rem_dist_rbButton = Button(perf_sub4, text="Remove", command=rem_rb)
rem_dist_rbButton.place(relx=7/10,rely=4/5)
rem_dist_rbButton.config(state=DISABLED)

add_dist_teButton = Button(perf_sub4, text="Add...", command=add_te)
add_dist_teButton.place(relx=4/5,rely=4/5)
add_dist_teButton.config(state=DISABLED)

rem_dist_teButton = Button(perf_sub4, text="Remove", command=rem_te)
rem_dist_teButton.place(relx=9/10,rely=4/5)
rem_dist_teButton.config(state=DISABLED)

# Radio Buttons Section 1
radbutton_perf_data_standard = Radiobutton(perf_sub1,
                                           text="Standard", #adds text to radiobuttons
                                           variable=x, #groups radiobuttons together if they share the same variable
                                           value=1,     #assigns each radiobutton a different value
                                           padx = spacer_unit_small,
                                           pady = spacer_unit_small,
                                           bg="#737377",
                                           command=what_data) 
radbutton_perf_data_select = Radiobutton(perf_sub1,
                                         text="Select",
                                         variable=x,
                                         value=2,
                                         padx = spacer_unit_small,
                                         pady = spacer_unit_small,
                                         bg="#737377",
                                         command=what_data)

radbutton_perf_data_standard.place(relx=1/5,rely=0)
radbutton_perf_data_select.place(relx=1/2,rely=0)

# Radio Buttons Section 2
radbutton_perf_gran_week = Radiobutton(perf_sub2,
                                           text="By Week", #adds text to radiobuttons
                                           variable=y, #groups radiobuttons together if they share the same variable
                                           value=1,     #assigns each radiobutton a different value
                                           padx = spacer_unit_small,
                                           pady = spacer_unit_small,
                                           bg="#737377",
                                           command=data_gran) 
radbutton_perf_gran_szn = Radiobutton(perf_sub2,
                                         text="By Season",
                                         variable=y,
                                         value=2,
                                         padx = spacer_unit_small,
                                         pady = spacer_unit_small,
                                         bg="#737377",
                                         command=data_gran)
radbutton_perf_gran_cum = Radiobutton(perf_sub2,
                                         text="Cumulative",
                                         variable=y,
                                         value=3,
                                         padx = spacer_unit_small,
                                         pady = spacer_unit_small,
                                         bg="#737377",
                                         command=data_gran)
radbutton_perf_gran_week.place(relx=1/4,rely=1/2)
radbutton_perf_gran_szn.place(relx=1/2,rely=1/2)
radbutton_perf_gran_cum.place(relx=3/4,rely=1/2)

radbutton_perf_gran_week.config(state=DISABLED)
radbutton_perf_gran_szn.config(state=DISABLED)
radbutton_perf_gran_cum.config(state=DISABLED)

# Radio Buttons Section 3
radbutton_perf_dist_weeks = Radiobutton(perf_sub3,
                                           text="Distinct Weeks", #adds text to radiobuttons
                                           variable=z, #groups radiobuttons together if they share the same variable
                                           value=1,     #assigns each radiobutton a different value
                                           padx = spacer_unit_small,
                                           pady = spacer_unit_small,
                                           bg="#737377",
                                           command=timeframeSlct) 
radbutton_perf_dist_szns = Radiobutton(perf_sub3,
                                           text="Distinct Seasons", #adds text to radiobuttons
                                           variable=z, #groups radiobuttons together if they share the same variable
                                           value=2,     #assigns each radiobutton a different value
                                           padx = spacer_unit_small,
                                           pady = spacer_unit_small,
                                           bg="#737377",
                                           command=timeframeSlct)
radbutton_perf_startend_week = Radiobutton(perf_sub3,
                                           text="Start Week End Week", #adds text to radiobuttons
                                           variable=z, #groups radiobuttons together if they share the same variable
                                           value=3,     #assigns each radiobutton a different value
                                           padx = spacer_unit_small,
                                           pady = spacer_unit_small,
                                           bg="#737377",
                                           command=timeframeSlct)
radbutton_perf_startend_szns = Radiobutton(perf_sub3,
                                           text="Start Season End Season", #adds text to radiobuttons
                                           variable=z, #groups radiobuttons together if they share the same variable
                                           value=4,     #assigns each radiobutton a different value
                                           padx = spacer_unit_small,
                                           pady = spacer_unit_small,
                                           bg="#737377",
                                           command=timeframeSlct)

radbutton_perf_dist_weeks.place(relx=1/3,rely=1/4)
radbutton_perf_dist_szns.place(relx=2/3,rely=1/4)
radbutton_perf_startend_week.place(relx=1/3,rely=3/4)
radbutton_perf_startend_szns.place(relx=2/3,rely=3/4)

radbutton_perf_dist_weeks.config(state=DISABLED)
radbutton_perf_dist_szns.config(state=DISABLED)
radbutton_perf_startend_week.config(state=DISABLED)
radbutton_perf_startend_szns.config(state=DISABLED)

# Checkboxes

checkbox_qb = Checkbutton(perf_sub4,text='All',variable=qb_check,onvalue=1,offvalue=0,command=qb_select_all)
checkbox_wr = Checkbutton(perf_sub4,text='All',variable=wr_check,onvalue=1,offvalue=0,command=wr_select_all)
checkbox_rb = Checkbutton(perf_sub4,text='All',variable=rb_check,onvalue=1,offvalue=0,command=rb_select_all)
checkbox_te = Checkbutton(perf_sub4,text='All',variable=te_check,onvalue=1,offvalue=0,command=te_select_all)

checkbox_qb.place(relx=1/5,rely=3/10)
checkbox_wr.place(relx=2/5,rely=3/10)
checkbox_rb.place(relx=3/5,rely=3/10)
checkbox_te.place(relx=4/5,rely=3/10)

# listboxes
slct_data_listbox = Listbox(perf_sub1,
                            bg="#45454a",
                            font=('Arial',spacer_unit,'bold'),
                            selectmode=MULTIPLE)
slct_data_listbox.place(relx=2/3,rely=0)

slct_data_listbox.insert(1,"Passing")
slct_data_listbox.insert(2,"Rushing")
slct_data_listbox.insert(3,"Receiving")
slct_data_listbox.insert(4,"Misc.")

slct_data_listbox.config(height=slct_data_listbox.size())

slct_data_listbox.config(state=DISABLED)


# Dropboxes
drop_week_dist = ttk.Combobox(perf_sub3, value=by_week_options)
drop_szn_dist = ttk.Combobox(perf_sub3, value=by_season_options)
drop_week_start = ttk.Combobox(perf_sub3, value=by_week_options)
drop_week_end = ttk.Combobox(perf_sub3, value=[" "])
drop_szn_start = ttk.Combobox(perf_sub3, value=by_season_options)
drop_szn_end = ttk.Combobox(perf_sub3, value=[" "])

drop_qb_dist = ttk.Combobox(perf_sub4, value = qbs)
drop_wr_dist = ttk.Combobox(perf_sub4, value = wrs)
drop_rb_dist = ttk.Combobox(perf_sub4, value = rbs)
drop_te_dist = ttk.Combobox(perf_sub4, value = tes)

drop_week_dist.place(relx=1/6,rely=1/4)
drop_szn_dist.place(relx=1/2,rely=1/4)
drop_week_start.place(relx=1/6,rely=5/8)
drop_week_end.place(relx=1/6,rely=7/8)
drop_szn_start.place(relx=1/2,rely=5/8)
drop_szn_end.place(relx=1/2,rely=7/8)

drop_qb_dist.place(relx=1/5,rely=3/5)
drop_wr_dist.place(relx=2/5,rely=3/5)
drop_rb_dist.place(relx=3/5,rely=3/5)
drop_te_dist.place(relx=4/5,rely=3/5)


drop_week_dist.bind("<<ComboboxSelected>>", pick_week)
drop_szn_dist.bind("<<ComboboxSelected>>", pick_season)
drop_week_start.bind("<<ComboboxSelected>>", pick_start_week)
drop_week_end.bind("<<ComboboxSelected>>", pick_end_week)
drop_szn_start.bind("<<ComboboxSelected>>", pick_start_season)
drop_szn_end.bind("<<ComboboxSelected>>", pick_end_season)

drop_qb_dist.bind("<<ComboboxSelected>>", pick_qb)
drop_wr_dist.bind("<<ComboboxSelected>>", pick_wr)
drop_rb_dist.bind("<<ComboboxSelected>>", pick_rb)
drop_te_dist.bind("<<ComboboxSelected>>", pick_te)

drop_week_dist.config(state=DISABLED)
drop_szn_dist.config(state=DISABLED)
drop_week_start.config(state=DISABLED)
drop_szn_start.config(state=DISABLED)

drop_week_end.config(state=DISABLED)
drop_szn_end.config(state=DISABLED)

what_data()

window.mainloop() # place window on computer screen, listen for events
