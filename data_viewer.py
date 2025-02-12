# -*- coding: utf-8 -*-
"""
Created on Wed Jan 29 23:38:03 2025

@author: nick_
"""
import pandas as pd
from tkinter import Tk, ttk, StringVar


#Create the main window
def display_data(WIDTH, HEIGHT, df):
    new_window = Tk()
    new_window.title("Dataframe Viewer")
    
    screen_width = new_window.winfo_screenwidth()
    screen_height = new_window.winfo_screenheight()
    WIDTH, HEIGHT = round(screen_width/1.6), round(screen_height/1.6)
    
    new_window.geometry(f"{WIDTH}x{HEIGHT}")
    
    # Create a StringVar to hold the current cell value
    cell_value = StringVar()
    
    # Create Scrollbar
    treeScroll_vert = ttk.Scrollbar(new_window)
    treeScroll_vert.pack(side="right", fill="y")
    
    treeScroll_hor = ttk.Scrollbar(new_window, orient='horizontal')
    treeScroll_hor.pack(side="bottom", fill="x")
    
    # Create a Treeview widget to display the DataFrame
    tree = ttk.Treeview(new_window, columns=list(df.columns), show="headings",
                        yscrollcommand=treeScroll_vert.set, xscrollcommand=treeScroll_hor.set)
    
    # Define Headers
    for col in df.columns:
        tree.heading(col, text=col)
    
    # Insert data into the Treeview
    for index, row in df.iterrows():
        tree.insert("", "end", values=list(row))
    
    
    # Function to handle cell selection
    def on_select(event):
        print(tree.selection())
        item = tree.selection()[0]
        row = tree.item(item)['values']
        col = tree.identify_column(event.x)[1:] # Get Column Name
        cell_value.set(row[int(col) - 1]) # Set entry box value
    
        
    # Bind Events
    tree.bind("<Button-1>", on_select)
    tree.selection_set("I001")
    # Arrange Widgets
    tree.pack(side="left", fill="both", expand=True)

    treeScroll_vert.config(command=tree.yview)
    treeScroll_hor.config(command=tree.xview)


# Run the application
#new_window.mainloop()
