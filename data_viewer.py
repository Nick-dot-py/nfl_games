# -*- coding: utf-8 -*-
"""
Created on Wed Jan 29 23:38:03 2025

@author: nick_
"""
import pandas as pd
from tkinter import Tk, ttk, StringVar


file_path = r"C:\Users\nick_\OneDrive\Desktop\Python\Projects\Fantasy\Data_Builder\Player_Static.xlsx"

df = pd.read_excel(file_path)

#Create the main window
root = Tk()
root.title("Dataframe Viewer")

# Create a StringVar to hold the current cell value
cell_value = StringVar()

# Create a Treeview widget to display the DataFrame
tree = ttk.Treeview(root, columns=list(df.columns), show="headings")

# Define Headers
for col in df.columns:
    tree.heading(col, text=col)

# Insert data into the Treeview
for index, row in df.iterrows():
    tree.insert("", "end", values=list(row))


# Function to handle cell selection
def on_select(event):
    item = tree.selection()[0]
    row = tree.item(item)['values']
    col = tree.identify_column(event.x)[1:] # Get Column Name
    cell_value.set(row[int(col) - 1]) # Set entry box value

    
# Bind Events
tree.bind("<Button-1>", on_select)

# Arrange Widgets
tree.pack(side="left", fill="both", expand=True)
#entry.pack(side="right", fill="x")

# Run the application
root.mainloop()
