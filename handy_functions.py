# -*- coding: utf-8 -*-
"""
Created on Fri Feb  7 21:57:07 2025

@author: nick_
"""

import pickle

def calc_passer_rating(completions, attempts, passing_yards, passing_tds, interceptions):
    a = ((completions/attempts)-0.3)*5
    b = ((passing_yards/attempts)-3)*0.25
    c = (passing_tds/attempts)*20
    d = 2.375 - ((interceptions/attempts)*25)
    pass_rating = ((a+b+c+d)/6)*100
    return pass_rating

def lookup_with_partial_keys(my_dict, partial_key):
    """
    Looks up values in a dictionary with tuple keys using a partial key.

    Args:
        my_dict (dict): The dictionary with tuple keys.
        partial_key: The partial key to search for.

    Returns:
        list: A list of values matching the partial key, or an empty list if no match is found.
    """
    results = []
    for key, value in my_dict.items():
        if isinstance(key, tuple) and is_sublist(key, partial_key):

            results.append(value)
    return results

def lookup_with_partial_key(my_dict, partial_key):
    """
    Looks up values in a dictionary with tuple keys using a partial key.

    Args:
        my_dict (dict): The dictionary with tuple keys.
        partial_key: The partial key to search for.

    Returns:
        list: A list of values matching the partial key, or an empty list if no match is found.
    """
    results = []
    for key, value in my_dict.items():
        if isinstance(key, tuple) and partial_key in key:

            results.append(value)
    return results

def get_keys_with_partial_key(my_dict, partial_key):
    """
    Looks up values in a dictionary with tuple keys using a partial key.

    Args:
        my_dict (dict): The dictionary with tuple keys.
        partial_key: The partial key to search for.

    Returns:
        list: A list of values matching the partial key, or an empty list if no match is found.
    """
    results = []
    for key, value in my_dict.items():
        if isinstance(key, tuple) and partial_key in key:

            results.append(key)
    return results

def make_list(paste=str):
    words = paste.split('\t')
    print(words)

def is_sublist(main_list, sublist):
    """
    Checks if a sublist exists within a main list.

    Args:
        main_list: The list to search within.
        sublist: The sublist to search for.

    Returns:
        True if the sublist is found, False otherwise.
    """
    for i in range(len(main_list) - len(sublist) + 1):
        if main_list[i:i + len(sublist)] == sublist:
            return True
    return False



with open("qb_pass_rating_dict.pkl", "rb") as f:
    player_qbr_dic = pickle.load(f)
    
with open("team_qb_pass_rating_dict.pkl", "rb") as f:
    team_qbr_dic = pickle.load(f)
