# -*- coding: utf-8 -*-
"""
Created on Fri Jun 30 14:39:05 2023

@author: Nima
"""

import pandas as pd

#df_subset = pd.DataFrame()

def is_info_completed(col):
    '''
    A new binary column will replace the selected column
    If the selected column is not null AND is not only whitespace,
    The value of the new column in that case will be True = 1
    '''
    df_subset[col] = ((df_subset[col].notnull()) & (df_subset[col].str.strip() != '')).astype(int)

def info_somewhere_else(col, group_by) :
    '''
    Assessing if a missing value in a specific column 
    is not populated in another line (for a host id)
    
    Returning a dataframe with host_id existing with and without {col}
    '''
    #Finding all the host_id with no col (ex: host_response_time)
    null_host_id = df_subset[df_subset[col].isnull()][group_by].unique()
    #Subsetting df_subset to those host_id
    host_id_with_one_null = df_subset[df_subset[group_by].isin(null_host_id)][[group_by, col]].drop_duplicates()
    #keeping host_id that exists with and without col (ex: host_response_time)
    filtering_host_id = host_id_with_one_null[host_id_with_one_null[col].notnull()]
    
    print(f'there is/are {filtering_host_id.shape[0]} {group_by} that exists with a {col} and without')
    return filtering_host_id

def proxy_mean(column_to_improve,group_by,round_val=0):
    '''
    By group_by category (ex: property type), this function replaces the Nan in a selected column (column to improve)
    by its average
    
    round_val = number of decimal in final output (bedroom is .0 and bathroom is .1 for example)
    cti= column_to_improve
    '''
    
    # Property type with column_to_improve is nan
    pt_nan_cti=df_subset[df_subset[column_to_improve].isnull()]
    pt_nan_cti=pt_nan_cti[group_by].unique()
             
    #filling empty cells with the mean value of for each property type:
    id_with_nan = df_subset[df_subset[column_to_improve].isnull()].index

    for idx in id_with_nan:
        property_type = df_subset.loc[idx, group_by]
    
        for pt in pt_nan_cti:
            if property_type == pt:
                mean_cti = round(df_subset[df_subset[group_by] == pt][column_to_improve].mean(),round_val)

                df_subset.loc[idx, column_to_improve] = mean_cti
 
def plot_all_r2():
    '''
    Generate a plot of correlation lines between predicted values and the actual values (y_test)
    '''

    length = len(prediction_dictionaries)
    n_col = 2
    if length < 2:
        n_col = length % 2

    nrow = 1
    if length > 2:
        nrow = int(length / 2)
        if length % 2 != 0:
            nrow += 1

    fig, axes = plt.subplots(nrow, n_col, figsize=(16, 3 * length))
    for ax, key in zip(axes.flatten(), prediction_dictionaries.keys()):
        sns.regplot(x=prediction_dictionaries[key], y=y_test, ax=ax)
        ax.set_title("The correlation line in {}".format(key))
    plt.show()
