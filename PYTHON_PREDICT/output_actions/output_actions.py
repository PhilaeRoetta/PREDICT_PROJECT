'''
    The purpose of this module is to generate message personalized on forums topics.
    This module:
    - choose which message to post (gameday calculated or inited)
    - present some utility functions to run
'''
import logging
logging.basicConfig(level=logging.INFO)
import os
import unicodedata
import re
import pandas as pd
import matplotlib.pyplot as plt

import config
import file_actions as fileA
from output_actions import output_actions_sql_queries as sqlQ
from output_actions import output_actions_calculated as outputAC
from output_actions import output_actions_inited as outputAI
from snowflake_actions import snowflake_execute

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('param_to_translate','country')})
def translate_param_for_country(param_to_translate: str | pd.DataFrame, country: str, translations: dict) -> str | pd.DataFrame:

    '''
        Translates a text or dataframe headers for a given country
        Inputs:
            param_to_translate (str or df): the parameter to translate
            country (str): The country for which we translate
            translations (dict): translations strings to translate
        Returns:
            The parameter translated (str or df)
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    if isinstance(param_to_translate, pd.DataFrame):
        param_translated = param_to_translate.rename(columns=translations[country])

    elif isinstance(param_to_translate, str):
        param_translated = param_to_translate
        for key, val in translations[country].items():
            param_translated = param_translated.replace(key, val)

    return param_translated

@config.exit_program(log_filter=lambda args: {})
def format_message(message: str) -> str:

    '''
        Formats a message by:
        - removing original newlines from it
        - replacing each |N| with N*newlines
        Inputs:
            message (str): the message
        Returns:
            The modified string with newlines
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    # we remove original newlines
    message = re.sub(r'\n', '', message)

    #subfunction to transform |N| into N* newlines
    def create_newlines(match):
        n = int(match.group(1))
        return '\n' * n
    
    message = re.sub(r'\|(\d+)\|', create_newlines, message)
    return message

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('begin_tag','end_tag','condition')})
def replace_conditionally_message(output_text: str, begin_tag: str, end_tag: str, condition: bool) -> str:

    '''
        Replaces the output template text conditionnally
        If it answers the condition we just remove tags keeping the enclosed block.
        Otherwise we delete all the block with tags
        Inputs:
            output_text (str): the output message text template
            begin_tag (str): the tag beginning the block potentially removed
            end_tag (str): the tag ending the block potentially removed
            condition (boolean): the condition to decide if we remove the block or not
        Returns:
            The output_text modified
        Raises:
            Exits the program if error running the function (using decorator)
    '''
    if condition:
        output_text = output_text.replace(begin_tag, "")
        output_text = output_text.replace(end_tag, "")
    else:
        pattern = f"{re.escape(begin_tag)}.*?{re.escape(end_tag)}"
        output_text = re.sub(pattern, '', output_text, flags=re.DOTALL)
    return output_text

@config.exit_program(log_filter=lambda args: dict(args))
def define_filename(input_type: str, sr_gameday_output_init: pd.Series, extension: str, country: str | None = None):

    '''
        Creates the name of the output file
        Inputs:
            input_type (str): the type of file (forumoutput_inited, forumoutput_calculated, capture_calculated,...)
            sr_gameday_output_init (series - one row) containing elements for the name
            country (str): the country will be suffixed to the name if provided
            extension (str): file extension (txt or jpg)
        Returns:
            The name of the file
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    #subfunction to normalize file name by removing accents, trimming, and lower-case it
    def normalize_string(s):
        # Remove accents
        s = ''.join(
            c for c in unicodedata.normalize('NFD', s)
            if unicodedata.category(c) != 'Mn'
        )
        # Convert to lower-case
        s = s.lower()
        # Remove spaces
        s = s.replace(' ', '')
        return s

    #we calculate the file_name then normalize it
    file_name = input_type + '_' + sr_gameday_output_init['SEASON_ID'] + '_' + sr_gameday_output_init['GAMEDAY'] 
    if country is not None:
        file_name += '_' + country
    file_name += '.' + extension
    file_name = normalize_string(file_name)

    return file_name

@config.exit_program(log_filter=lambda args: {'columns_df': args['df'].columns.tolist(), 'columns': args['columns'] })
def display_rank(df: pd.DataFrame, rank_column: str) -> pd.DataFrame:

    '''
        Sorts a dataframe by its rank - when same rank , replaced with '-' for UI
        Inputs:
            df (dataframe) the dataframe for which we add the rank
            rank_column (str) column used for display the rank
        Returns:
            The dataframe sorted by its rank, on first column
        Raises:
            Exits the program if error running the function (using decorator)
    '''
    df = df.copy()
    df = df.sort_values(by=rank_column, ascending=True)
    df[rank_column] = df[rank_column].astype(int).mask(df.duplicated(subset=rank_column), '-')

    # Move rank to the first position
    col = df.pop(rank_column)    
    df.insert(0, rank_column, col)
    
    return df

@config.exit_program(log_filter=lambda args: {'columns_df': args['df'].columns.tolist(), 'columns': args['columns'] })
def calculate_and_display_rank(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:

    '''
        The purpose of this function is to:
        - add a rank to a dataframe based on given list of columns, sorted descending
        - sort it by this rank - when same rank , replaced with '-' for UI
        Inputs:
            df (dataframe) the dataframe for which we add the rank
            columns (list) list of columns used for calculate the rank
        Returns:
            The dataframe sorted by its rank, on first column
        Raises:
            Exits the program if error running the function (using decorator)
    '''
    df = df.copy()
        
    # Sort and rank
    df = df.sort_values(by=columns, ascending=[False] * len(columns))
    df['RANK'] = df[columns].apply(tuple, axis=1).rank(method='min', ascending=False)
    df = display_rank(df,'RANK')

    return df

@config.exit_program(log_filter=lambda args: {'columns_df': args['df'].columns.tolist(), 'capture_name': args['capture_name'] })
def capture_df_oneheader(df: pd.DataFrame, capture_name: str):

    '''
        Captures a styled jpg using matplotlib from a dataframe with one header level
        Inputs:
            df (dataframe): the dataframe we capture
            capture_name (str): the name of the capture
        Style of the figure:
            applies alternating row colors for readability.
            highlights  headers in bold.
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    # color for rows switching background color
    color1 = '#ccd9ff'
    color2 = '#ffffcc'

    fig, ax = plt.subplots(figsize=(5, 0.25*len(df)))
    ax.axis('off')  # Turn off the axes
    table = ax.table(cellText=df.values, colLabels=df.columns, loc='center', cellLoc='center')
    
    # Style the table
    for (row, _), cell in table.get_celld().items():
        cell.set_height(0.07)
        if row == 0:  # Header row
            cell.set_text_props(weight='bold')  # Bold text for column headers
        elif row > 0:  # Data rows (skip the header row)
            # Alternate row colors for readability
            if row % 2 == 0:  # Even row index (data rows)
                cell.set_facecolor(color1) 
            else:  # Odd row index (data rows)
                cell.set_facecolor(color2) 
    
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.auto_set_column_width(range(len(df.columns)))

    # we create the jpg from the figure
    fileA.create_jpg(os.path.join(config.TMPF,capture_name),fig)

@config.exit_program(log_filter=lambda args: {})
def generate_output_message(context_dict: dict):

    '''
        Generates and posts messages on forums.It:
        - checks if an inited or calculated message should be posted 
        - calls the process for generating and post the message
        Input:
            context_dict (dict): The data dictionary to decide what needs to be posted
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    # we personalize the query and get the output need gameday
    sr_output_need = context_dict['sr_output_need']
    qGamedayOutput = sqlQ.qGamedayOutput
    df_gameday_output = snowflake_execute(context_dict['sr_snowflake_account_connect'],qGamedayOutput,(sr_output_need['SEASON_ID'],sr_output_need['GAMEDAY']))
    
    has_to_post_inited_message = False
    has_to_post_calculated_message = False
    
    #We choose what needs to be posted based on TASK_RUN
    if len(df_gameday_output) > 0 and sr_output_need['TASK_RUN']  == config.TASK_RUN_MAP["INIT"]:
        has_to_post_inited_message = True
    
    if len(df_gameday_output) > 0 and sr_output_need['TASK_RUN']  == config.TASK_RUN_MAP["CALCULATE"]:
        has_to_post_calculated_message = True

    if len(df_gameday_output) > 0:
        #if the process deleted calculation we post also calculated message as it might change rankings
        if sr_output_need['IS_TO_DELETE'] == 1:
            has_to_post_calculated_message = True

    if has_to_post_inited_message:
        outputAI.process_output_message_inited(context_dict,df_gameday_output.iloc[0])

    if has_to_post_calculated_message:
       outputAC.process_output_message_calculated(context_dict,df_gameday_output.iloc[0])
