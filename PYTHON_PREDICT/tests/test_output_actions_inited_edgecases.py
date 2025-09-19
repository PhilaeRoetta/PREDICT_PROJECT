'''
This tests file concern all functions in the output_actions_inited module.
It units test unexpected paths
'''
import unittest
from unittest.mock import patch
import pandas as pd
from datetime import datetime as dt
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import output_actions_inited
from testutils import assertExit

def test_get_inited_list_games_empty_df():
    # this test the function get_inited_list_games with snowflake returning empty dataframe. Must return an empty string.
    with patch("output_actions_inited.snowflake_execute", return_value=pd.DataFrame(columns=["GAME_MESSAGE","GAME_MESSAGE_SHORT","TEAM_HOME_NAME","TEAM_AWAY_NAME", "SCORE_HOME","SCORE_AWAY", "RESULT"])) as mock_exec:
        
        sr_inited_gameday = pd.Series({
            'GAMEDAY': 'GAMEDAY1',
            'IS_CALCULATED':1,
            'BEGIN_DATE_LOCAL': dt.strptime('2025-02-01', '%Y-%m-%d'),
            'BEGIN_TIME_LOCAL': '20:15:00',
            'GAMEDAY_MESSAGE': 'GD1',
            'SEASON_DIVISION': 'SD1',
            'SEASON_ID': 'S1',
            'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP': 1,
            'BEGIN_DATE_WEEKDAY': 'Monday'
        })
        
        sr_snowflake_account= pd.Series({
            'ACCOUNT': 'my_account',
            'WAREHOUSE': 'my_wh',
            'DATABASE_PROD': 'my_dbprod',
            'DATABASE_TEST': 'my_dbtest'
        })


        result = output_actions_inited.get_inited_list_games(sr_snowflake_account, sr_inited_gameday)
        assert result == ("", 0)

@patch('output_actions_inited.snowflake_execute')
def test_get_inited_list_games_special_chars(mock_execute):
    # this test the function get_inited_list_games with special characters in dataframe. Must be accepted
    df_games = pd.DataFrame([
    {"GAME_MESSAGE": '3EJ.01', "GAME_MESSAGE_SHORT": 1, "TEAM_HOME_NAME": 'M@tch!',
     "TEAM_AWAY_NAME": "team2", "SCORE_HOME": 0, "SCORE_AWAY": 0, "RESULT": 0},
    
    {"GAME_MESSAGE": '&TeamB', "GAME_MESSAGE_SHORT": 2, "TEAM_HOME_NAME": '<TeamA>',
     "TEAM_AWAY_NAME": "<TeamA>", "SCORE_HOME": 0, "SCORE_AWAY": 0, "RESULT": 0}
    ])

    sr_inited_gameday = pd.Series({
        'GAMEDAY': 'GAMEDAY1',
        'IS_CALCULATED':1,
        'BEGIN_DATE_LOCAL': dt.strptime('2025-02-01', '%Y-%m-%d'),
        'BEGIN_TIME_LOCAL': '20:15:00',
        'GAMEDAY_MESSAGE': 'GD1',
        'SEASON_DIVISION': 'SD1',
        'SEASON_ID': 'S1',
        'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP': 1,
        'BEGIN_DATE_WEEKDAY': 'Monday'
    })
    
    sr_snowflake_account= pd.Series({
        'ACCOUNT': 'my_account',
        'WAREHOUSE': 'my_wh',
        'DATABASE_PROD': 'my_dbprod',
        'DATABASE_TEST': 'my_dbtest'
    })
    mock_execute.return_value = df_games

    list_games_expected = f"""#3EJ.01# M@tch! vs team2 ==> [i]+1[/i]
#&TeamB# <TeamA> vs <TeamA> ==> [i]+1[/i]"""
    
    list_games, nb_games = output_actions_inited.get_inited_list_games(sr_snowflake_account, sr_inited_gameday)
    assert list_games == list_games_expected
    assert nb_games == 2

def test_get_inited_list_games_missing_key():
    # this test the function get_inited_list_games with missing SEASON_ID and missing GAMEDAY. Must exit program

    sr_inited_gameday = pd.Series({
        'IS_CALCULATED':1,
        'BEGIN_DATE_LOCAL': dt.strptime('2025-02-01', '%Y-%m-%d'),
        'BEGIN_TIME_LOCAL': '20:15:00',
        'GAMEDAY_MESSAGE': 'GD1',
        'SEASON_DIVISION': 'SD1',
        'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP': 1,
        'BEGIN_DATE_WEEKDAY': 'Monday'
    })
    
    sr_snowflake_account= pd.Series({
        'ACCOUNT': 'my_account',
        'WAREHOUSE': 'my_wh',
        'DATABASE_PROD': 'my_dbprod',
        'DATABASE_TEST': 'my_dbtest'
    })
    
    assertExit(lambda: output_actions_inited.get_inited_list_games(sr_snowflake_account, sr_inited_gameday))

@patch('output_actions_inited.snowflake_execute')
def test_get_inited_remaining_games_duplicate_gamedays(mock_execute):
    # this test the function get_inited_remaining_games with duplicates gamedays. Must accept it
    df_remaining = pd.DataFrame([
        {"GAMEDAY": '3eme journee', "GAME_MESSAGE": '3EJ.01', "TEAM_HOME_NAME": 'team1',"TEAM_AWAY_NAME": "team2"},
        {"GAMEDAY": '3eme journee', "GAME_MESSAGE": '3EJ.02', "TEAM_HOME_NAME": 'team2',"TEAM_AWAY_NAME": "team3"},
        {"GAMEDAY": '4eme journee', "GAME_MESSAGE": '4EJ.02', "TEAM_HOME_NAME": 'team3',"TEAM_AWAY_NAME": "team4"}
    ])

    sr_inited_gameday = pd.Series({
        'GAMEDAY': 'GAMEDAY1',
        'IS_CALCULATED':1,
        'BEGIN_DATE_LOCAL': dt.strptime('2025-02-01', '%Y-%m-%d'),
        'BEGIN_TIME_LOCAL': '20:15:00',
        'GAMEDAY_MESSAGE': 'GD1',
        'SEASON_DIVISION': 'SD1',
        'SEASON_ID': 'S1',
        'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP': 1,
        'BEGIN_DATE_WEEKDAY': 'Monday'
    })
    
    sr_snowflake_account= pd.Series({
        'ACCOUNT': 'my_account',
        'WAREHOUSE': 'my_wh',
        'DATABASE_PROD': 'my_dbprod',
        'DATABASE_TEST': 'my_dbtest'
    })

    mock_execute.return_value = df_remaining

    rem_games_exp = f"""#3EJ.01# team1 vs team2 ==> [i]+1[/i]
#3EJ.02# team2 vs team3 ==> [i]+1[/i]
#4EJ.02# team3 vs team4 ==> [i]+1[/i]"""
    
    rem_days, rem_games, nb_rem = output_actions_inited.get_inited_remaining_games(sr_snowflake_account, sr_inited_gameday)
    assert nb_rem == 3
    assert rem_days == '3eme journee , 4eme journee'
    assert rem_games_exp == rem_games

@patch('output_actions_inited.snowflake_execute')   
def test_get_inited_remaining_games_empty_df(mock_execute):
    # this test the function get_inited_remaining_games with empty dataframe. Must return eempty string
    df_remaining = pd.DataFrame(columns=['GAMEDAY','GAME_MESSAGE','TEAM_HOME_NAME','TEAM_AWAY_NAME'])

    sr_inited_gameday = pd.Series({
        'GAMEDAY': 'GAMEDAY1',
        'IS_CALCULATED':1,
        'BEGIN_DATE_LOCAL': dt.strptime('2025-02-01', '%Y-%m-%d'),
        'BEGIN_TIME_LOCAL': '20:15:00',
        'GAMEDAY_MESSAGE': 'GD1',
        'SEASON_DIVISION': 'SD1',
        'SEASON_ID': 'S1',
        'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP': 1,
        'BEGIN_DATE_WEEKDAY': 'Monday'
    })
    
    sr_snowflake_account= pd.Series({
        'ACCOUNT': 'my_account',
        'WAREHOUSE': 'my_wh',
        'DATABASE_PROD': 'my_dbprod',
        'DATABASE_TEST': 'my_dbtest'
    })

    mock_execute.return_value = df_remaining

    
    rem_days, rem_games, nb_rem = output_actions_inited.get_inited_remaining_games(sr_snowflake_account, sr_inited_gameday)
    assert nb_rem == 0
    assert rem_days == ''
    assert rem_games == ""

def test_get_inited_dategame1_midnight_time():
    # this test the function get_inited_dategame1 with midnight time. Minute 00 must not be displayed

    sr_inited_gameday = pd.Series({
        'GAMEDAY': 'GAMEDAY1',
        'IS_CALCULATED':1,
        'BEGIN_DATE_LOCAL': dt.strptime('2025-02-01', '%Y-%m-%d'),
        'BEGIN_TIME_LOCAL': '00:00:00',
        'GAMEDAY_MESSAGE': 'GD1',
        'SEASON_DIVISION': 'SD1',
        'SEASON_ID': 'S1',
        'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP': 1,
        'BEGIN_DATE_WEEKDAY': 'Monday'}
        )
    
    dategame1 = output_actions_inited.get_inited_dategame1(sr_inited_gameday)
    assert dategame1 == "Monday 01/02 0h"

def test_get_inited_dategame1_invalid_time():
    # this test the function get_inited_dategame1 with invalid time. Must exit the program
    sr_inited_gameday = pd.Series({
        'GAMEDAY': 'GAMEDAY1',
        'IS_CALCULATED':1,
        'BEGIN_DATE_LOCAL': dt.strptime('2025-02-01', '%Y-%m-%d'),
        'BEGIN_TIME_LOCAL': 'invalid_time',
        'GAMEDAY_MESSAGE': 'GD1',
        'SEASON_DIVISION': 'SD1',
        'SEASON_ID': 'S1',
        'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP': 1,
        'BEGIN_DATE_WEEKDAY': 'Monday'}
        )
    assertExit(lambda: output_actions_inited.get_inited_dategame1(sr_inited_gameday))

def test_derive_inited_parameters_with_empty_countries():
    # this test the function derive_inited_parameters with empty countries. Must retrun empty dict
    with patch("output_actions_inited.fileA.read_json", return_value={}):
        with patch("output_actions_inited.config.multithreading_run", return_value=[]):
            
            param_dict = {"DATEGAME1": "DATEGAME1"}
            translations = []
            result = output_actions_inited.derive_inited_parameters(param_dict, translations)
            assert result == {}

def test_derive_inited_parameters_for_country_empty_translations():
    # this test the function derive_inited_parameters_for_country with empty translations. Must exit the program
    param_dict = {"DATEGAME1": "DATEGAME1"}
    translations = {} 
    assertExit(lambda: output_actions_inited.derive_inited_parameters_for_country(param_dict, 'FR', translations))
    
def test_create_inited_messages_for_country_no_remaining_games():
    # this test the function create_inited_messages_for_country with NB_GAMES_REMAINING = 0. Must not change #REMAINING_GAMEDAYS#
    params = {
        "DATEGAME1_FR": "Vendredi 01/03 20h",
        "GAMEDAY": "15",
        "LIST_GAMES": "Games list",
        "BONUS_GAME": "Bonus here",
        "USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP": 0,
        "NB_GAMES_REMAINING": 0
    }

    sr_inited_gameday = pd.Series({
        'GAMEDAY': 'GAMEDAY1',
        'IS_CALCULATED':1,
        'BEGIN_DATE_LOCAL': dt.strptime('2025-02-01', '%Y-%m-%d'),
        'BEGIN_TIME_LOCAL': '20:15:00',
        'GAMEDAY_MESSAGE': 'GD1',
        'SEASON_DIVISION': 'SD1',
        'SEASON_ID': 'S1',
        'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP': 1,
        'BEGIN_DATE_WEEKDAY': 'Monday'
    })

    with patch("output_actions_inited.outputA.format_message", side_effect=lambda t: t):
        with patch("output_actions_inited.outputA.replace_conditionally_message", side_effect=lambda **kwargs: kwargs["output_text"]):
            with patch("output_actions_inited.outputA.define_filename", return_value="file.txt"):
                with patch("output_actions_inited.fileA.create_txt") as mock_create:
                    template = "#DATEGAME1# - #GAMEDAY# - #LIST_GAMES# - #BONUS_GAME# #REMAINING_GAMEDAYS#"
                    content, country = output_actions_inited.create_inited_messages_for_country(params,"FR",template,sr_inited_gameday)
                    # REMAINING_GAMEDAYS should not appear because NB_GAMES_REMAINING=0
                    assert content == "Vendredi 01/03 20h - 15 - Games list - Bonus here #REMAINING_GAMEDAYS#"
                    assert country == "FR"

def test_create_inited_messages_for_country_none_param():
    # this test the function create_inited_messages_for_country with no parameters. Must exit the program
    param_dict = None
    country = 'FR'
    template = "#DATEGAME1#"
    sr_inited_gameday = pd.Series({
        'GAMEDAY': 'GAMEDAY1',
        'IS_CALCULATED':1,
        'BEGIN_DATE_LOCAL': dt.strptime('2025-02-01', '%Y-%m-%d'),
        'BEGIN_TIME_LOCAL': '20:15:00',
        'GAMEDAY_MESSAGE': 'GD1',
        'SEASON_DIVISION': 'SD1',
        'SEASON_ID': 'S1',
        'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP': 1,
        'BEGIN_DATE_WEEKDAY': 'Monday'
    })

    assertExit(lambda: output_actions_inited.create_inited_messages_for_country(param_dict, country, template, sr_inited_gameday))

def test_process_output_message_inited_with_no_topics():
    # this test the function process_output_message_inited with no topics provided. multithreading_run for posting should be called with empty list
    with patch("output_actions_inited.snowflake_execute", return_value=pd.DataFrame(columns=["FORUM_COUNTRY"])) as mock_exec:
        with patch("output_actions_inited.get_inited_parameters", return_value={"DATEGAME1":"X","GAMEDAY":"1","LIST_GAMES":"Y","BONUS_GAME":"Z","USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP":1,"REMAINING_GAMEDAYS":"A","REMAINING_GAMES":"B","NB_GAMES_REMAINING":0,"NB_GAMES":0}):
            with patch("output_actions_inited.derive_inited_parameters", return_value={"DATEGAME1_FR":"X"}) :
                with patch("output_actions_inited.config.multithreading_run", return_value=[]) as mock_mt:
                    dd = {"sr_snowflake_account_connect": {}, "str_output_gameday_init_template_FR": "template"}
                    output_actions_inited.process_output_message_inited(dd, {"dummy":"init"})
                    # posting step should have been called once with []
                    called_args = mock_mt.call_args_list[-1][0][1]
                    assert called_args == []

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_list_games_empty_df))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_list_games_special_chars))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_list_games_missing_key))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_remaining_games_duplicate_gamedays))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_remaining_games_empty_df))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_dategame1_midnight_time))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_dategame1_invalid_time))
    test_suite.addTest(unittest.FunctionTestCase(test_derive_inited_parameters_with_empty_countries))
    test_suite.addTest(unittest.FunctionTestCase(test_derive_inited_parameters_for_country_empty_translations))
    test_suite.addTest(unittest.FunctionTestCase(test_create_inited_messages_for_country_no_remaining_games))
    test_suite.addTest(unittest.FunctionTestCase(test_create_inited_messages_for_country_none_param))
    '''test_suite.addTest(unittest.FunctionTestCase(test_process_output_message_inited_with_no_topics))'''
    runner = unittest.TextTestRunner()
    runner.run(test_suite)