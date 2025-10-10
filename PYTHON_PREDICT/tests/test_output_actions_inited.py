'''
This tests file concern all functions in the output_actions_inited module.
It units test the happy path for each function
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


@patch('output_actions_inited.snowflake_execute')
def test_get_inited_list_games(mock_execute):
    # this test the function get_inited_list_games

    df_games = pd.DataFrame([
    {"GAME_MESSAGE": '3EJ.01', "GAME_MESSAGE_SHORT": 1, "TEAM_HOME_NAME": 'team1',
     "TEAM_AWAY_NAME": "team2", "SCORE_HOME": 0, "SCORE_AWAY": 0, "RESULT": 0},
    
    {"GAME_MESSAGE": '3EJ.02', "GAME_MESSAGE_SHORT": 2, "TEAM_HOME_NAME": 'team3',
     "TEAM_AWAY_NAME": "team4", "SCORE_HOME": 0, "SCORE_AWAY": 0, "RESULT": 0}
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
    
    list_games_expected = f"""#3EJ.01# team1 vs team2 ==> [i]+1[/i]
#3EJ.02# team3 vs team4 ==> [i]+1[/i]"""
    
    list_games, nb_games = output_actions_inited.get_inited_list_games(sr_snowflake_account, sr_inited_gameday)
    assert list_games == list_games_expected
    assert nb_games == 2

@patch('output_actions_inited.snowflake_execute')
def test_get_inited_remaining_games(mock_execute):
    # this test the function get_inited_remaining_games

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

def test_get_inited_dategame1():
    # this test the function get_inited_dategame1

    sr_inited_gameday = pd.Series({
        'GAMEDAY': 'GAMEDAY1',
        'IS_CALCULATED':1,
        'BEGIN_DATE_LOCAL': dt.strptime('2025-02-01', '%Y-%m-%d'),
        'BEGIN_TIME_LOCAL': '20:15:00',
        'GAMEDAY_MESSAGE': 'GD1',
        'SEASON_DIVISION': 'SD1',
        'SEASON_ID': 'S1',
        'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP': 1,
        'BEGIN_DATE_WEEKDAY': 'Monday'}
        )
    
    dategame1 = output_actions_inited.get_inited_dategame1(sr_inited_gameday)
    assert dategame1 == "Monday 01/02 20h15"

@patch('output_actions_inited.get_inited_list_games', return_value=("GAMES_STR", 2))
@patch('output_actions_inited.get_inited_remaining_games', return_value=("REMAINING_DAYS", "REMAINING_GAMES", 1))
@patch('output_actions_inited.get_inited_dategame1', return_value="DATEGAME1_STR")
def test_get_inited_parameters(mock_date, mock_rem, mock_list):
    # this test the function get_inited_parameters

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

    params = output_actions_inited.get_inited_parameters(sr_snowflake_account, sr_inited_gameday)
    assert params['LIST_GAMES'] == "GAMES_STR"
    assert params['NB_GAMES'] == 2
    assert params['REMAINING_GAMEDAYS'] == "REMAINING_DAYS"
    assert params['DATEGAME1'] == "DATEGAME1_STR"

@patch('output_actions_inited.outputA.translate_param_for_country', side_effect=lambda v, c, t: v+"_translated")
def test_derive_inited_parameters_for_country(mock_translate):
    # this test the function derive_inited_parameters_for_country
    param_dict = {"DATEGAME1": "DATEGAME1"}
    translations = []
    derived = output_actions_inited.derive_inited_parameters_for_country(param_dict, "FR", translations)
    assert derived['DATEGAME1_FR'] == "DATEGAME1_translated"

@patch('output_actions_inited.fileA.read_json', return_value=[])
@patch('output_actions_inited.config.multithreading_run', side_effect=lambda func, args: [func(*a) for a in args])
@patch('output_actions_inited.outputA.translate_param_for_country', side_effect=lambda v, c, t: v+"_translated")
def test_derive_inited_parameters(mock_translate, mock_mt, mock_json):
    # this test the function derive_inited_parameters
    param_dict = {"DATEGAME1": "DATEGAME1"}
    derived = output_actions_inited.derive_inited_parameters(param_dict, ["FR"])
    assert derived['DATEGAME1_FR'] == "DATEGAME1_translated"

@patch('output_actions_inited.outputA.format_message', side_effect=lambda x: x)
@patch('output_actions_inited.outputA.replace_conditionally_message', side_effect=lambda **kwargs: kwargs['output_text'])
@patch('output_actions_inited.outputA.define_filename', return_value="file.txt")
@patch('output_actions_inited.fileA.create_txt')
def test_create_inited_messages_for_country(mock_create, mock_filename, mock_replace, mock_format):
    # this test the function create_inited_messages_for_country
    param_dict = {
        'DATEGAME1_FR': "DATE1",
        'GAMEDAY': 'GD1',
        'LIST_GAMES': 'GAMES',
        'BONUS_GAME': 'BONUS',
        'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP': 1,
        'NB_GAMES_REMAINING': 1,
        'REMAINING_GAMEDAYS': 'RDAYS',
        'REMAINING_GAMES': 'RGAMES'
    }
    template = "Template #DATEGAME1# #GAMEDAY# #LIST_GAMES# #BONUS_GAME#"
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
    content, country = output_actions_inited.create_inited_messages_for_country(param_dict, "FR", template, sr_inited_gameday)
    assert content == "Template DATE1 GD1 GAMES BONUS"
    assert country == "FR"

@patch('output_actions_inited.outputA.define_filename', return_value="fake_filename.txt")
@patch('output_actions_inited.fileA.create_txt', return_value=None)
@patch('output_actions_inited.post_message')
@patch('output_actions_inited.config.multithreading_run', side_effect=lambda func, args: [func(*a) for a in args])
@patch('output_actions_inited.derive_inited_parameters', return_value={'DATEGAME1_FR': 'DATE1_translated'})
@patch('output_actions_inited.get_inited_parameters', return_value={
    'GAMEDAY': 'GD1',
    'LIST_GAMES': 'GAMES',
    'NB_GAMES': 2,
    'BONUS_GAME': 'BONUS',
    'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP': 1,
    'REMAINING_GAMEDAYS': 'RDAYS',
    'REMAINING_GAMES': 'RGAMES',
    'NB_GAMES_REMAINING': 1,
    'DATEGAME1': 'DATE1'
})
@patch('output_actions_inited.snowflake_execute', return_value=pd.DataFrame({'FORUM_COUNTRY': ['FR']}))
def test_process_output_message_inited(mock_aa,mock_sf, mock_get_params, mock_derive_params, mock_mt, mock_post, mock_file):
    # this test the function process_output_message_inited
    dd = {
        'sr_snowflake_account_connect': pd.Series({
            'ACCOUNT': 'my_account',
            'WAREHOUSE': 'my_wh',
            'DATABASE_PROD': 'my_dbprod',
            'DATABASE_TEST': 'my_dbtest'
        }),
        'str_output_gameday_init_template_FR': "Template #DATEGAME1#"
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

    output_actions_inited.process_output_message_inited(dd, sr_inited_gameday)

    mock_post.assert_called()
    mock_file.assert_called()


if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_list_games))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_remaining_games))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_dategame1))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_parameters))
    test_suite.addTest(unittest.FunctionTestCase(test_derive_inited_parameters_for_country))
    test_suite.addTest(unittest.FunctionTestCase(test_derive_inited_parameters))
    test_suite.addTest(unittest.FunctionTestCase(test_create_inited_messages_for_country))
    test_suite.addTest(unittest.FunctionTestCase(test_process_output_message_inited))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
