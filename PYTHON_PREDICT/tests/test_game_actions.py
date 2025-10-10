'''
This tests file concern all functions in the game_actions.
It units test the happy path for each function
'''

import unittest
import pandas as pd
from unittest.mock import patch
from pandas.testing import assert_frame_equal
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import game_actions

# Sample dummy data
game = pd.read_csv("materials/game.csv")
sample_game_row = pd.Series({
    'COMPETITION_SOURCE': 'LNB',
    'GAME_SOURCE_ID': 28316
})

sample_game_details_df = game.iloc[[0]]

@patch.dict('os.environ', {'LNB_URL': 'http://dummyurl.com'})
@patch.dict('game_actions.game_info_functions', {'LNB': lambda url, row: sample_game_details_df})
def test_extract_game():
    # this test the function extract_game
    result = game_actions.extract_game(sample_game_row)
    assert_frame_equal(result.reset_index(drop=True), sample_game_details_df.reset_index(drop=True))

@patch('game_actions.snowflake_execute', return_value=pd.DataFrame([]))
def test_get_list_games_from_competition(mock_snowflake):
    # this test the function get_list_games_from_competition
    df_account = pd.DataFrame([{'account': 'dummy'}])
    df_comp = pd.DataFrame([{
        'SEASON_ID': 'S1',
        'COMPETITION_ID': 'C1',
        'COMPETITION_SOURCE': 'LNB',
        'GAME_SOURCE_ID_MIN': 100,
        'GAME_SOURCE_ID_MAX': 105
    }])
    data = {
    "SEASON_ID": ["S1"] * 6,
    "COMPETITION_ID": ["C1"] * 6,
    "COMPETITION_SOURCE": ["LNB"] * 6,
    "GAME_SOURCE_ID": ['100', '101', '102', '103', '104', '105']
    }
    expected_df = pd.DataFrame(data).astype(str)
    result = game_actions.get_list_games_from_competition(df_account, df_comp).astype(str)
    assert_frame_equal(result.reset_index(drop=True), expected_df.reset_index(drop=True))

@patch('game_actions.snowflake_execute', return_value=pd.DataFrame([{
    'SEASON_ID': 'S1',
    'COMPETITION_ID': 'C1',
    'COMPETITION_SOURCE': 'LNB',
    'GAME_SOURCE_ID': 123
}]))
def test_get_list_games_from_need(mock_snowflake):
    # this test the function get_list_games_from_need
    expected_df = pd.DataFrame([{
    'SEASON_ID': 'S1',
    'COMPETITION_ID': 'C1',
    'COMPETITION_SOURCE': 'LNB',
    'GAME_SOURCE_ID': 123
    }])
    
    sr_account = pd.Series({
    'ACCOUNT': 'dummy',
    'WAREHOUSE': 'wh',
    'DATABASE_PROD': 'prod',
    'DATABASE_TEST': 'test'
    })
    df_need = pd.DataFrame([{
        'SEASON_ID': 'S1',
        'GAMEDAY': 1
    }])
    result = game_actions.get_list_games_from_need(sr_account, df_need)
    assert_frame_equal(result.reset_index(drop=True), expected_df.reset_index(drop=True))


if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_extract_game))
    test_suite.addTest(unittest.FunctionTestCase(test_get_list_games_from_competition))
    test_suite.addTest(unittest.FunctionTestCase(test_get_list_games_from_need))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
