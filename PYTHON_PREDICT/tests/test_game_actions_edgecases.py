'''
This tests file concern all functions in the game_actions module.
It units test the unexpected path for each function, which return exception
'''

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import game_actions

# To avoid sys.exit from killing the test runner
def prevent_exit(*args, **kwargs):
    raise Exception("sys.exit called")

def test_extract_game_missing_env_var():
    # this test the function extract_game without the environment variable
    with patch.dict('os.environ', {}, clear=True), \
         patch('game_actions.game_info_functions', {'LNB': MagicMock(return_value=pd.DataFrame())}), \
         patch('game_actions.get_game_details_lnb', return_value=pd.DataFrame()):
        game_row = pd.Series({'COMPETITION_SOURCE': 'LNB', 'GAME_SOURCE_ID': 123})
        try:
            game_actions.extract_game(game_row)
        except SystemExit:
            assert True  # expected due to decorator exit
        except Exception:
            assert False, "Should exit due to missing env var"

def test_extract_game_unknown_competition_source():
    # this test the function extract_game with an unknown competition source
    with patch.dict('os.environ', {'XYZ_URL': 'http://fakeurl.com'}), \
         patch('game_actions.game_info_functions', {'LNB': MagicMock()}):
        game_row = pd.Series({'COMPETITION_SOURCE': 'XYZ', 'GAME_SOURCE_ID': 123})
        try:
            game_actions.extract_game(game_row)
        except SystemExit:
            assert True
        except Exception:
            assert False, "Should exit due to NoneType function call"

def test_extract_games_with_multithread_failure():
    # this test the function extract_games with a multithread failing
    with patch('game_actions.config.multithreading_run', side_effect=Exception("thread fail")), \
         patch('game_actions.create_csv'), \
         patch('game_actions.pd', new=pd):
        df = pd.DataFrame([{'COMPETITION_SOURCE': 'LNB', 'GAME_SOURCE_ID': 1}])
        try:
            game_actions.extract_games(df)
        except SystemExit:
            assert True
        except Exception:
            assert False, "Should exit due to multithreading failure"

def test_get_list_games_from_competition_merge_fail():
    # this test the function get_list_games_from_competition failing
    with patch('game_actions.snowflake_execute', return_value=pd.Series({})), \
         patch('game_actions.pd', new=pd):
        sr_account = pd.Series({})
        df_comp = pd.DataFrame([{
            'SEASON_ID': '2022', 'COMPETITION_ID': 'A',
            'COMPETITION_SOURCE': 'LNB',
            'GAME_SOURCE_ID_MIN': 2, 'GAME_SOURCE_ID_MAX': 1  # reverse range = empty list
        }])
        try:
            df = game_actions.get_list_games_from_competition(sr_account, df_comp)
            assert df.empty, "Should return empty dataframe"
        except Exception:
            assert False, "Should not raise exception even for bad range"

def test_get_list_games_from_need_snowflake_fail():
    # this test the function get_list_games_from_need failing
    with patch('game_actions.snowflake_execute', side_effect=Exception("Snowflake failure")):
        sr_account = pd.Series({
        'ACCOUNT': 'acc',
        'WAREHOUSE': 'wh',
        'DATABASE_PROD': 'prod',
        'DATABASE_TEST': 'test'
        })
        df_need = pd.DataFrame([{'SEASON_ID': '2022', 'GAMEDAY': '3'}])
        try:
            game_actions.get_list_games_from_need(sr_account, df_need)
        except SystemExit:
            assert True
        except Exception:
            assert False, "Should exit gracefully due to decorator"

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_extract_game_missing_env_var))
    test_suite.addTest(unittest.FunctionTestCase(test_extract_game_unknown_competition_source))
    test_suite.addTest(unittest.FunctionTestCase(test_extract_games_with_multithread_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_get_list_games_from_competition_merge_fail))
    test_suite.addTest(unittest.FunctionTestCase(test_get_list_games_from_need_snowflake_fail))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
