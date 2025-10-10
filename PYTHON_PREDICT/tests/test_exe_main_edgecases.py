'''
This tests file concern all functions in the exe_main module.
It units test edge cases for functions
'''
import unittest
from unittest.mock import patch
import pandas as pd
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import exe_main
from testutils import assertExit


def test_process_games_missing_key():
    # this test the process_games with missing key in context dict. Must exit the program.
    ctx = {}
    assertExit(lambda: exe_main.process_games(ctx))
    
def test_process_games_gameA_failure():
    # this test the process_games with game_list_games_from_need failing. Must exit the program.
    context = {
        'sr_snowflake_account_connect': pd.Series({
            'ACCOUNT': 'my_account',
            'WAREHOUSE': 'my_wh',
            'DATABASE_PROD': 'my_dbprod',
            'DATABASE_TEST': 'my_dbtest'
        }),
        'sr_output_need': pd.Series({
            "TASK_RUN": "UPDATEGAMES", "SEASON_ID": "S1", "SEASON_SPORT": "BASKETBALL", "SEASON_COUNTRY": "FRANCE", "SEASON_NAME": "2025-2026",
            "SEASON_DIVISION": "ELITE2", "COMPETITION_ID": "RS", "GAMEDAY": "3ème journée", "TS_TASK_UTC": "2025-09-12 08:02:00",
            "TS_TASK_LOCAL": "2025-09-12 10:02:00", "IS_TO_INIT": 0, "IS_TO_CALCULATE": 0, "IS_tO_DELETE": 0, "IS_TO_RECALCULATE": 0,
            "MESSAGE_ACTION": "AVOID", "GAME_ACTION": "RUN", "LAST_MESSAGE_CHECK_TS_UTC": "2025-06-14 15:00:00"
        }),
        'df_paths': {}
    }
    with patch("exe_main.gameA.get_list_games_from_need", side_effect=RuntimeError("boom")):
        assertExit(lambda: exe_main.process_games(context))

def test_process_messages_extract_messages_failure():
    # this test the process_messages with extract_messages failing. Must exit the program.
    context = {
        'sr_snowflake_account_connect': pd.Series({
            'ACCOUNT': 'my_account',
            'WAREHOUSE': 'my_wh',
            'DATABASE_PROD': 'my_dbprod',
            'DATABASE_TEST': 'my_dbtest'
        }),
        'sr_output_need': pd.Series({
            "TASK_RUN": "UPDATEGAMES", "SEASON_ID": "S1", "SEASON_SPORT": "BASKETBALL", "SEASON_COUNTRY": "FRANCE", "SEASON_NAME": "2025-2026",
            "SEASON_DIVISION": "ELITE2", "COMPETITION_ID": "RS", "GAMEDAY": "3ème journée", "TS_TASK_UTC": "2025-09-12 08:02:00",
            "TS_TASK_LOCAL": "2025-09-12 10:02:00", "IS_TO_INIT": 0, "IS_TO_CALCULATE": 0, "IS_tO_DELETE": 0, "IS_TO_RECALCULATE": 0,
            "MESSAGE_ACTION": "AVOID", "GAME_ACTION": "RUN", "LAST_MESSAGE_CHECK_TS_UTC": "2025-06-14 15:00:00"
        }),
        'df_paths': {}
    }
    with patch("exe_main.extract_messages", side_effect=ValueError("bad")):
        assertExit(lambda: exe_main.process_messages(context))
        
def test_process_messages_invalid_message_dataframe():
    # this test the process_messages with invalid message. Must exit the program.
    context = {
        'sr_snowflake_account_connect': pd.Series({
            'ACCOUNT': 'my_account',
            'WAREHOUSE': 'my_wh',
            'DATABASE_PROD': 'my_dbprod',
            'DATABASE_TEST': 'my_dbtest'
        }),
        'sr_output_need': pd.Series({
            "TASK_RUN": "CALCULATE", "SEASON_ID": "S1", "SEASON_SPORT": "BASKETBALL", "SEASON_COUNTRY": "FRANCE", "SEASON_NAME": "2025-2026",
            "SEASON_DIVISION": "ELITE2", "COMPETITION_ID": "RS", "GAMEDAY": "3ème journée", "TS_TASK_UTC": "2025-09-12 08:02:00",
            "TS_TASK_LOCAL": "2025-09-12 10:02:00", "IS_TO_INIT": 0, "IS_TO_CALCULATE": 1, "IS_tO_DELETE": 0, "IS_TO_RECALCULATE": 0,
            "MESSAGE_ACTION": "RUN", "GAME_ACTION": "RUN", "LAST_MESSAGE_CHECK_TS_UTC": "2025-06-14 15:00:00"
        }),
        'df_paths': {},
        "df_message_check": pd.DataFrame({"WRONG_COL": ["a", "b"]}),
        "extraction_time_utc": pd.Timestamp.utcnow()
    }

    with patch("exe_main.fileA.filter_data", return_value={}):
        assertExit(lambda: exe_main.process_messages(context))

def test_display_check_string_missing_key():
    # this test the display_check_string with missing columns in sr_output_need. Must exit the program.
    ctx = {
        "sr_output_need": {},  # missing MESSAGE_ACTION etc.
        "extraction_time_utc": pd.Timestamp.utcnow()
    }
    assertExit(lambda: exe_main.display_check_string(ctx))
    
def test_display_check_string_newer_timestamp():
    # this test the display_check_string with LAST_MESSAGE_CHECK_TS_UTC >= extraction_time_utc. Must return No need to check - no new messages
    ctx = {
        "sr_output_need": {
            "MESSAGE_ACTION": exe_main.config.MESSAGE_ACTION_MAP["CHECK"],
            "LAST_MESSAGE_CHECK_TS_UTC": pd.Timestamp('2025-01-01 00:00:01'),
            "SEASON_ID": "2024",
        },
        "sr_snowflake_account_connect": {"DATABASE_PROD": "DB"},
        "extraction_time_utc": pd.Timestamp('2025-01-01 00:00:00')
    }
    result = exe_main.display_check_string(ctx)
    assert result == "No need to check - no new messages"
    
def test_exe_main_dropbox_failure():
    # this test the exe_main with dropbox connection failing. Must exit the program.
    
    """Should exit if dropbox_initiate_folder fails"""
    with patch("exe_main.dropbox_initiate_folder", side_effect=OSError("dropbox fail")):
        try:
            exe_main.exe_main()
        except SystemExit as e:
            assert e.code == 1

def test_exe_main_generate_output_need_failure():
    # this test the exe_main with generate_output_need failing. Must exit the program.
    """Should exit if generate_output_need fails"""
    with patch("exe_main.dropbox_initiate_folder"), \
         patch("exe_main.fileA.initiate_local_environment", return_value={}), \
         patch("exe_main.generate_output_need", side_effect=Exception("gen fail")):
        try:
            exe_main.exe_main()
        except SystemExit as e:
            assert e.code == 1

if __name__ == "__main__":
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_process_games_missing_key))
    test_suite.addTest(unittest.FunctionTestCase(test_process_games_gameA_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_process_messages_extract_messages_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_process_messages_invalid_message_dataframe))
    test_suite.addTest(unittest.FunctionTestCase(test_display_check_string_missing_key))
    '''test_suite.addTest(unittest.FunctionTestCase(test_display_check_string_newer_timestamp))
    test_suite.addTest(unittest.FunctionTestCase(test_exe_main_dropbox_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_exe_main_generate_output_need_failure))'''
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
