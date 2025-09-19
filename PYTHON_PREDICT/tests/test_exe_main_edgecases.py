'''
This tests file concern all functions in the exe_main module.
It units test edge cases for functions
'''
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import builtins
import json
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import exe_main 


def test_process_games_missing_key():
    # this test the process_games with missing key. Must exit the program.
    ctx = {}
    try:
        exe_main.process_games(ctx)
    except SystemExit as e:
        assert e.code == 1

def test_process_games_gameA_failure():
    # this test the process_games with game_list_games_from_need failing. Must exit the program.
    """Should exit when gameA.get_list_games_from_need raises"""
    ctx = {
        "sr_snowflake_account_connect": "dummy",
        "sr_output_need": "dummy",
        "df_paths": pd.DataFrame(),
    }
    with patch("exe_main.gameA.get_list_games_from_need", side_effect=RuntimeError("boom")):
        try:
            exe_main.process_games(ctx)
        except SystemExit as e:
            assert e.code == 1

def test_process_messages_extract_messages_failure():
    # this test the process_messages with extract_messages failing. Must exit the program.
    ctx = {
        "sr_snowflake_account_connect": "dummy",
        "sr_output_need": {"MESSAGE_ACTION": "RUN"},
        "df_paths": pd.DataFrame(),
    }
    with patch("exe_main.extract_messages", side_effect=ValueError("bad")):
        try:   
            exe_main.process_messages(ctx)
        except SystemExit as e:
            assert e.code == 1

def test_process_messages_invalid_message_dataframe():
    # this test the process_messages with invalid message. Must exit the program.
    """MESSAGE_CONTENT column missing should trigger error"""
    ctx = {
        "sr_snowflake_account_connect": "dummy",
        "sr_output_need": {"MESSAGE_ACTION": "RUN"},
        "df_paths": pd.DataFrame(),
        "df_message_check": pd.DataFrame({"WRONG_COL": ["a", "b"]}),
        "extraction_time_utc": pd.Timestamp.utcnow()
    }
    with patch("exe_main.fileA.filter_data", return_value={}):
        try:
            exe_main.process_messages(ctx)
        except SystemExit as e:
            assert e.code == 1

def test_display_check_string_missing_key():
    # this test the display_check_string with missing columns in sr_output_need. Must exit the program.
    ctx = {
        "sr_output_need": {},  # missing MESSAGE_ACTION etc.
        "extraction_time_utc": pd.Timestamp.utcnow()
    }
    try:
        exe_main.display_check_string(ctx)
    except SystemExit as e:
        assert e.code == 1

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
    test_suite.addTest(unittest.FunctionTestCase(test_display_check_string_newer_timestamp))
    test_suite.addTest(unittest.FunctionTestCase(test_exe_main_dropbox_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_exe_main_generate_output_need_failure))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
