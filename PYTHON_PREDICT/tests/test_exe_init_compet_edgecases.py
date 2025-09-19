'''
This tests file concern all functions in the exe_init_compet module.
It units test unhappy paths
'''

import unittest
from unittest.mock import patch
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import exe_init_compet
from testutils import assertExit

def test_dropbox_initiate_folder_failure():
    # this test the function exe_init_compet with a failing dropboxA.initiate_folder. Must exit program
    with patch("exe_init_compet.dropbox_initiate_folder", side_effect=Exception("Dropbox error")):
        
        assertExit(lambda: exe_init_compet.exe_init_compet())

def test_initiate_local_environment_missing_key():
    # this test the function exe_init_compet with a failing fileA.initiate_local_environment, because of missing keys. Must exit program
    with patch("exe_init_compet.dropbox_initiate_folder"), \
         patch("exe_init_compet.fileA.initiate_local_environment", return_value={}):

        assertExit(lambda: exe_init_compet.exe_init_compet())

def test_game_extraction_failure():
    # this test the function exe_init_compet with a failing gameA.get_list_games_from_competition. Must exit program
    with patch("exe_init_compet.dropbox_initiate_folder"), \
         patch("exe_init_compet.fileA.initiate_local_environment", return_value={"sr_snowflake_account_connect": "con", "df_competition": "df"}), \
         patch("exe_init_compet.gameA.get_list_games_from_competition", side_effect=Exception("Game extraction failed")):

        assertExit(lambda: exe_init_compet.exe_init_compet())

def test_filter_data_failure():
    # this test the function exe_init_compet with a failing fileA.filter_data. Must exit program
    with patch("exe_init_compet.dropbox_initiate_folder"), \
         patch("exe_init_compet.fileA.initiate_local_environment", return_value={
             "sr_snowflake_account_connect": "con", 
             "df_competition": "df", 
             "df_paths": "paths"
         }), \
         patch("exe_init_compet.gameA.get_list_games_from_competition", return_value=[1, 2]), \
         patch("exe_init_compet.gameA.extract_games", return_value="df_game"), \
         patch("exe_init_compet.fileA.filter_data", side_effect=Exception("Filter failed")):

        assertExit(lambda: exe_init_compet.exe_init_compet())

def test_snowflake_update_failure():
    # this test the function exe_init_compet with a failing snowflakeA.update_snowflake. Must exit program
    with patch("exe_init_compet.dropbox_initiate_folder"), \
         patch("exe_init_compet.fileA.initiate_local_environment", return_value={
             "sr_snowflake_account_connect": "con", 
             "df_competition": "df", 
             "df_paths": "paths"
         }), \
         patch("exe_init_compet.gameA.get_list_games_from_competition", return_value=[1]), \
         patch("exe_init_compet.gameA.extract_games", return_value="df_game"), \
         patch("exe_init_compet.fileA.filter_data", return_value={"extra": "ok"}), \
         patch("exe_init_compet.snowflakeA.delete_tables_data_from_python"), \
         patch("exe_init_compet.snowflakeA.update_snowflake", side_effect=Exception("Snowflake error")):

        assertExit(lambda: exe_init_compet.exe_init_compet())

def test_calendar_update_failure():
    # this test the function exe_init_compet with a failing update_calendar_related_files. Must exit program
    with patch("exe_init_compet.dropbox_initiate_folder"), \
         patch("exe_init_compet.fileA.initiate_local_environment", return_value={
             "sr_snowflake_account_connect": "con", 
             "df_competition": "df", 
             "df_paths": "paths", 
             "df_task_done": "df_task"
         }), \
         patch("exe_init_compet.gameA.get_list_games_from_competition", return_value=[1]), \
         patch("exe_init_compet.gameA.extract_games", return_value="df_game"), \
         patch("exe_init_compet.fileA.filter_data", return_value={"extra": "ok"}), \
         patch("exe_init_compet.snowflakeA.delete_tables_data_from_python"), \
         patch("exe_init_compet.snowflakeA.update_snowflake"), \
         patch("exe_init_compet.update_calendar_related_files", side_effect=Exception("Calendar error")):

        assertExit(lambda: exe_init_compet.exe_init_compet())

def test_terminate_local_environment_failure():
    # this test the function exe_init_compet with a failing fileA.terminate_local_environment. Must exit program
    with patch("exe_init_compet.dropbox_initiate_folder"), \
         patch("exe_init_compet.fileA.initiate_local_environment", return_value={
             "sr_snowflake_account_connect": "con", 
             "df_competition": "df", 
             "df_paths": "paths", 
             "df_task_done": "df_task"
         }), \
         patch("exe_init_compet.gameA.get_list_games_from_competition", return_value=[1]), \
         patch("exe_init_compet.gameA.extract_games", return_value="df_game"), \
         patch("exe_init_compet.fileA.filter_data", return_value={"extra": "ok"}), \
         patch("exe_init_compet.snowflakeA.delete_tables_data_from_python"), \
         patch("exe_init_compet.snowflakeA.update_snowflake"), \
         patch("exe_init_compet.update_calendar_related_files", return_value="2025-01-01T00:00:00Z"), \
         patch("exe_init_compet.fileA.terminate_local_environment", side_effect=Exception("Terminate failed")):

        assertExit(lambda: exe_init_compet.exe_init_compet())

if __name__ == "__main__":
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_dropbox_initiate_folder_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_initiate_local_environment_missing_key))
    test_suite.addTest(unittest.FunctionTestCase(test_game_extraction_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_filter_data_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_snowflake_update_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_calendar_update_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_terminate_local_environment_failure))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)