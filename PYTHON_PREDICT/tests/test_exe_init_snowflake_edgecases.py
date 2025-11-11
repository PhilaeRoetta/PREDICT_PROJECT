'''
This tests file concern all functions in the exe_init_snowflake module.
It units test unhappy paths
'''

import unittest
from unittest.mock import patch
import sys
import os
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import exe_init_snowflake
from testutils import assertExit

def test_dropbox_init_failure():
    
    # this test the function exe_init_snowflake with a failing dropboxA.initiate_folder. Must exit the program
    with patch("exe_init_snowflake.dropboxA.initiate_folder", side_effect=Exception("Dropbox init failed")):
            
            assertExit(lambda: exe_init_snowflake.exe_init_snowflake())

def test_local_env_failure():
    
    # this test the function exe_init_snowflake with a failing fileA.initiate_local_environment. Must exit the program
    with patch("exe_init_snowflake.dropboxA.initiate_folder"), \
         patch("exe_init_snowflake.fileA.initiate_local_environment", side_effect=Exception("Local env failed")):

          assertExit(lambda: exe_init_snowflake.exe_init_snowflake())

def test_snowflake_script_failure():
    
    # this test the function exe_init_snowflake with a failing snowflakeA.snowflake_execute_script. Must exit the program
    mock_initiate_local_dict = {
        "sr_snowflake_account_connect": pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        "str_script_creating_database" : "",
        "df_paths": pd.read_csv("materials/paths.csv")
    }

    with patch("exe_init_snowflake.dropboxA.initiate_folder"), \
         patch("exe_init_snowflake.fileA.initiate_local_environment", return_value=mock_initiate_local_dict), \
         patch("exe_init_snowflake.snowflakeA.snowflake_execute_script", side_effect=Exception("Snowflake script failed")):

          assertExit(lambda: exe_init_snowflake.exe_init_snowflake())

def test_dropbox_download_failure():
    
    # this test the function exe_init_snowflake with a failing dropboxA.download_folder. Must exit the program
    mock_initiate_local_dict = {
        "sr_snowflake_account_connect": pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        "str_script_creating_database" : "", \
        "df_paths": pd.read_csv("materials/paths.csv"),

        "df_competition": pd.read_csv("materials/competition.csv"),
        "df_task_done": pd.read_csv("materials/task_done.csv")
    }

    with patch("exe_init_snowflake.dropboxA.initiate_folder"), \
         patch("exe_init_snowflake.fileA.initiate_local_environment", return_value=mock_initiate_local_dict), \
         patch("exe_init_snowflake.snowflakeA.snowflake_execute_script"), \
         patch("exe_init_snowflake.dropboxA.download_folder", side_effect=Exception("Download failed")):

          assertExit(lambda: exe_init_snowflake.exe_init_snowflake())

def test_update_snowflake_failure():
    
    # this test the function exe_init_snowflake with a failing snowflakeA.update_snowflake. Must exit the program
    mock_initiate_local_dict = {
        "sr_snowflake_account_connect": pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        "str_script_creating_database" : "", \
        "df_paths": pd.read_csv("materials/paths.csv"),

        "df_competition": pd.read_csv("materials/competition.csv"),
        "df_task_done": pd.read_csv("materials/task_done.csv")
    }

    with patch("exe_init_snowflake.dropboxA.initiate_folder"), \
         patch("exe_init_snowflake.fileA.initiate_local_environment", return_value=mock_initiate_local_dict), \
         patch("exe_init_snowflake.snowflakeA.snowflake_execute_script"), \
         patch("exe_init_snowflake.dropboxA.download_folder"), \
         patch("exe_init_snowflake.snowflakeA.update_snowflake", side_effect=Exception("Update failed")):

          assertExit(lambda: exe_init_snowflake.exe_init_snowflake())

def test_terminate_local_env_failure():
    
    # this test the function exe_init_snowflake with a failing fileA.terminate_local_environment. Must exit the program
    mock_initiate_local_dict = {
        "sr_snowflake_account_connect": pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        "str_script_creating_database" : "", \
        "df_paths": pd.read_csv("materials/paths.csv"),

        "df_competition": pd.read_csv("materials/competition.csv"),
        "df_task_done": pd.read_csv("materials/task_done.csv")
    }

    with patch("exe_init_snowflake.dropboxA.initiate_folder"), \
         patch("exe_init_snowflake.fileA.initiate_local_environment", return_value=mock_initiate_local_dict), \
         patch("exe_init_snowflake.snowflakeA.snowflake_execute_script"), \
         patch("exe_init_snowflake.dropboxA.download_folder"), \
         patch("exe_init_snowflake.snowflakeA.update_snowflake"), \
         patch("exe_init_snowflake.fileA.terminate_local_environment", side_effect=Exception("Terminate failed")):

          assertExit(lambda: exe_init_snowflake.exe_init_snowflake())

if __name__ == "__main__":
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_dropbox_init_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_local_env_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_snowflake_script_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_dropbox_download_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_update_snowflake_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_terminate_local_env_failure))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)