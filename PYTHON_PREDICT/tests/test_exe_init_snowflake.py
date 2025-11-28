'''
This tests file concern all functions in the exe_init_snowflake module.
It units test the happy path for each function
'''

import unittest
from unittest.mock import patch
import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import exe_init_snowflake  

def test_exe_init_snowflake_happy_path():
    
    # this test the function exe_init_snowflake mocking all dependencies
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
         patch("exe_init_snowflake.fileA.terminate_local_environment"):

          exe_init_snowflake.exe_init_snowflake()

    
if __name__ == "__main__":
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_exe_init_snowflake_happy_path))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
