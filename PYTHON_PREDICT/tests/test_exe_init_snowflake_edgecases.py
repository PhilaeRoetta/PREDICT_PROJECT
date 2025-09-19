'''
This tests file concern all functions in the exe_init_snowflake module.
It units test unhappy paths
'''

import unittest
from unittest.mock import patch
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import exe_init_snowflake  

def test_dropbox_init_failure():
    # this test the function exe_init_snowflake with a failing dropboxA.initiate_folder. Must exit program
    with patch("exe_init_snowflake.dropboxA.initiate_folder", side_effect=Exception("Dropbox init failed")), \
         patch("exe_init_snowflake.config.onError_final_execute") as mock_final_execute, \
         patch("exe_init_snowflake.config.sys_exit") as mock_exit:
        
        try:
            exe_init_snowflake.exe_init_snowflake()
        except SystemExit:
            # Confirm final execute was called
            mock_final_execute.assert_called_once()
            # Confirm sys_exit was called with 1
            mock_exit.assert_called_once_with(1)

@patch("exe_init_snowflake.dropboxA.initiate_folder")
@patch("exe_init_snowflake.fileA.initiate_local_environment", side_effect=Exception("Local env failed"))
@patch("exe_init_snowflake.config.onError_final_execute")
@patch("exe_init_snowflake.config.sys_exit")
def test_local_env_failure(mock_exit, mock_final_execute, mock_local_env, mock_dropbox):
    # this test the function exe_init_snowflake with a failing fileA.initiate_local_environment. Must exit program
    
    try:
        exe_init_snowflake.exe_init_snowflake()
    except SystemExit:
        # Confirm final execute was called
        mock_final_execute.assert_called_once()
        # Confirm sys_exit was called with 1
        mock_exit.assert_called_once_with(1)

@patch("exe_init_snowflake.dropboxA.initiate_folder")
@patch("exe_init_snowflake.fileA.initiate_local_environment", return_value={"sr_snowflake_account_connect": {}, "str_script_creating_database": "script.sql", "df_paths": {}})
@patch("exe_init_snowflake.snowflakeA.snowflake_execute_script", side_effect=Exception("Snowflake script failed"))
@patch("exe_init_snowflake.config.onError_final_execute")
@patch("exe_init_snowflake.config.sys_exit")
def test_snowflake_script_failure(mock_exit, mock_final_execute, mock_snowflake, mock_local_env, mock_dropbox):
    # this test the function exe_init_snowflake with a failing snowflakeA.snowflake_execute_script. Must exit program
    try:
        exe_init_snowflake.exe_init_snowflake()
    except SystemExit:
        # Confirm final execute was called
        mock_final_execute.assert_called_once()
        # Confirm sys_exit was called with 1
        mock_exit.assert_called_once_with(1)

@patch("exe_init_snowflake.dropboxA.initiate_folder")
@patch("exe_init_snowflake.fileA.initiate_local_environment", return_value={"sr_snowflake_account_connect": {}, "str_script_creating_database": "script.sql", "df_paths": {}})
@patch("exe_init_snowflake.snowflakeA.snowflake_execute_script")
@patch("exe_init_snowflake.dropboxA.download_folder", side_effect=Exception("Download failed"))
@patch("exe_init_snowflake.config.onError_final_execute")
@patch("exe_init_snowflake.config.sys_exit")
def test_dropbox_download_failure(mock_exit, mock_final_execute, mock_download, mock_snowflake, mock_local_env, mock_dropbox):
    # this test the function exe_init_snowflake with a failing dropboxA.download_folder. Must exit program
    try:
        exe_init_snowflake.exe_init_snowflake()
    except SystemExit:
        # Confirm final execute was called
        mock_final_execute.assert_called_once()
        # Confirm sys_exit was called with 1
        mock_exit.assert_called_once_with(1)

@patch("exe_init_snowflake.dropboxA.initiate_folder")
@patch("exe_init_snowflake.fileA.initiate_local_environment", return_value={"sr_snowflake_account_connect": {}, "str_script_creating_database": "script.sql", "df_paths": {}})
@patch("exe_init_snowflake.snowflakeA.snowflake_execute_script")
@patch("exe_init_snowflake.dropboxA.download_folder")
@patch("exe_init_snowflake.snowflakeA.update_snowflake", side_effect=Exception("Update failed"))
@patch("exe_init_snowflake.config.onError_final_execute")
@patch("exe_init_snowflake.config.sys_exit")
def test_update_snowflake_failure(mock_exit, mock_final_execute, mock_update, mock_download, mock_snowflake, mock_local_env, mock_dropbox):
    # this test the function exe_init_snowflake with a failing snowflakeA.update_snowflake. Must exit program
    try:
        exe_init_snowflake.exe_init_snowflake()
    except SystemExit:
        # Confirm final execute was called
        mock_final_execute.assert_called_once()
        # Confirm sys_exit was called with 1
        mock_exit.assert_called_once_with(1)

@patch("exe_init_snowflake.dropboxA.initiate_folder")
@patch("exe_init_snowflake.fileA.initiate_local_environment", return_value={"sr_snowflake_account_connect": {}, "str_script_creating_database": "script.sql", "df_paths": {}})
@patch("exe_init_snowflake.snowflakeA.snowflake_execute_script")
@patch("exe_init_snowflake.dropboxA.download_folder")
@patch("exe_init_snowflake.snowflakeA.update_snowflake")
@patch("exe_init_snowflake.fileA.terminate_local_environment", side_effect=Exception("Terminate failed"))
@patch("exe_init_snowflake.config.onError_final_execute")
@patch("exe_init_snowflake.config.sys_exit")
def test_terminate_local_env_failure(mock_exit, mock_final_execute, mock_terminate, mock_update, mock_download, mock_snowflake, mock_local_env, mock_dropbox):
    # this test the function exe_init_snowflake with a failing fileA.terminate_local_environment. Must exit program
    exe_init_snowflake.exe_init_snowflake()
    mock_final_execute.assert_called_once()
    mock_exit.assert_called_once_with(1)

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