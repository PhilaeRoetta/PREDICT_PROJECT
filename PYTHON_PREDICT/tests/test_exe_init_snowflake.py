'''
This tests file concern all functions in the exe_init_snowflake module.
It units test the happy path for each function
'''

import unittest
from unittest.mock import patch
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import exe_init_snowflake  

@patch("exe_init_snowflake.dropboxA.initiate_folder")
@patch("exe_init_snowflake.fileA.initiate_local_environment")
@patch("exe_init_snowflake.snowflakeA.snowflake_execute_script")
@patch("exe_init_snowflake.dropboxA.download_folder")
@patch("exe_init_snowflake.snowflakeA.update_snowflake")
@patch("exe_init_snowflake.fileA.terminate_local_environment")
def test_exe_init_snowflake_happy_path(
    # this test the function exe_init_snowflake mocking all dependencies
    mock_terminate,
    mock_update_snowflake,
    mock_download_folder,
    mock_snowflake_execute,
    mock_initiate_local,
    mock_initiate_folder
):
    # Setup mocks
    mock_initiate_folder.return_value = None
    mock_initiate_local.return_value = {
        'sr_snowflake_account_connect': 'fake_connection',
        'str_script_creating_database': 'fake_script',
        'df_paths': ['file1', 'file2']
    }
    mock_snowflake_execute.return_value = None
    mock_download_folder.return_value = None
    mock_update_snowflake.return_value = None
    mock_terminate.return_value = None

    # Call the function
    exe_init_snowflake.exe_init_snowflake()

    # Assertions: verify that each external call was made once
    mock_initiate_folder.assert_called_once()
    mock_initiate_local.assert_called_once_with(exe_init_snowflake.config.CALLER["SNOWFLAKE"])
    mock_snowflake_execute.assert_called_once_with('fake_connection', 'fake_script')
    mock_download_folder.assert_called_once_with("database_folder", ['file1', 'file2'], exe_init_snowflake.config.TMPD)
    mock_update_snowflake.assert_called_once_with(exe_init_snowflake.config.CALLER["SNOWFLAKE"], 
                                                  {
                                                      'sr_snowflake_account_connect': 'fake_connection',
                                                      'str_script_creating_database': 'fake_script',
                                                      'df_paths': ['file1', 'file2']
                                                  }, 
                                                  exe_init_snowflake.config.TMPD)
    mock_terminate.assert_called_once_with(exe_init_snowflake.config.CALLER["SNOWFLAKE"], 
                                           {
                                               'sr_snowflake_account_connect': 'fake_connection',
                                               'str_script_creating_database': 'fake_script',
                                               'df_paths': ['file1', 'file2']
                                           })

# Run tests if this file is executed directly
if __name__ == "__main__":
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_exe_init_snowflake_happy_path))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
