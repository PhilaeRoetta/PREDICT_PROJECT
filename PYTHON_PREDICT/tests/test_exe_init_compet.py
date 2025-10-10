'''
This tests file concern all functions in the exe_init_compet module.
It units test the happy path for each function
'''

import unittest
from unittest.mock import patch
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import exe_init_compet

# Patch all external dependencies so we don't actually hit DB or Dropbox
@patch("exe_init_compet.dropbox_initiate_folder")
@patch("exe_init_compet.fileA.initiate_local_environment")
@patch("exe_init_compet.gameA.get_list_games_from_competition")
@patch("exe_init_compet.gameA.extract_games")
@patch("exe_init_compet.fileA.filter_data")
@patch("exe_init_compet.snowflakeA.delete_tables_data_from_python")
@patch("exe_init_compet.snowflakeA.update_snowflake")
@patch("exe_init_compet.update_calendar_related_files")
@patch("exe_init_compet.fileA.terminate_local_environment")
def test_exe_init_compet_happy_path(
    # this test the function exe_init_compet mocking all dependencies
    mock_terminate,
    mock_update_calendar,
    mock_update_snowflake,
    mock_delete_tables,
    mock_filter_data,
    mock_extract_games,
    mock_get_list_games,
    mock_initiate_local,
    mock_dropbox
):
    # Setup mock return values
    mock_initiate_local.return_value = {
        "sr_snowflake_account_connect": "mock_connect",
        "df_competition": "mock_competition",
        "df_paths": "mock_paths",
        "df_task_done": "mock_task_done"
    }
    mock_get_list_games.return_value = ["game1", "game2"]
    mock_extract_games.return_value = "mock_df_game"
    mock_filter_data.return_value = {"df_paths": "mock_filtered_paths"}
    mock_update_calendar.return_value = "2025-09-01T00:00:00Z"

    # Call the function (happy path)
    exe_init_compet.exe_init_compet()

    # Assertions to ensure all steps were called
    mock_dropbox.assert_called_once()
    mock_initiate_local.assert_called_once()
    mock_get_list_games.assert_called_once_with("mock_connect", "mock_competition")
    mock_extract_games.assert_called_once_with(["game1", "game2"])
    mock_filter_data.assert_called_once()
    mock_delete_tables.assert_called_once_with("mock_connect", schema=exe_init_compet.config.landing_database_schema)
    mock_update_snowflake.assert_called_once()
    mock_update_calendar.assert_called_once()
    mock_terminate.assert_called_once()

if __name__ == "__main__":
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_exe_init_compet_happy_path))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
