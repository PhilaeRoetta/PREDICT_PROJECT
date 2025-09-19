'''
This tests file concern all functions in the exe_main module.
It units test the happy path for each function
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

@patch("exe_main.gameA.get_list_games_from_need", return_value=[1, 2, 3])
@patch("exe_main.gameA.extract_games", return_value=pd.DataFrame({"game": [1, 2, 3]}))
@patch("exe_main.fileA.filter_data", return_value={"filtered": True})
def test_process_games(mock_filter, mock_extract, mock_get_list):
    # this test the process_games function
    context = {
        'sr_snowflake_account_connect': {},
        'sr_output_need': {},
        'df_paths': {}
    }
    result = exe_main.process_games(context)
    assert "games_scope_id" in result
    assert "df_game" in result
    assert result["filtered"] == True


@patch("exe_main.fileA.filter_data", return_value={"filtered": True})
@patch("exe_main.fileA.create_csv")
@patch("exe_main.set_output_need_to_check_status")
@patch("exe_main.extract_messages", return_value=(pd.DataFrame({"MESSAGE_CONTENT": ["hello", "*****test"]}), "2025-09-02T00:00:00Z"))
def test_process_messages(mock_extract_messages, mock_set_status, mock_create_csv, mock_filter):
    # this test the process_messages function
    context = {
        'sr_snowflake_account_connect': {},
        'sr_output_need': {'MESSAGE_ACTION': 'RUN'},
        'df_paths': {}
    }
    result = exe_main.process_messages(context)
    assert "df_message_check" in result
    assert "extraction_time_utc" in result


def test_display_check_string():
    # this test the display_check_string function
    context = {
        'sr_output_need': {
            'MESSAGE_ACTION': 'CHECK',
            'LAST_MESSAGE_CHECK_TS_UTC': '2025-09-01 00:00:00',
            'SEASON_ID': '2025'
        },
        'sr_snowflake_account_connect': {'DATABASE_PROD': 'DB'},
        'extraction_time_utc': '2025-09-02 00:00:00'
    }
    check_str = exe_main.display_check_string(context)
    assert check_str == f'''check messages at ==> 
;SELECT * FROM DB.CURATED.VW_MESSAGE_CHECKING WHERE SEASON_ID = '2025' AND EDITION_TIME_UTC between '2025-09-01 00:00:00' AND '2025-09-02 00:00:00'; 
If ok replace SEASON_ID 2025 check time with:
2025-09-02 00:00:00
'''


@patch("builtins.open", new_callable=unittest.mock.mock_open)
@patch("exe_main.update_calendar_related_files", return_value="2025-09-02T00:00:00Z")
@patch("exe_main.snowflakeA.update_snowflake")
@patch("exe_main.snowflakeA.delete_tables_data_from_python")
@patch("exe_main.outputA.generate_output_message")
@patch("exe_main.fileA.terminate_local_environment")
@patch("exe_main.fileA.download_needed_files")
@patch("exe_main.fileA.initiate_local_environment", return_value={
    'df_paths': {},
    'df_task_done': pd.DataFrame(),
    'sr_snowflake_account_connect': {}
})
@patch("exe_main.dropbox_initiate_folder")
@patch("exe_main.generate_output_need", return_value= pd.Series({
    'GAME_ACTION': 'RUN',
    'MESSAGE_ACTION': 'RUN',
    'TASK_RUN': 'INIT',
    'SEASON_ID': '2025'
}))
@patch("exe_main.process_games", side_effect=lambda ctx: {**ctx, 'games_scope_id': [1,2], 'df_game': pd.DataFrame()})
@patch("exe_main.process_messages", side_effect=lambda ctx: {**ctx, 'df_message_check': pd.DataFrame(), 'extraction_time_utc': '2025-09-02T00:00:00Z'})
def test_exe_main(
    mock_process_messages,
    mock_process_games,
    mock_generate_output_need,
    mock_dropbox,
    mock_initiate_local_env,
    mock_download_files,
    mock_terminate_env,
    mock_generate_output_message,
    mock_delete_tables,
    mock_update_snowflake,
    mock_update_calendar,
    mock_open
):
    # this test the exe_main function with all inputs mocked
    exe_main.exe_main()
    mock_open.assert_called_once_with("exe_output.json", "w")

if __name__ == "__main__":
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_process_games))
    test_suite.addTest(unittest.FunctionTestCase(test_process_messages))
    test_suite.addTest(unittest.FunctionTestCase(test_display_check_string))
    test_suite.addTest(unittest.FunctionTestCase(test_exe_main))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
