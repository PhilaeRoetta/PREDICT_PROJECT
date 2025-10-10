'''
This tests file concern all functions in the exe_main module.
It units test the happy path for each function
'''
import unittest
from unittest.mock import patch
import pandas as pd
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import exe_main 

def test_process_games():
    # this test the process_games function
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
    game_scope_id = pd.DataFrame({
        "SEASON_ID": ["S1", "S1"],
        "COMPETITION_ID": ["RS", "RS"],
        "COMPETITION_SOURCE": ["LNB", "LNB"],
        "GAME_SOURCE_ID": [1, 2],
    })

    df_game = pd.DataFrame({
        "COMPETITION_SOURCE": ["LNB", "LNB"],
        "COMPETITION_ID": ["RS", "RS"],
        "SEASON_ID": ["S1", "S1"],
        "GAMEDAY":["1ere journee","1ere journee"],
        "DATE_GAME_LOCAL": ["2025-01-01", "2025-01-02"],
        "TIME_GAME_LOCAL": ["10:00:00", "10:00:02"],
        "TEAM_HOME": ["teamA", "teamC"],
        "SCORE_HOME": [0,0],
        "TEAM_AWAY": ["teamB", "teamD"],
        "SCORE_AWAY": [0,0],
        "GAME_SOURCE_ID": [1, 2]
    })

    with patch("exe_main.gameA.get_list_games_from_need",return_value=game_scope_id), \
        patch("exe_main.gameA.extract_games", return_value=df_game), \
        patch("exe_main.fileA.filter_data", return_value={"filtered": True}):

        result = exe_main.process_games(context)
        assert "games_scope_id" in result
        assert "df_game" in result
        assert result["filtered"] == True

def test_process_messages():
    # this test the process_messages function
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

    df_messages = pd.DataFrame({
        "FORUM_SOURCE": ["BI", "BI"],
        "TOPIC_NUMBER": [2,2],
        "USER": ["user1", "user2"],
        "MESSAGE_FORUM_ID":[1000,1001],
        "CREATION_TIME_LOCAL": ["2025-01-01 00:00:01", "2025-01-01 00:00:02"],
        "EDITION_TIME_LOCAL": ["", "2025-01-01 00:00:03"],
        "MESSAGE_CONTENT": ["aBc", "DeF"]
    })
    
    extraction_time_utc = '2025-01-01 18:00:00'

    with patch("exe_main.fileA.filter_data", return_value={"filtered": True}), \
        patch("exe_main.fileA.create_csv"), \
        patch("exe_main.set_output_need_to_check_status"), \
        patch("exe_main.extract_messages", return_value=(df_messages, extraction_time_utc)):

            result = exe_main.process_messages(context)
            assert "df_message_check" in result
            assert "extraction_time_utc" in result


def test_display_check_string():
    # this test the display_check_string function
    context = {
        'sr_snowflake_account_connect': pd.Series({
            'ACCOUNT': 'my_account',
            'WAREHOUSE': 'my_wh',
            'DATABASE_PROD': 'my_dbprod',
            'DATABASE_TEST': 'my_dbtest'
        }),
        'sr_output_need': pd.Series({
            "TASK_RUN": "CHECK", "SEASON_ID": "S1", "SEASON_SPORT": "BASKETBALL", "SEASON_COUNTRY": "FRANCE", "SEASON_NAME": "2025-2026",
            "SEASON_DIVISION": "ELITE2", "COMPETITION_ID": "RS", "GAMEDAY": "3ème journée", "TS_TASK_UTC": "2025-09-12 08:02:00",
            "TS_TASK_LOCAL": "2025-09-12 10:02:00", "IS_TO_INIT": 0, "IS_TO_CALCULATE": 0, "IS_tO_DELETE": 0, "IS_TO_RECALCULATE": 0,
            "MESSAGE_ACTION": "CHECK", "GAME_ACTION": "AVOID", "LAST_MESSAGE_CHECK_TS_UTC": "2025-06-14 15:00:00"
        }),
        'extraction_time_utc': '2025-09-02 00:00:00'
    }
    exe_main.display_check_string(context)

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
