'''
This tests file concern all functions in the calendar_actions module.
It units test the happy path for each function
'''
import unittest
from unittest.mock import patch
import pandas as pd
from pandas.testing import assert_frame_equal
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import calendar_actions

@patch("calendar_actions.config.exit_program", lambda **kwargs: (lambda f: f))
def test_get_calendar():
    # this test the function get_calendar
    
    df_mock_snowflake_execute = pd.DataFrame([
        {"TASK_RUN": "UPDATEGAMES", "SEASON_ID": 'S1', "SEASON_SPORT": "BASKETBALL", "SEASON_COUNTRY": "FRANCE", "SEASON_NAME": "2024-2025",
        "SEASON_DIVISION": "PROB", "COMPETITION_ID": "RS", "GAMEDAY": '1ere journee', "TS_TASK_UTC": "2024-01-01 10:00:00",
        "TS_TASK_LOCAL": "2024-01-01 12:00:00", "IS_TO_INIT": 0, "IS_TO_CALCULATE": 0, "IS_tO_DELETE": 0, "IS_TO_RECALCULATE": 0,
        "MESSAGE_ACTION": "AVOID", "GAME_ACTION": "RUN"},
        
        {"TASK_RUN": "CHECK", "SEASON_ID": 'S1', "SEASON_SPORT": "BASKETBALL", "SEASON_COUNTRY": "FRANCE", "SEASON_NAME": "2024-2025",
        "SEASON_DIVISION": "PROB", "COMPETITION_ID": "RS", "GAMEDAY": '2eme journee', "TS_TASK_UTC": "2024-01-02 10:00:00",
        "TS_TASK_LOCAL": "2024-01-02 12:00:00", "IS_TO_INIT": 0, "IS_TO_CALCULATE": 0, "IS_tO_DELETE": 0, "IS_TO_RECALCULATE": 0,
        "MESSAGE_ACTION": "CHECK", "GAME_ACTION": "AVOID"}
    ])

    sr_snowflake_account = pd.Series({
        'ACCOUNT': 'my_account',
        'WAREHOUSE': 'my_wh',
        'DATABASE_PROD': 'my_dbprod',
        'DATABASE_TEST': 'my_dbtest'
    })

    with patch("calendar_actions.snowflake_execute", return_value=df_mock_snowflake_execute):
        expected_df = df_mock_snowflake_execute
        result_df = calendar_actions.get_calendar(sr_snowflake_account)
    # Assert the returned DataFrame is equal to expected
    pd.testing.assert_frame_equal(result_df, expected_df)

def test_get_notrun_task():
    # this test the function get_notrun_task

    df_calendar = pd.DataFrame([
        {"TASK_RUN": "UPDATEGAMES", "SEASON_ID": 'S1', "SEASON_SPORT": "BASKETBALL", "SEASON_COUNTRY": "FRANCE", "SEASON_NAME": "2024-2025",
        "SEASON_DIVISION": "PROB", "COMPETITION_ID": "RS", "GAMEDAY": '1ere journee', "TS_TASK_UTC": "2024-01-01 10:00:00",
        "TS_TASK_LOCAL": "2024-01-01 12:00:00", "IS_TO_INIT": 0, "IS_TO_CALCULATE": 0, "IS_tO_DELETE": 0, "IS_TO_RECALCULATE": 0,
        "MESSAGE_ACTION": "AVOID", "GAME_ACTION": "RUN"},
        
        {"TASK_RUN": "CHECK", "SEASON_ID": 'S1', "SEASON_SPORT": "BASKETBALL", "SEASON_COUNTRY": "FRANCE", "SEASON_NAME": "2024-2025",
        "SEASON_DIVISION": "PROB", "COMPETITION_ID": "RS", "GAMEDAY": '2eme journee', "TS_TASK_UTC": "2024-01-02 10:00:00",
        "TS_TASK_LOCAL": "2024-01-02 12:00:00", "IS_TO_INIT": 0, "IS_TO_CALCULATE": 0, "IS_tO_DELETE": 0, "IS_TO_RECALCULATE": 0,
        "MESSAGE_ACTION": "CHECK", "GAME_ACTION": "AVOID"}
    ])

    df_task_done = pd.DataFrame([
    {"TASK_RUN": "UPDATEGAMES", "SEASON_ID": 'S1', "GAMEDAY": '1ere journee', "TS_TASK_UTC": "2024-01-01 10:00:00"}
    ])

    expected_df = df_calendar.iloc[[1]]
    expected_df['TS_TASK_UTC'] = pd.to_datetime(expected_df['TS_TASK_UTC'], errors='coerce')

    result_df = calendar_actions.get_notrun_task(df_calendar.copy(), df_task_done.copy())
    assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))

@patch("calendar_actions.fileA.create_txt")
def test_update_nextrun(mock_create_txt):
    # this test the function update_nextrun

    df_calendar = pd.DataFrame([
        {"TASK_RUN": "UPDATEGAMES", "SEASON_ID": 'S1', "SEASON_SPORT": "BASKETBALL", "SEASON_COUNTRY": "FRANCE", "SEASON_NAME": "2024-2025",
        "SEASON_DIVISION": "PROB", "COMPETITION_ID": "RS", "GAMEDAY": '1ere journee', "TS_TASK_UTC": "2024-01-01 10:00:00",
        "TS_TASK_LOCAL": "2024-01-01 12:00:00", "IS_TO_INIT": 0, "IS_TO_CALCULATE": 0, "IS_tO_DELETE": 0, "IS_TO_RECALCULATE": 0,
        "MESSAGE_ACTION": "AVOID", "GAME_ACTION": "RUN"},
        
        {"TASK_RUN": "CHECK", "SEASON_ID": 'S1', "SEASON_SPORT": "BASKETBALL", "SEASON_COUNTRY": "FRANCE", "SEASON_NAME": "2024-2025",
        "SEASON_DIVISION": "PROB", "COMPETITION_ID": "RS", "GAMEDAY": '2eme journee', "TS_TASK_UTC": "2024-01-02 10:00:00",
        "TS_TASK_LOCAL": "2024-01-02 12:00:00", "IS_TO_INIT": 0, "IS_TO_CALCULATE": 0, "IS_tO_DELETE": 0, "IS_TO_RECALCULATE": 0,
        "MESSAGE_ACTION": "CHECK", "GAME_ACTION": "AVOID"}
    ])

    df_task_done = pd.DataFrame([
    {"TASK_RUN": "UPDATEGAMES", "SEASON_ID": 'S1', "GAMEDAY": '1ere journee', "TS_TASK_UTC": "2024-01-01 10:00:00"}
    ])

    result = calendar_actions.update_nextrun(df_calendar.copy(), df_task_done.copy())
    assert result == "2024-01-02 10:00:00.000"
    mock_create_txt.assert_called_once()

@patch("calendar_actions.fileA.create_csv")
def test_add_task_to_taskdone(mock_create_csv):
    # this test the function add_task_to_taskdone

    sr_output_need = pd.Series({
        "TASK_RUN": "UPDATEGAMES", "SEASON_ID": "S1", "SEASON_SPORT": "BASKETBALL", "SEASON_COUNTRY": "FRANCE", "SEASON_NAME": "2025-2026",
        "SEASON_DIVISION": "ELITE2", "COMPETITION_ID": "RS", "GAMEDAY": "3ème journée", "TS_TASK_UTC": "2025-09-12 08:02:00",
        "TS_TASK_LOCAL": "2025-09-12 10:02:00", "IS_TO_INIT": 0, "IS_TO_CALCULATE": 0, "IS_tO_DELETE": 0, "IS_TO_RECALCULATE": 0,
        "MESSAGE_ACTION": "AVOID", "GAME_ACTION": "RUN", "LAST_MESSAGE_CHECK_TS_UTC": "2025-06-14 15:00:00"
    })

    df_task_done = pd.DataFrame([
        {"TASK_RUN": "UPDATEGAMES", "SEASON_ID": 'S1', "GAMEDAY": '1ere journee', "TS_TASK_UTC": "2024-01-01 10:00:00"}
    ])

    expected_df = pd.DataFrame([
        {"TASK_RUN": "UPDATEGAMES", "SEASON_ID": 'S1', "GAMEDAY": "3ème journée", "TS_TASK_UTC": "2025-09-12 08:02:00"},
        {"TASK_RUN": "UPDATEGAMES", "SEASON_ID": 'S1', "GAMEDAY": '1ere journee', "TS_TASK_UTC": "2024-01-01 10:00:00"}
    ])
    result = calendar_actions.add_task_to_taskdone(sr_output_need.copy(), df_task_done.copy())
    assert_frame_equal(result.reset_index(drop=True), expected_df.reset_index(drop=True))
    mock_create_csv.assert_called_once()


@patch("calendar_actions.get_calendar")
@patch("calendar_actions.fileA.create_txt")
@patch("calendar_actions.fileA.create_csv")
def test_update_calendar_related_files_main(mock_csv, mock_txt, mock_get_calendar):
    # this test the function update_calendar_related_files from caller exe_main
    
    df_calendar = pd.DataFrame([
        {"TASK_RUN": "UPDATEGAMES", "SEASON_ID": 'S1', "SEASON_SPORT": "BASKETBALL", "SEASON_COUNTRY": "FRANCE", "SEASON_NAME": "2024-2025",
        "SEASON_DIVISION": "PROB", "COMPETITION_ID": "RS", "GAMEDAY": '1ere journee', "TS_TASK_UTC": "2024-01-01 10:00:00",
        "TS_TASK_LOCAL": "2024-01-01 12:00:00", "IS_TO_INIT": 0, "IS_TO_CALCULATE": 0, "IS_tO_DELETE": 0, "IS_TO_RECALCULATE": 0,
        "MESSAGE_ACTION": "AVOID", "GAME_ACTION": "RUN"},
        
        {"TASK_RUN": "CHECK", "SEASON_ID": 'S1', "SEASON_SPORT": "BASKETBALL", "SEASON_COUNTRY": "FRANCE", "SEASON_NAME": "2024-2025",
        "SEASON_DIVISION": "PROB", "COMPETITION_ID": "RS", "GAMEDAY": '2eme journee', "TS_TASK_UTC": "2024-01-02 10:00:00",
        "TS_TASK_LOCAL": "2024-01-02 12:00:00", "IS_TO_INIT": 0, "IS_TO_CALCULATE": 0, "IS_tO_DELETE": 0, "IS_TO_RECALCULATE": 0,
        "MESSAGE_ACTION": "CHECK", "GAME_ACTION": "AVOID"}
    ])

    sr_snowflake_account = pd.Series({
        'ACCOUNT': 'my_account',
        'WAREHOUSE': 'my_wh',
        'DATABASE_PROD': 'my_dbprod',
        'DATABASE_TEST': 'my_dbtest'
    })

    sr_output_need = pd.Series({
        "TASK_RUN": "UPDATEGAMES", "SEASON_ID": "S1", "SEASON_SPORT": "BASKETBALL", "SEASON_COUNTRY": "FRANCE", "SEASON_NAME": "2025-2026",
        "SEASON_DIVISION": "ELITE2", "COMPETITION_ID": "RS", "GAMEDAY": "3ème journée", "TS_TASK_UTC": "2025-09-12 08:02:00",
        "TS_TASK_LOCAL": "2025-09-12 10:02:00", "IS_TO_INIT": 0, "IS_TO_CALCULATE": 0, "IS_tO_DELETE": 0, "IS_TO_RECALCULATE": 0,
        "MESSAGE_ACTION": "AVOID", "GAME_ACTION": "RUN", "LAST_MESSAGE_CHECK_TS_UTC": "2025-06-14 15:00:00"
    })

    df_task_done = pd.DataFrame([
        {"TASK_RUN": "UPDATEGAMES", "SEASON_ID": 'S1', "GAMEDAY": '1ere journee', "TS_TASK_UTC": "2024-01-01 10:00:00"}
    ])

    mock_get_calendar.return_value = df_calendar.copy()
    result = calendar_actions.update_calendar_related_files("main", sr_snowflake_account, df_task_done.copy(), sr_output_need.copy())
    assert result == "2024-01-02 10:00:00.000"


@patch("calendar_actions.get_calendar")
@patch("calendar_actions.fileA.create_txt")
def test_update_calendar_related_files_compet(mock_txt, mock_get_calendar):
    # this test the function update_calendar_related_files

    df_calendar = pd.DataFrame([
        {"TASK_RUN": "UPDATEGAMES", "SEASON_ID": 'S1', "SEASON_SPORT": "BASKETBALL", "SEASON_COUNTRY": "FRANCE", "SEASON_NAME": "2024-2025",
        "SEASON_DIVISION": "PROB", "COMPETITION_ID": "RS", "GAMEDAY": '1ere journee', "TS_TASK_UTC": "2024-01-01 10:00:00",
        "TS_TASK_LOCAL": "2024-01-01 12:00:00", "IS_TO_INIT": 0, "IS_TO_CALCULATE": 0, "IS_tO_DELETE": 0, "IS_TO_RECALCULATE": 0,
        "MESSAGE_ACTION": "AVOID", "GAME_ACTION": "RUN"},
        
        {"TASK_RUN": "CHECK", "SEASON_ID": 'S1', "SEASON_SPORT": "BASKETBALL", "SEASON_COUNTRY": "FRANCE", "SEASON_NAME": "2024-2025",
        "SEASON_DIVISION": "PROB", "COMPETITION_ID": "RS", "GAMEDAY": '2eme journee', "TS_TASK_UTC": "2024-01-02 10:00:00",
        "TS_TASK_LOCAL": "2024-01-02 12:00:00", "IS_TO_INIT": 0, "IS_TO_CALCULATE": 0, "IS_tO_DELETE": 0, "IS_TO_RECALCULATE": 0,
        "MESSAGE_ACTION": "CHECK", "GAME_ACTION": "AVOID"}
    ])

    sr_snowflake_account = pd.Series({
        'ACCOUNT': 'my_account',
        'WAREHOUSE': 'my_wh',
        'DATABASE_PROD': 'my_dbprod',
        'DATABASE_TEST': 'my_dbtest'
    })

    df_task_done = pd.DataFrame([
        {"TASK_RUN": "UPDATEGAMES", "SEASON_ID": 'S1', "GAMEDAY": '1ere journee', "TS_TASK_UTC": "2024-01-01 10:00:00"}
    ])

    mock_get_calendar.return_value = df_calendar
    result = calendar_actions.update_calendar_related_files("init_compet", sr_snowflake_account, df_task_done.copy())
    assert result == "2024-01-02 10:00:00.000"


if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_get_calendar))
    test_suite.addTest(unittest.FunctionTestCase(test_get_notrun_task))
    test_suite.addTest(unittest.FunctionTestCase(test_update_nextrun))
    test_suite.addTest(unittest.FunctionTestCase(test_add_task_to_taskdone))
    test_suite.addTest(unittest.FunctionTestCase(test_update_calendar_related_files_main))
    test_suite.addTest(unittest.FunctionTestCase(test_update_calendar_related_files_compet))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)