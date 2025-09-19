'''
This tests file concern all functions in the calendar_actions module.
It units test the unexpected path for each function
'''
import unittest
from unittest.mock import patch
import pandas as pd
from pandas.testing import assert_frame_equal
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from testutils import assertExit
import calendar_actions


def test_get_calendar_snowflake_execute_fails():
    # this test the function get_calendar with failed snowflake execution. Must exit the program
    with patch('calendar_actions.snowflake_execute', side_effect=RuntimeError("DB fail")):
        assertExit(lambda: calendar_actions.get_calendar(pd.DataFrame()))

def test_get_notrun_task_malformed_timestamps():
    # this test the function get_notrun_task with badly written timestamps. Must still return a result at this step

    df_calendar = pd.DataFrame([
        {"TASK_RUN": "UPDATEGAMES", "SEASON_ID": 'S1', "SEASON_SPORT": "BASKETBALL", "SEASON_COUNTRY": "FRANCE", "SEASON_NAME": "2024-2025",
        "SEASON_DIVISION": "PROB", "COMPETITION_ID": "RS", "GAMEDAY": '1ere journee', "TS_TASK_UTC": "not a timestamp",
        "TS_TASK_LOCAL": "2024-01-01 12:00:00", "IS_TO_INIT": 0, "IS_TO_CALCULATE": 0, "IS_tO_DELETE": 0, "IS_TO_RECALCULATE": 0,
        "MESSAGE_ACTION": "AVOID", "GAME_ACTION": "RUN"},
        
        {"TASK_RUN": "CHECK", "SEASON_ID": 'S1', "SEASON_SPORT": "BASKETBALL", "SEASON_COUNTRY": "FRANCE", "SEASON_NAME": "2024-2025",
        "SEASON_DIVISION": "PROB", "COMPETITION_ID": "RS", "GAMEDAY": '2eme journee', "TS_TASK_UTC": "not a timestamp neither",
        "TS_TASK_LOCAL": "2024-01-02 12:00:00", "IS_TO_INIT": 0, "IS_TO_CALCULATE": 0, "IS_tO_DELETE": 0, "IS_TO_RECALCULATE": 0,
        "MESSAGE_ACTION": "CHECK", "GAME_ACTION": "AVOID"}
    ])

    df_task_done = pd.DataFrame([
    {"TASK_RUN": "UPDATEGAMES", "SEASON_ID": 'S1', "GAMEDAY": '1ere journee', "TS_TASK_UTC": "again, not a timestamp"}
    ])

    expected_df = df_calendar.iloc[[1]]
    expected_df['TS_TASK_UTC'] = pd.to_datetime(expected_df['TS_TASK_UTC'], errors='coerce')
    result_df = calendar_actions.get_notrun_task(df_calendar, df_task_done)
    
    assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_update_nextrun_all_null_timestamps():
    # this test the function update_nextrun with null timestamps. Next run must be null
    df_calendar = pd.DataFrame([
        {"TASK_RUN": "UPDATEGAMES", "SEASON_ID": 'S1', "SEASON_SPORT": "BASKETBALL", "SEASON_COUNTRY": "FRANCE", "SEASON_NAME": "2024-2025",
        "SEASON_DIVISION": "PROB", "COMPETITION_ID": "RS", "GAMEDAY": '1ere journee', "TS_TASK_UTC": pd.NaT,
        "TS_TASK_LOCAL": "2024-01-01 12:00:00", "IS_TO_INIT": 0, "IS_TO_CALCULATE": 0, "IS_tO_DELETE": 0, "IS_TO_RECALCULATE": 0,
        "MESSAGE_ACTION": "AVOID", "GAME_ACTION": "RUN"},
        
        {"TASK_RUN": "CHECK", "SEASON_ID": 'S1', "SEASON_SPORT": "BASKETBALL", "SEASON_COUNTRY": "FRANCE", "SEASON_NAME": "2024-2025",
        "SEASON_DIVISION": "PROB", "COMPETITION_ID": "RS", "GAMEDAY": '2eme journee', "TS_TASK_UTC": pd.NaT,
        "TS_TASK_LOCAL": "2024-01-02 12:00:00", "IS_TO_INIT": 0, "IS_TO_CALCULATE": 0, "IS_tO_DELETE": 0, "IS_TO_RECALCULATE": 0,
        "MESSAGE_ACTION": "CHECK", "GAME_ACTION": "AVOID"}
    ])
    
    df_task_done = pd.DataFrame(columns=df_calendar.columns)

    with patch('calendar_actions.fileA.create_txt') as mock_create:
        result = calendar_actions.update_nextrun(df_calendar, df_task_done)
    assert result == "NONE"
    mock_create.assert_called_once()
    assert "next_run_time_utc.txt" in mock_create.call_args[0][0]

def test_add_task_to_taskdone_missing_column():
    # this test the function add_task_to_taskdone with columns non defined
    df_output_need = pd.DataFrame({'TASK_RUN': [1]})  # Missing SEASON_ID, GAMEDAY, TS_TASK_UTC...
    df_task_done = pd.DataFrame()
    assertExit(lambda: calendar_actions.add_task_to_taskdone(df_output_need, df_task_done))

def test_update_calendar_related_files_snowflake_returns_empty():
    # this test the function update_calendar_related_files with snowflake execution returning empty dataframe
    with patch('calendar_actions.get_calendar', return_value=pd.DataFrame(columns=['TASK_RUN','SEASON_ID','GAMEDAY','TS_TASK_UTC'])), \
         patch('calendar_actions.fileA.create_txt') as mock_txt:
        result = calendar_actions.update_calendar_related_files(
            called_by="worker", 
            sr_snowflake_account=pd.Series(),
            df_task_done=pd.DataFrame(columns=['TASK_RUN','SEASON_ID','GAMEDAY','TS_TASK_UTC'])
        )
    assert result == "NONE"
    mock_txt.assert_called_once()

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_get_calendar_snowflake_execute_fails))
    test_suite.addTest(unittest.FunctionTestCase(test_get_notrun_task_malformed_timestamps))
    test_suite.addTest(unittest.FunctionTestCase(test_update_nextrun_all_null_timestamps))
    test_suite.addTest(unittest.FunctionTestCase(test_add_task_to_taskdone_missing_column))
    test_suite.addTest(unittest.FunctionTestCase(test_update_calendar_related_files_snowflake_returns_empty))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)