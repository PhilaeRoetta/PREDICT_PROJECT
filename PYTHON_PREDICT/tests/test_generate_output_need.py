'''
This tests file concern all functions in the generate_output_need module.
It units test the happy path for each function
'''

import unittest
from unittest.mock import patch
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import generate_output_need

def test_create_output_need_auto_returns_expected():
    # this test the function create_output_need_auto with fake inputs
    sr_snowflake_account = pd.Series({"account": "demo"})
    df_calendar = pd.DataFrame([{
        "TASK_RUN": "TASK1",
        "SEASON_ID": 1,
        "GAMEDAY": 1,
        "TS_TASK_UTC": "2025-08-21 12:00:00",
        "MESSAGE_ACTION": "RUN",
        "GAME_ACTION": "PLAY",
        "IS_TO_INIT": 1,
        "IS_TO_CALCULATE": 0,
        "IS_TO_RECALCULATE": 0,
        "IS_TO_DELETE": 0
    }])
    str_current_run_time_utc = "2025-08-21 12:00:00"

    df_task_done = pd.DataFrame([{
        "TASK_RUN": "TASK1",
        "SEASON_ID": 1,
        "GAMEDAY": 1,
        "TS_TASK_UTC": "2025-08-21 12:00:00"
    }])

    with patch("calendar_actions.get_calendar", return_value=df_calendar), \
         patch("calendar_actions.get_notrun_task", return_value=df_calendar):

        result = generate_output_need.create_output_need_auto(
            sr_snowflake_account, df_task_done, str_current_run_time_utc
        )
        expected_df = pd.DataFrame ({
            "TASK_RUN": ["TASK1"],
            "SEASON_ID": [1],
            "GAMEDAY": [1],
            "TS_TASK_UTC": ["2025-08-21 12:00:00"],
            "MESSAGE_ACTION": ["RUN"],
            "GAME_ACTION": ["PLAY"],
            "IS_TO_INIT": [1],
            "IS_TO_CALCULATE": [0],
            "IS_TO_RECALCULATE": [0],
            "IS_TO_DELETE": [0]
        })
        result['TS_TASK_UTC'] = pd.to_datetime(result['TS_TASK_UTC']).dt.tz_localize(None)
        expected_df['TS_TASK_UTC'] = pd.to_datetime(expected_df['TS_TASK_UTC']).dt.tz_localize(None)

    assert_frame_equal(result.reset_index(drop=True), expected_df.reset_index(drop=True),check_dtype=False)

def test_generate_output_need_auto_path():
    # this test the function generate_output_need with auto output_need and fake inputs
    df_task_done = pd.DataFrame([{
        "TASK_RUN": "TASK1",
        "SEASON_ID": 1,
        "GAMEDAY": 1,
        "TS_TASK_UTC": "2025-08-21 12:00:00"
    }])

    df_calendar = pd.DataFrame([{
        "TASK_RUN": "TASK1",
        "SEASON_ID": 1,
        "GAMEDAY": 1,
        "TS_TASK_UTC": "2025-08-21 12:00:00",
        "MESSAGE_ACTION": "RUN",
        "GAME_ACTION": "RUN",
        "IS_TO_INIT": 1,
        "IS_TO_CALCULATE": 0,
        "IS_TO_RECALCULATE": 0,
        "IS_TO_DELETE": 0
    }])
    df_check_ts = pd.DataFrame([{"SEASON_ID": 1, "LAST_CHECK_TS_UTC": "2025-08-20 12:00:00"}])
    sr_snowflake_account = pd.Series({"account": "demo"})

    data_dict = {
        "sr_snowflake_account_connect": sr_snowflake_account,
        "df_task_done": df_task_done,
        "str_current_run_time_utc": "2025-08-21 12:00:00",
        "df_message_check_ts": df_check_ts,
    }

    with patch("calendar_actions.get_calendar", return_value=df_calendar), \
        patch("calendar_actions.get_notrun_task", return_value=df_calendar), \
        patch("file_actions.create_csv") as mock_csv, \
        patch("os.getenv", return_value="1"), \
        patch("config.TMPF", "/tmp"), \
        patch("config.need_encapsulated", False):  # simulate AUTO mode

        result = generate_output_need.generate_output_need(data_dict)

        expected_sr = pd.Series({
            "TASK_RUN": "TASK1",
            "SEASON_ID": 1,
            "GAMEDAY": 1,
            "TS_TASK_UTC": "2025-08-21 12:00:00",
            "MESSAGE_ACTION": "RUN",
            "GAME_ACTION": "RUN",
            "IS_TO_INIT": 1,
            "IS_TO_CALCULATE": 0,
            "IS_TO_RECALCULATE": 0,
            "IS_TO_DELETE": 0,
            "LAST_MESSAGE_CHECK_TS_UTC": "2025-08-20 12:00:00"
        })
    result['TS_TASK_UTC'] = pd.to_datetime(result['TS_TASK_UTC']).tz_localize(None)
    expected_sr['TS_TASK_UTC'] = pd.to_datetime(expected_sr['TS_TASK_UTC']).tz_localize(None)

    assert_series_equal(result.reset_index(drop=True), expected_sr.reset_index(drop=True),check_dtype=False,check_names=False)

def test_set_output_need_to_check_status_updates_series():
    # this test the function set_output_need_to_check_status
    sr_output_need = pd.Series({
        "TASK_RUN": "TASK1",
        "SEASON_ID": 1,
        "GAMEDAY": 1,
        "TS_TASK_UTC": "2025-08-21 12:00:00",
        "MESSAGE_ACTION": "RUN",
        "GAME_ACTION": "RUN",
        "IS_TO_INIT": 1,
        "IS_TO_CALCULATE": 0,
        "IS_TO_RECALCULATE": 0,
        "IS_TO_DELETE": 0,
        "LAST_MESSAGE_CHECK_TS_UTC": "2025-08-20 12:00:00"
    })

    with patch("file_actions.create_csv") as mock_csv, \
         patch("config.TASK_RUN_MAP", {"CHECK": "CHECK"}), \
         patch("config.MESSAGE_ACTION_MAP", {"CHECK": "CHECK"}), \
         patch("config.GAME_ACTION_MAP", {"AVOID": "AVOID"}), \
         patch("config.TMPF", "/tmp"), \
         patch("config.need_encapsulated", False):

        result = generate_output_need.set_output_need_to_check_status(sr_output_need.copy())

    expected_sr = pd.Series({
        "TASK_RUN": "CHECK",
        "SEASON_ID": 1,
        "GAMEDAY": 1,
        "TS_TASK_UTC": "2025-08-21 12:00:00",
        "MESSAGE_ACTION": "CHECK",
        "GAME_ACTION": "AVOID",
        "IS_TO_INIT": 1,
        "IS_TO_CALCULATE": 0,
        "IS_TO_RECALCULATE": 0,
        "IS_TO_DELETE": 0,
        "LAST_MESSAGE_CHECK_TS_UTC": "2025-08-20 12:00:00"
        })
    
    result['TS_TASK_UTC'] = pd.to_datetime(result['TS_TASK_UTC']).tz_localize(None)
    expected_sr['TS_TASK_UTC'] = pd.to_datetime(expected_sr['TS_TASK_UTC']).tz_localize(None)

    assert_series_equal(result.reset_index(drop=True), expected_sr.reset_index(drop=True),check_dtype=False,check_names=False)

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_create_output_need_auto_returns_expected))
    test_suite.addTest(unittest.FunctionTestCase(test_generate_output_need_auto_path))
    test_suite.addTest(unittest.FunctionTestCase(test_set_output_need_to_check_status_updates_series))
    
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
