'''
This tests file concern all functions in the generate_output_need module.
It units test unhappy paths for each function
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

def test_create_output_need_auto_empty_calendar():
    # this test the function create_output_need_auto with an empty calendar. Must exit
    with patch.object(generate_output_need.calendarA, "get_calendar", return_value=pd.DataFrame(columns=["TS_TASK_UTC"])), \
         patch.object(generate_output_need.calendarA, "get_notrun_task", return_value=pd.DataFrame()):
        df_task_done = pd.DataFrame([{"TS_TASK_UTC": "2025-08-28T00:00:00Z"}])

        with unittest.TestCase().assertRaises(SystemExit):
            generate_output_need.create_output_need_auto(pd.Series(), df_task_done, "2025-08-28T00:00:00Z")

def test_create_output_need_auto_bad_timestamp():
    # this test the function create_output_need_auto with a not timestamp badly writen. Must exit

    calendar = pd.DataFrame([{
        "TASK_RUN": "RUN",
        "SEASON_ID": 1,
        "GAMEDAY": 10,
        "TS_TASK_UTC": pd.to_datetime("2025-08-28T00:00:00Z"),
        "MESSAGE_ACTION": "DO",
        "GAME_ACTION": "PLAY",
        "IS_TO_INIT": 0,
        "IS_TO_CALCULATE": 1,
        "IS_TO_RECALCULATE": 0,
        "IS_TO_DELETE": 0,
    }])

    with patch.object(generate_output_need.calendarA, "get_calendar", return_value=calendar), \
         patch.object(generate_output_need.calendarA, "get_notrun_task", return_value=pd.DataFrame()):
        with unittest.TestCase().assertRaises(SystemExit):
            generate_output_need.create_output_need_auto(pd.Series(), pd.DataFrame([{"TS_TASK_UTC": "not_a_date"}]), "not_a_date")

def test_generate_output_need_manual_path():
    # this test the function generate_output_need with a manual path
    os.environ["IS_OUTPUT_AUTO"] = "0"

    context = {
        "df_output_need_manual": pd.DataFrame([{
        "TASK_RUN": "RUN",
        "SEASON_ID": 1,
        "GAMEDAY": 10,
        "TS_TASK_UTC": pd.to_datetime("2025-08-28T00:00:00Z"),
        "MESSAGE_ACTION": "DO",
        "GAME_ACTION": "PLAY",
        "IS_TO_INIT": 0,
        "IS_TO_CALCULATE": 1,
        "IS_TO_RECALCULATE": 0,
        "IS_TO_DELETE": 0,
    }]),
        "df_message_check_ts": pd.DataFrame([{"SEASON_ID": 1, "LAST_CHECK_TS_UTC": "2025-08-28T00:00:00Z"}]),
    }

    with patch.object(generate_output_need, "create_csv", return_value=None):
        result = generate_output_need.generate_output_need(context)
        assert "LAST_MESSAGE_CHECK_TS_UTC" in result.index

def test_generate_output_need_auto_with_merge_failure():
    # this test the function generate_output_need with a merge removing all rows. Must exit when trying to do .iloc[0]
    
    os.environ["IS_OUTPUT_AUTO"] = "1"
    calendar = pd.DataFrame([{
        "TASK_RUN": "RUN",
        "SEASON_ID": 1,
        "GAMEDAY": 10,
        "TS_TASK_UTC": pd.to_datetime("2025-08-28T00:00:00Z"),
        "MESSAGE_ACTION": "DO",
        "GAME_ACTION": "PLAY",
        "IS_TO_INIT": 0,
        "IS_TO_CALCULATE": 1,
        "IS_TO_RECALCULATE": 0,
        "IS_TO_DELETE": 0,
    }])

    task_done = pd.DataFrame([{
        "TASK_RUN": "RUN",
        "SEASON_ID": 1,
        "GAMEDAY": 10,
        "TS_TASK_UTC": pd.to_datetime("2025-08-28T00:00:00Z"),
    }])

    with patch.object(generate_output_need, "create_output_need_auto", return_value=calendar), \
         patch.object(generate_output_need, "create_csv", return_value=None):
        context = {
            "sr_snowflake_account_connect": pd.Series(),
            "df_task_done": task_done,
            "str_current_run_time_utc": "2025-08-28T00:00:00Z",
            "df_message_check_ts": pd.DataFrame([{"SEASON_ID": 999, "LAST_CHECK_TS_UTC": "2025-08-28T00:00:00Z"}]),
        }

        with unittest.TestCase().assertRaises(SystemExit):
            generate_output_need.generate_output_need(context)

def test_set_output_need_to_check_status():
    # this test the function set_output_need_to_check_status
    """Should update sr_output_need fields and call create_csv."""
    sr = pd.DataFrame([{
        "TASK_RUN": "RUN",
        "SEASON_ID": 1,
        "GAMEDAY": 10,
        "TS_TASK_UTC": pd.to_datetime("2025-08-28T00:00:00Z"),
        "MESSAGE_ACTION": "DO",
        "GAME_ACTION": "PLAY",
        "IS_TO_INIT": 0,
        "IS_TO_CALCULATE": 1,
        "IS_TO_RECALCULATE": 0,
        "IS_TO_DELETE": 0,
    }]).iloc[0]

    with patch.object(generate_output_need, "create_csv", return_value=None), \
         patch.object(generate_output_need.config, "TASK_RUN_MAP", {"CHECK": "CHECK_RUN"}), \
         patch.object(generate_output_need.config, "MESSAGE_ACTION_MAP", {"CHECK": "CHECK_MSG"}), \
         patch.object(generate_output_need.config, "GAME_ACTION_MAP", {"AVOID": "NO_GAME"}):
        updated = generate_output_need.set_output_need_to_check_status(sr.copy())
        assert updated["TASK_RUN"] == "CHECK_RUN"
        assert updated["MESSAGE_ACTION"] == "CHECK_MSG"
        assert updated["GAME_ACTION"] == "NO_GAME"

def test_exit_program_decorator_triggers():
    # this ensure exit_program decorator calls sys_exit when function raises
    called = {}

    @generate_output_need.config.exit_program()
    def faulty(x):
        raise RuntimeError("boom")

    with patch.object(generate_output_need.config, "onError_final_execute", side_effect=lambda: called.setdefault("cleanup", True)), \
         patch.object(sys, "exit", side_effect=lambda code: called.setdefault("exit", code)):
        with unittest.TestCase().assertRaises(SystemExit):
            faulty(123)

    assert called["cleanup"] is True
            
if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_create_output_need_auto_empty_calendar))
    test_suite.addTest(unittest.FunctionTestCase(test_create_output_need_auto_bad_timestamp))
    test_suite.addTest(unittest.FunctionTestCase(test_generate_output_need_manual_path))
    test_suite.addTest(unittest.FunctionTestCase(test_generate_output_need_auto_with_merge_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_set_output_need_to_check_status))
    test_suite.addTest(unittest.FunctionTestCase(test_exit_program_decorator_triggers))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
