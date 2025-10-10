'''
This tests file concern all functions in the snowflake_actions module.
It units test unexpected path for each function
'''
import unittest
from unittest.mock import patch, MagicMock,call
import pandas as pd
import sys
import os
from pathlib import Path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import snowflake_actions

sr_account = pd.Series({
    'ACCOUNT': 'acc',
    'WAREHOUSE': 'wh',
    'DATABASE_PROD': 'prod',
    'DATABASE_TEST': 'test'
    })

df_paths = pd.DataFrame([
    {'NAME': 'landing_test_table', 'PYTHON_CATEGORY': ['INIT_COMPET'], 'DBT_CATEGORY': ['INIT_COMPET'], 'IS_ENCAPSULATED': 1}
])

def test_snowflake_connect_missing_env_vars():
    # this test the function snowflake_connect with SNOWFLAKE environment variables are missing
    with patch.dict(os.environ, {}, clear=True), \
         patch("snowflake_actions.snowflake.connector.connect", side_effect=Exception("bad connect")):
        try:
            snowflake_actions.snowflake_connect(sr_account)
        except SystemExit:
            pass  # exit_program triggers sys.exit

def test_snowflake_execute_select_path():
    # this test the function snowflake_execute select path
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.fetch_pandas_all.return_value = pd.DataFrame({"a": [1]})
    with patch("snowflake_actions.snowflake_connect") as mock_conn:
        mock_conn.return_value.cursor.return_value = mock_cursor
        df = snowflake_actions.snowflake_execute(sr_account, "select * from test")
        assert isinstance(df, pd.DataFrame)
        assert df["a"][0] == 1

def test_snowflake_execute_show_path():
    # this test the function snowflake_execute for show command
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("t1",)]
    with patch("snowflake_actions.snowflake_connect") as mock_conn:
        mock_conn.return_value.cursor.return_value = mock_cursor
        result = snowflake_actions.snowflake_execute(sr_account, "show tables")
        assert result == [("t1",)]

def test_snowflake_execute_script_empty_script():
    # this test the function snowflake_execute_script with empty script. Must exit program
    with patch("snowflake_actions.snowflake_connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        snowflake_actions.snowflake_execute_script(sr_account, "")
        mock_conn.execute_string.assert_called_once_with("")

def test_snowflake_execute_script_raises_error():
    # this test the function snowflake_execute_script with bad script. Must exit program
    with patch("snowflake_actions.snowflake_connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn  
        try:
            snowflake_actions.snowflake_execute_script(sr_account, "SELECT * FROM #DATABASE#.TABLE")
        except SystemExit:
            pass  # exit_program

def test_get_list_tables_to_update_main_with_calculation():
    # this test the function get_list_tables_to_update called by main with calculation needed
    df_paths_copy = df_paths.copy()
    df_paths_copy.loc[0, 'DBT_CATEGORY'] = ['CALCULATION']
    lst_python, lst_dbt = snowflake_actions.get_list_tables_to_update(
        "main", df_paths_copy, message_action="RUN", game_action=None, calculation_needed=True
    )
    assert "landing_output_need" in lst_python
    assert "landing_test_table" in lst_dbt

def test_delete_table_data_executes_expected_queries():
    # this test the function delete_table_data truncate and remove queries
    with patch("snowflake_actions.snowflake_execute") as mock_exec:
        snowflake_actions.delete_table_data(sr_account, "myschema", ["meta", "mytable"])
        calls = [call(sr_account, unittest.mock.ANY), call(sr_account, unittest.mock.ANY)]
        mock_exec.assert_has_calls(calls, any_order=False)

def test_update_snowflake_from_python_encapsulated():
    # this test the function update_snowflake_from_python with encapsulated data
    df_paths_copy = df_paths.copy()
    file_path = Path("landing_test_table.csv")
    with patch("snowflake_actions.snowflake_execute") as mock_exec, \
         patch("snowflake_actions.create_table_file") as mock_csv, \
         patch("os.path.join", return_value=str(file_path)), \
         patch("pathlib.Path.resolve", return_value=file_path):
        snowflake_actions.update_snowflake_from_python("main", sr_account, "landing_test_table", df_paths_copy, "folder")
        assert any("FIELD_OPTIONALLY_ENCLOSED_BY" in c[0][1] for c in mock_exec.call_args_list)

def test_update_snowflake_from_dbt_failure():
    # this test the function update_snowflake_from_dbt a failing dbt command
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 1
        mock_run.return_value.stdout = "bad"
        mock_run.return_value.stderr = "error"
        try:
            snowflake_actions.update_snowflake_from_dbt("main", sr_account, df_paths, ["tbl"])
        except SystemExit:
            pass  # exit_program

def test_update_snowflake_initsnowflake_runs_python_and_dbt():
    # this test the function update_snowflake called by initsnowflake for python and dbt
    with patch("os.listdir", return_value=["file1.csv"]), \
         patch("snowflake_actions.config.multithreading_run") as mock_mt, \
         patch("snowflake_actions.update_snowflake_from_dbt") as mock_dbt:
        snowflake_actions.update_snowflake("init_snowflake", {"sr_snowflake_account_connect": sr_account, "df_paths": df_paths}, "folder")
        assert mock_mt.called
        assert mock_dbt.called

def test_update_snowflake_main_with_empty_lists():
    # this test the function update_snowflake with empty tables list for python and dbt
    dd = {
        "df_paths": df_paths,
        "sr_snowflake_account_connect": sr_account,
        "sr_output_need": pd.Series({
            "MESSAGE_ACTION": None, "GAME_ACTION": None,
            "IS_TO_CALCULATE": 0, "IS_TO_DELETE": 0, "IS_TO_RECALCULATE": 0
        })
    }
    with patch("snowflake_actions.get_list_tables_to_update", return_value=([], [])), \
         patch("snowflake_actions.update_snowflake_from_dbt") as mock_dbt, \
         patch("snowflake_actions.config.multithreading_run") as mock_mt:
        snowflake_actions.update_snowflake("main", dd, "folder")
        assert not mock_dbt.called
        assert not mock_mt.called

if __name__ == "__main__":
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_snowflake_connect_missing_env_vars))
    test_suite.addTest(unittest.FunctionTestCase(test_snowflake_execute_select_path))
    test_suite.addTest(unittest.FunctionTestCase(test_snowflake_execute_script_empty_script))
    test_suite.addTest(unittest.FunctionTestCase(test_snowflake_execute_show_path))
    test_suite.addTest(unittest.FunctionTestCase(test_get_list_tables_to_update_main_with_calculation))
    test_suite.addTest(unittest.FunctionTestCase(test_snowflake_execute_script_raises_error))
    test_suite.addTest(unittest.FunctionTestCase(test_delete_table_data_executes_expected_queries))
    test_suite.addTest(unittest.FunctionTestCase(test_update_snowflake_from_python_encapsulated))
    test_suite.addTest(unittest.FunctionTestCase(test_update_snowflake_from_dbt_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_update_snowflake_initsnowflake_runs_python_and_dbt))
    test_suite.addTest(unittest.FunctionTestCase(test_update_snowflake_main_with_empty_lists))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)