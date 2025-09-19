'''
This tests file concern all functions in the snowflake_actions module.
It units test the happy path for each function
'''

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pandas.testing import assert_frame_equal
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import snowflake_actions

def test_snowflake_connect():
    # this test the function snowflake_connect
    sr_mock = pd.Series({
    'ACCOUNT': 'acc',
    'WAREHOUSE': 'wh',
    'DATABASE_PROD': 'prod',
    'DATABASE_TEST': 'test'
    })

    mock_conn = MagicMock()
    mock_conn.is_closed.return_value = False

    with patch('snowflake_actions.snowflake.connector.connect', return_value=mock_conn) as mock_connect, \
         patch('snowflake_actions.os.getenv', side_effect=lambda k: 'user' if k == 'SNOWFLAKE_USERNAME' else 'pass' if k == 'SNOWFLAKE_PASSWORD' else '0'):
        
        conn = snowflake_actions.snowflake_connect(sr_mock)

        assert conn == mock_conn
        mock_connect.assert_called_once()

def test_snowflake_execute():
    # this test the function snowflake_execute
    sr_mock = pd.Series({
    'ACCOUNT': 'acc',
    'WAREHOUSE': 'wh',
    'DATABASE_PROD': 'prod',
    'DATABASE_TEST': 'test'
    })

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.fetch_pandas_all.return_value = pd.DataFrame({"col": [1, 2]})
    
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.is_closed.return_value = False

    with patch('snowflake_actions.snowflake_connect', return_value=mock_conn), \
         patch('snowflake_actions.os.getenv', return_value='0'):

        result = snowflake_actions.snowflake_execute(sr_mock, "SELECT * FROM #DATABASE#.table")
        assert_frame_equal(result.reset_index(drop=True), pd.DataFrame({"col": [1, 2]}).reset_index(drop=True))

def test_snowflake_execute_script_uses_prod_db():
    # this test the function snowflake_execute_script with prod database
    with patch("os.getenv", return_value="0"):  # simulate prod run
        with patch("snowflake_actions.snowflake_connect") as mock_connect:

            sr_mock = pd.Series({
                'ACCOUNT': 'acc',
                'WAREHOUSE': 'wh',
                'DATABASE_PROD': 'prod',
                'DATABASE_TEST': 'test'
            })
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn

            script = "SELECT * FROM #DATABASE#.TABLE1;"
            expected_script = "SELECT * FROM prod.TABLE1;"

            snowflake_actions.snowflake_execute_script(sr_mock, script)

            mock_connect.assert_called_once_with(sr_mock)
            mock_conn.execute_string.assert_called_once_with(expected_script)

def test_snowflake_execute_script_uses_test_db():
    # this test the function snowflake_execute_script with test database
    with patch("os.getenv", return_value="1"):  # simulate test run
        with patch("snowflake_actions.snowflake_connect") as mock_connect:

            sr_mock = pd.Series({
                'ACCOUNT': 'acc',
                'WAREHOUSE': 'wh',
                'DATABASE_PROD': 'prod',
                'DATABASE_TEST': 'test'
            })

            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn

            script = "INSERT INTO #DATABASE#.TABLE2 VALUES (1);"
            expected_script = "INSERT INTO test.TABLE2 VALUES (1);"

            snowflake_actions.snowflake_execute_script(sr_mock, script)

            mock_connect.assert_called_once_with(sr_mock)
            mock_conn.execute_string.assert_called_once_with(expected_script)

def test_get_list_tables_to_update():
    # this test the function get_list_tables_to_update
    
    called_by = "main"
    message_action="check"

    df_paths = pd.DataFrame({
        'NAME': ['table1', 'table2'],
        'PYTHON_CATEGORY': [['MESSAGE_CHECK'], ['GAME_RUN']],
        'DBT_CATEGORY': [['MESSAGE_CHECK'], ['MESSAGE_RUN']]
    })

    result = snowflake_actions.get_list_tables_to_update(called_by,df_paths,message_action)
    assert result[0] == ['landing_output_need']
    assert result[1] == []

def test_delete_table_data():
    # this test the function delete_table_data
    sr_mock = pd.Series({
    'ACCOUNT': 'acc',
    'WAREHOUSE': 'wh',
    'DATABASE_PROD': 'prod',
    'DATABASE_TEST': 'test'
    })
    table_metadata = [None, "test_table"]

    with patch('snowflake_actions.snowflake_execute') as mock_exec:
        snowflake_actions.delete_table_data(sr_mock, "schema", table_metadata)
        assert mock_exec.call_count == 2
        args1 = mock_exec.call_args_list[0][0][1]
        assert "TRUNCATE TABLE" in args1

def test_create_table_file():
    # this test the function create_table_file
    sr_mock = pd.Series({
    'ACCOUNT': 'acc',
    'WAREHOUSE': 'wh',
    'DATABASE_PROD': 'prod',
    'DATABASE_TEST': 'test'
    })
    table = "schema_testtable"

    with patch('snowflake_actions.snowflake_execute', return_value=pd.DataFrame({'col': [1]})), \
         patch('snowflake_actions.create_csv') as mock_create_csv, \
         patch('snowflake_actions.os.path.join', return_value='/tmp/table.csv'), \
         patch('snowflake_actions.config.TMPD', '/tmp'):

        snowflake_actions.create_table_file(sr_mock, table, 1)
        mock_create_csv.assert_called_once()

def test_update_snowflake_from_dbt():
    # this test the function update_snowflake_from_dbt
    sr_mock = pd.Series({
    'ACCOUNT': 'acc',
    'WAREHOUSE': 'wh',
    'DATABASE_PROD': 'prod',
    'DATABASE_TEST': 'test'
    })
    df_paths = pd.DataFrame({
        'NAME': ['table1'],
        'IS_ENCAPSULATED': [1]
    })

    with patch('snowflake_actions.subprocess.run') as mock_run, \
         patch('snowflake_actions.create_table_file'), \
         patch('snowflake_actions.config.dbt_directory', '/fake/dbt'), \
         patch('snowflake_actions.os.environ', {'DBT_PROFILES_DIR': '/fake/dbt'}):

        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "Success"
        mock_run.return_value.stderr = ""

        snowflake_actions.update_snowflake_from_dbt(
            called_by="main",
            sr_snowflake_account=sr_mock,
            df_paths=df_paths,
            lst_dbt_tables=["table1"]
        )

        mock_run.assert_called_once()

if __name__ == "__main__":
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_snowflake_connect))
    test_suite.addTest(unittest.FunctionTestCase(test_snowflake_execute))
    test_suite.addTest(unittest.FunctionTestCase(test_snowflake_execute_script_uses_prod_db))
    test_suite.addTest(unittest.FunctionTestCase(test_snowflake_execute_script_uses_test_db))
    test_suite.addTest(unittest.FunctionTestCase(test_get_list_tables_to_update))
    test_suite.addTest(unittest.FunctionTestCase(test_delete_table_data))
    test_suite.addTest(unittest.FunctionTestCase(test_create_table_file))
    test_suite.addTest(unittest.FunctionTestCase(test_update_snowflake_from_dbt))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)