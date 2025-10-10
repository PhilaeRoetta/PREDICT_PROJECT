'''
This tests file concern all functions in the file_actions module.
It units test the happy path for each function
'''

import unittest
from unittest.mock import patch, MagicMock,mock_open
from pandas.testing import assert_frame_equal
import pandas as pd
import json
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import file_actions

# Example JSON schema for read_and_check_csv
mock_schema = {
    "schemas": {
        "test.csv": {
            "columns": {"col1": "int64", "col2": "object"}
        }
    }
}

def test_read_json():
    # this test the function read_json
    test_data = {"key": "value"}
    with patch("builtins.open", mock_open(read_data=json.dumps(test_data))):
        result = file_actions.read_json("dummy.json")
    unittest.TestCase().assertEqual(result, test_data)

def test_read_and_check_csv():
    # this test the function read_and_check_csv
    df_csv = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    with patch("file_actions.read_json", return_value=mock_schema):
        with patch("pandas.read_csv", return_value=df_csv):
            df_result = file_actions.read_and_check_csv("test.csv")
    unittest.TestCase().assertTrue(isinstance(df_result, pd.DataFrame))

def test_read_yml():
    # this test the function read_yml
    content = "key: value"
    with patch("builtins.open", mock_open(read_data=content)):
        result = file_actions.read_yml("dummy.yml")
    unittest.TestCase().assertEqual(result, content)

def test_read_txt():
    # this test the function read_txt
    content = "hello world"
    with patch("builtins.open", mock_open(read_data=content)):
        result = file_actions.read_txt("dummy.txt")
    unittest.TestCase().assertEqual(result, content)

def test_create_csv():
    # this test the function create_csv
    df = pd.DataFrame({"a": [1], "b": [2]})
    m = MagicMock()
    with patch.object(df, "to_csv", m):
        file_actions.create_csv("dummy.csv", df)
    unittest.TestCase().assertTrue(m.called)

def test_create_yml():
    # this test the function create_yml
    m = mock_open()
    with patch("builtins.open", m):
        file_actions.create_yml("dummy.yml", "text")
    m().write.assert_called_once_with("text")

def test_create_txt():
    # this test the function create_txt
    m = mock_open()
    with patch("builtins.open", m):
        file_actions.create_txt("dummy.txt", "text")
    m().write.assert_called_once_with("text")

def test_personalize_yml_dbt_file():
    # this test the function personalize_yml_dbt_file
    with patch("file_actions.read_yml", return_value="#ACCOUNT# #DATABASE#"), \
         patch("file_actions.create_yml") as mock_create_yml, \
         patch.dict(os.environ, {"SNOWFLAKE_USERNAME":"u","SNOWFLAKE_PASSWORD":"p","IS_TESTRUN":"0"}):
        file_actions.personalize_yml_dbt_file("dummy.yml", {"ACCOUNT":"acc","DATABASE_PROD":"db","DATABASE_TEST":"dbt","WAREHOUSE":"w"})
    unittest.TestCase().assertTrue(mock_create_yml.called)

def test_parametrize_yml_dbt_file():
    # this test the function parametrize yml dbt file
    with patch("file_actions.read_yml", return_value="account: A\ndatabase: B\nuser: U\npassword: P"), \
         patch("file_actions.create_yml") as mock_create_yml:
        file_actions.parametrize_yml_dbt_file("dummy.yml")
    unittest.TestCase().assertTrue(mock_create_yml.called)

def test_filter_data():
    # this test the function filter_data with two dependant files
    df_paths = pd.DataFrame({
        "NAME": ["df1", "df2"],
        "FILTERING_CATEGORY": ["cat", "cat"],
        "FILTERING_FILE": ["", "df1"],
        "FILTERING_COLUMN": ["col", "col"],
        "IS_FOR_UPLOAD": [0, 0]
    })
    data_dict = {
        "df_df1": pd.DataFrame({"col": [1, 2]}),
        "df_df2": pd.DataFrame({"col": [2, 3]})
    }
    expected = {
        "df_df1": pd.DataFrame({"col": [1, 2]}),
        "df_df2": pd.DataFrame({"col": [2]})
    }
    with patch("file_actions.create_csv") as mock_create_csv:
        result = file_actions.filter_data(data_dict, df_paths, "cat")
    
    assert_frame_equal(result["df_df1"].reset_index(drop=True), expected["df_df1"].reset_index(drop=True))
    assert_frame_equal(result["df_df2"].reset_index(drop=True), expected["df_df2"].reset_index(drop=True))

def test_get_locally_from_dropbox():
    # this test the function get_locally_from_dropbox
    df_paths = pd.DataFrame({"NAME": ["file1"], "PATH": ["p"], "IS_ENCAPSULATED":[0]})
    with patch("file_actions.dropboxA.download_file", return_value={"df_file1": pd.DataFrame()}):
        result = file_actions.get_locally_from_dropbox("file1", "folder", df_paths)
    unittest.TestCase().assertIn("df_file1", result)

def test_modify_run_file():
    # this test the function modify_run_file
    df_run = pd.DataFrame(columns=["RUN_TIME_UTC","EVENT","RUN_TYPE","RUN_METHOD","OUTPUT_AUTO","PLANNED_RUN_TIME_UTC"])
    with patch("file_actions.create_csv") as mock_create_csv:
        df_mod = file_actions.modify_run_file(df_run,"caller","initiate")
    print(df_mod)
    unittest.TestCase().assertTrue("RUN_TIME_UTC" in df_mod.columns)
    df_mod_to_test = df_mod[['EVENT','RUN_TYPE','RUN_METHOD','OUTPUT_AUTO','PLANNED_RUN_TIME_UTC']]
    expected_df = pd.DataFrame({
        'EVENT': ['initiate'],
        'RUN_TYPE': ['caller'],
        'RUN_METHOD': [None],
        'OUTPUT_AUTO': [1],
        'PLANNED_RUN_TIME_UTC': [None]
    })
    
    assert_frame_equal(df_mod_to_test.reset_index(drop=True), expected_df.reset_index(drop=True),check_dtype=False)

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_read_json))
    test_suite.addTest(unittest.FunctionTestCase(test_read_and_check_csv))
    test_suite.addTest(unittest.FunctionTestCase(test_read_yml))
    test_suite.addTest(unittest.FunctionTestCase(test_read_txt))
    test_suite.addTest(unittest.FunctionTestCase(test_create_csv))
    test_suite.addTest(unittest.FunctionTestCase(test_create_yml))
    test_suite.addTest(unittest.FunctionTestCase(test_create_txt))
    test_suite.addTest(unittest.FunctionTestCase(test_personalize_yml_dbt_file))
    test_suite.addTest(unittest.FunctionTestCase(test_parametrize_yml_dbt_file))
    test_suite.addTest(unittest.FunctionTestCase(test_filter_data))
    test_suite.addTest(unittest.FunctionTestCase(test_get_locally_from_dropbox))
    test_suite.addTest(unittest.FunctionTestCase(test_modify_run_file))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)

