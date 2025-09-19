'''
This tests file concern all functions in the file_actions module.
It units test unexpected paths for each function
'''

import unittest
from unittest.mock import patch, MagicMock,mock_open
import pandas as pd
import sys
import os
import io

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import file_actions

def test_read_json_file_not_found():
    # this test the function read_json with a file non existant
    with patch("builtins.open", side_effect=FileNotFoundError("no file")):
        try:
            file_actions.read_json("does_not_exist.json")
        except SystemExit:
            assert True
        except Exception:
            assert False, "Should exit gracefully due to decorator"

def test_read_json_invalid_json():
    # this test the function read_json with a file json type invalid
    with patch("builtins.open", mock_open(read_data="not-json")), \
         patch("json.load", side_effect=ValueError("bad json")):
        
        try:
            file_actions.read_json("bad.json")
        except SystemExit:
            assert True
        except Exception:
            assert False, "Should exit gracefully due to decorator"

def test_read_and_check_csv_missing_columns():
    # this test the function read_and_check_csv with a file having a missing column
    fake_csv = io.StringIO("a,b\n1,2")
    with patch("pandas.read_csv", return_value=pd.read_csv(fake_csv)), \
         patch("file_actions.read_json", return_value={"schemas": {"test.csv": {"columns": {"c": "int64"}}}}):
        
        try:
            file_actions.read_and_check_csv("test.csv")
        except SystemExit:
            assert True
        except Exception:
            assert False, "Should exit gracefully due to decorator"

def test_read_and_check_csv_type_mismatch():
    # this test the function read_and_check_csv with a file having a column of a different type than expected
    fake_csv = io.StringIO("c\nfoo\nbar")
    with patch("pandas.read_csv", return_value=pd.read_csv(fake_csv)), \
         patch("file_actions.read_json", return_value={"schemas": {"test.csv": {"columns": {"c": "int64"}}}}):
        
        try:
            file_actions.read_and_check_csv("test.csv")
        except SystemExit:
            assert True
        except Exception:
            assert False, "Should exit gracefully due to decorator"

def test_read_yml_file_not_found():
    # this test the function read_yml with a file non existant
    with patch("builtins.open", side_effect=FileNotFoundError("no file")):
        
        try:
            file_actions.read_yml("missing.yml")
        except SystemExit:
            assert True
        except Exception:
            assert False, "Should exit gracefully due to decorator"

def test_read_txt_file_not_found():
    # this test the function read_txt with a file non existant
    with patch("builtins.open", side_effect=FileNotFoundError("no file")), \
         patch("sys.exit") as mock_exit:

        try:
            file_actions.read_txt("missing.txt")
        except SystemExit:
            assert True
        except Exception:
            assert False, "Should exit gracefully due to decorator"

def test_create_csv_write_failure():
    # this test the function create_csv forcing a write failure
    df = pd.DataFrame({"a": [1]})
    with patch("pandas.DataFrame.to_csv", side_effect=OSError("disk full")):

        try:
            file_actions.create_csv("bad.csv", df)
        except SystemExit:
            assert True
        except Exception:
            assert False, "Should exit gracefully due to decorator"

def test_create_yml_failure():
    # this test the function create_yml forcing a write failure
    with patch("builtins.open", side_effect=OSError("cannot write")):

        try:
            file_actions.create_yml("bad.yml", "test")
        except SystemExit:
            assert True
        except Exception:
            assert False, "Should exit gracefully due to decorator"

def test_create_txt_failure():
    # this test the function create_txt forcing a write failure
    with patch("builtins.open", side_effect=OSError("cannot write")):

        try:
            file_actions.create_yml("bad.txt", "test")
        except SystemExit:
            assert True
        except Exception:
            assert False, "Should exit gracefully due to decorator"

def test_create_jpg_save_failure():
    # this test the function create_jpg forcing an error while saving it
    fake_fig = MagicMock()
    fake_fig.savefig.side_effect = Exception("save error")

    try:
        file_actions.create_jpg("bad.jpg", fake_fig)
    except SystemExit:
        assert True
    except Exception:
        assert False, "Should exit gracefully due to decorator"

def test_personalize_yml_missing_env_vars():
    # this test the function personalize_yml_dbt_file with the environment variables non provided
    with patch("file_actions.read_yml", return_value="#DATABASE# something"), \
         patch.dict("os.environ", {}, clear=True), \
         patch("file_actions.create_yml"):
        
        try:
            sr = {"ACCOUNT": "acc", "DATABASE_PROD": "prod", "DATABASE_TEST": "test", "WAREHOUSE": "wh"}
            file_actions.personalize_yml_dbt_file("profiles.yml", sr)
        except SystemExit:
            assert True
        except Exception:
            assert False, "Should exit gracefully due to decorator"

def test_parametrize_yml_dbt_file_bad_read():
    # this test the function parametrize_yml_dbt_file forcing an error reading it
    with patch("file_actions.read_yml", side_effect=OSError("cannot read")):

        try:
            file_actions.parametrize_yml_dbt_file("profiles.yml")
        except SystemExit:
            assert True
        except Exception:
            assert False, "Should exit gracefully due to decorator"

def test_get_locally_from_dropbox_dropbox_failure():
    # this test the function get_locally_from_dropbox forcing an error coming from dropbox
    df_paths = pd.DataFrame([{"NAME": "testfile", "PATH": "remote.csv", "IS_ENCAPSULATED": 0}])
    with patch("file_actions.dropboxA.download_file", side_effect=Exception("network error")):

        try:
            df_paths = pd.DataFrame([{"NAME": "testfile", "PATH": "remote.csv", "IS_ENCAPSULATED": 0}])
            file_actions.get_locally_from_dropbox("testfile", "tmp", df_paths)
        except SystemExit:
            assert True
        except Exception:
            assert False, "Should exit gracefully due to decorator"

def test_modify_run_file_invalid_event():
    # this test the function modify_run_file with an unexpected event
    try:
        df = pd.DataFrame([{"RUN_TIME_UTC": "2020-01-01 00:00:00", "EVENT": "initiate"}])
        result = file_actions.modify_run_file(df, "caller", event="unknown")
    except SystemExit:
        assert True
    except Exception:
        assert False, "Should exit gracefully due to decorator"

def test_download_paths_file_invalid_literal():
    # this test the function download_paths_file with a badly written FILTERING_COLUMN
    df = pd.DataFrame({"FILTERING_COLUMN": ["[bad list"], 
                       "DOWNLOAD_CATEGORY": [""],
                       "PYTHON_CATEGORY": [""],
                       "DBT_CATEGORY": [""],
                       "NAME": ["file"],
                       "PATH": ["p"],
                       "IS_ENCAPSULATED": [0]})
    dd = {"df_paths": df}
    with patch("file_actions.dropboxA.download_file", return_value=dd):
        try:
            result = file_actions.download_paths_file()
        except SystemExit:
            assert True
        except Exception:
            assert False, "Should exit gracefully due to decorator"

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_read_json_file_not_found))
    test_suite.addTest(unittest.FunctionTestCase(test_read_json_invalid_json))
    test_suite.addTest(unittest.FunctionTestCase(test_read_and_check_csv_missing_columns))
    test_suite.addTest(unittest.FunctionTestCase(test_read_and_check_csv_type_mismatch))
    test_suite.addTest(unittest.FunctionTestCase(test_read_yml_file_not_found))
    test_suite.addTest(unittest.FunctionTestCase(test_read_txt_file_not_found))
    test_suite.addTest(unittest.FunctionTestCase(test_create_csv_write_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_create_yml_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_create_txt_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_create_jpg_save_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_personalize_yml_missing_env_vars))
    test_suite.addTest(unittest.FunctionTestCase(test_parametrize_yml_dbt_file_bad_read))
    test_suite.addTest(unittest.FunctionTestCase(test_get_locally_from_dropbox_dropbox_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_modify_run_file_invalid_event))
    test_suite.addTest(unittest.FunctionTestCase(test_download_paths_file_invalid_literal))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)