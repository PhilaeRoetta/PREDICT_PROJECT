'''
This tests file concern all functions in the dropbox_actions module.
It units test the happy path for each function
'''
import pandas as pd
import unittest
from unittest.mock import patch, MagicMock
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import dropbox_actions


def test_copy_folder():
    # this test the function copy folder
    with patch("subprocess.run") as mock_run, \
         patch("config.dropbox_folder_root", "/root"), \
         patch("config.dropbox_folder", "/dropbox"):

        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="file1\nfile2"),  # rclone lsf
            MagicMock(returncode=0)  # rclone copy
        ]

        dropbox_actions.copy_folder("src", "dest")

        assert mock_run.call_count == 2
        args_lsf = mock_run.call_args_list[0][0][0]
        assert "lsf" in args_lsf

def test_initiate_folder():
    # this test the function initiate_folder
    with patch("dropbox_actions.copy_folder") as mock_copy:
        dropbox_actions.initiate_folder()

        # Expecting 3 calls with specific arguments
        expected_calls = [
            (("current", "-1"),),
            (("global_manual_inputs", "current/inputs/manual"),),
            (("local_manual_inputs", "current/inputs/manual"),)
        ]
        # Match kwargs too for the 2nd and 3rd calls
        mock_copy.assert_any_call("current", "-1")
        mock_copy.assert_any_call("global_manual_inputs", "current/inputs/manual", sourcepath_from_root=1, targetpath_from_root=0)
        mock_copy.assert_any_call("local_manual_inputs", "current/inputs/manual")

def test_download_file():
    # this test the function download_file
    with patch("os.path.exists", return_value=True), \
         patch("config.dropbox_folder", "/dropbox"), \
         patch("file_actions.read_txt", return_value="text"):

        result = dropbox_actions.download_file("file.txt", "/local")
        assert "str_file" in result

def test_upload_file():
    # this test the function upload_file
    df = pd.DataFrame([{"NAME": "myfile", "PATH": "folder/myfile.csv"}])
    with patch("pathlib.Path.stem", new_callable=MagicMock(return_value="myfile")), \
         patch("pathlib.Path.name", new_callable=MagicMock(return_value="myfile.csv")), \
         patch("pathlib.Path.suffix", new_callable=MagicMock(return_value=".csv")), \
         patch("config.dropbox_folder", "/dropbox"), \
         patch("config.rclone_config_path", "~/.rclone.conf"), \
         patch("subprocess.run") as mock_run:

        mock_run.return_value = MagicMock(returncode=0)

        dropbox_actions.upload_file(df, "/local/myfile.csv")
        assert mock_run.called

def test_download_folder():
    # this test the function download_folder
    df = pd.DataFrame([{"NAME": "input_folder", "PATH": "remote/input_folder"}])
    with patch("subprocess.run") as mock_run, \
         patch("config.dropbox_folder", "/dropbox"), \
         patch("config.multithreading_run") as mock_thread, \
         patch("file_actions.get_locally_from_dropbox"):

        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="file1.csv\nfile2.txt")  # rclone lsf
        ]

        dropbox_actions.download_folder("input_folder", df, "/local")
        mock_thread.assert_called_once()

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_copy_folder))
    test_suite.addTest(unittest.FunctionTestCase(test_initiate_folder))
    test_suite.addTest(unittest.FunctionTestCase(test_download_file))
    test_suite.addTest(unittest.FunctionTestCase(test_upload_file))
    test_suite.addTest(unittest.FunctionTestCase(test_download_folder))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
