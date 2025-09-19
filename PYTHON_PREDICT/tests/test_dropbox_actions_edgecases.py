'''
This tests file concern all functions in the dropbox_actions module.
It units test the unexpected path for each function
'''
import unittest
from unittest.mock import patch
import subprocess
import pandas as pd
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import dropbox_actions
from testutils import assertExit


def test_copy_folder_empty_source_or_target():
    # this test the function copy_folder with empty sources and target. Must be accepted
    with patch('dropbox_actions.config.dropbox_folder', 'dropbox/'), \
         patch('dropbox_actions.config.dropbox_folder_root', 'dropbox_root/'), \
         patch('dropbox_actions.subprocess.run') as mock_run:
        # Simulate success for both commands
        mock_run.side_effect = [
            subprocess.CompletedProcess(args=[], returncode=0, stdout="file1.txt\n", stderr=""),
            subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
        ]
        dropbox_actions.copy_folder('', '')

def test_copy_folder_fail_lsf_command():
    # this test the function copy_folder with a failed rclone command. Must exit the program
    with patch('dropbox_actions.config.dropbox_folder', 'dropbox/'), \
         patch('dropbox_actions.config.dropbox_folder_root', 'dropbox_root/'), \
         patch('dropbox_actions.subprocess.run') as mock_run:
        
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=1, stderr="lsf failed", stdout="")
        assertExit(lambda: dropbox_actions.copy_folder('source', 'target'))

def test_download_file_already_exists():
    # this test the function download_file with a file already existing
    with patch('dropbox_actions.config.dropbox_folder', 'dropbox/'), \
         patch('dropbox_actions.config.rclone_config_path', 'config.conf'), \
         patch('dropbox_actions.fileA.read_txt', return_value="mocked_content"), \
         patch('os.path.exists', return_value=True):
        result = dropbox_actions.download_file('test.txt', 'local_folder')
        assert result['str_test'] == "mocked_content"

def test_download_file_rclone_failure():
    # this test the function download_file with a failed rclone command. Must exit the program
    with patch('dropbox_actions.config.dropbox_folder', 'dropbox/'), \
         patch('dropbox_actions.config.rclone_config_path', 'config.conf'), \
         patch('os.path.exists', return_value=False), \
         patch('dropbox_actions.subprocess.run', return_value=subprocess.CompletedProcess(args=[], returncode=1, stderr="rclone error", stdout="")):
        
        assertExit(lambda: dropbox_actions.download_file('test.txt', 'local_folder'))

        
def test_upload_file_folder_path_edge():
    # this test the function upload_file with a path badly written
    with patch('dropbox_actions.config.dropbox_folder', 'dropbox/'), \
         patch('dropbox_actions.config.rclone_config_path', 'config.conf'), \
         patch('dropbox_actions.subprocess.run', return_value=subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")):
        # Should handle path ending with '/'
        dropbox_actions.upload_file('local/file.txt', 'target/folder/')

def test_upload_file_fail():
    # this test the function upload_file with a rclone command failing. Must exit the program
    with patch('dropbox_actions.config.dropbox_folder', 'dropbox/'), \
         patch('dropbox_actions.config.rclone_config_path', 'config.conf'), \
         patch('dropbox_actions.subprocess.run', return_value=subprocess.CompletedProcess(args=[], returncode=1, stderr="upload fail", stdout="")):

        assertExit(lambda: dropbox_actions.upload_file('local/file.txt', 'target/folder/'))
        
def test_download_folder_empty_listing():
    # this test the function download_folder with an empty folder. Must be accepted but without nothing getting locally
    df_paths = pd.DataFrame([{"NAME": "folderA", "PATH": "folderApath"}])
    with patch('dropbox_actions.config.dropbox_folder', 'dropbox/'), \
         patch('dropbox_actions.subprocess.run', return_value=subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")), \
         patch('dropbox_actions.config.multithreading_run') as mock_multi:
        dropbox_actions.download_folder("folderA", df_paths, "local_folder")
        mock_multi.assert_called_with(dropbox_actions.fileA.get_locally_from_dropbox, [])

def test_download_folder_listing_error():
    # this test the function download_folder with a rclone command failing for listing files. Ñust exit the program
    df_paths = pd.DataFrame([{"NAME": "folderA", "PATH": "folderApath"}])
    with patch('dropbox_actions.config.dropbox_folder', 'dropbox/'), \
         patch('dropbox_actions.subprocess.run', return_value=subprocess.CompletedProcess(args=[], returncode=1, stderr="lsf error", stdout="")):
        
        assertExit(lambda: dropbox_actions.download_folder("folderA", df_paths, "local_folder"))
        
if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_copy_folder_empty_source_or_target))
    test_suite.addTest(unittest.FunctionTestCase(test_copy_folder_fail_lsf_command))
    test_suite.addTest(unittest.FunctionTestCase(test_download_file_already_exists))
    test_suite.addTest(unittest.FunctionTestCase(test_download_file_rclone_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_upload_file_folder_path_edge))
    test_suite.addTest(unittest.FunctionTestCase(test_upload_file_fail))
    test_suite.addTest(unittest.FunctionTestCase(test_download_folder_empty_listing))
    test_suite.addTest(unittest.FunctionTestCase(test_download_folder_listing_error))
    
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
