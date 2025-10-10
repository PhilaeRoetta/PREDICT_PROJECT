'''
This tests file concern all functions in the config module.
It units test the happy path for each function
'''

import unittest
import tempfile
import shutil
from unittest.mock import patch
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import config

def test_onError_final_execute_parametrizes_file():
    # this test function onError_final_execute
    with tempfile.TemporaryDirectory() as tmpdir:
        fake_file = os.path.join(tmpdir, "profiles.yml")
        with open(fake_file, "w", encoding="utf-8") as f:
            f.write("account: myacc\ndatabase: mydb\npassword: mypwd\nuser: me\n")

        with patch("config.dbt_profiles_path", fake_file):
            config.onError_final_execute()

        with open(fake_file, "r", encoding="utf-8") as f:
            content = f.read()
        assert content == f"""account: #ACCOUNT#
database: #DATABASE#
password: #PASSWORD#
user: #USER#
"""

def test_exit_program_decorator_exits():
    # this test decorator exit_program
    @config.exit_program()
    def faulty():
        raise ValueError("boom")

    with patch("config.sys_exit") as mock_exit:
        faulty()
        mock_exit.assert_called_once_with(1)

def test_retry_function_eventually_succeeds():
    # this test decorator retry_function
    calls = {"n": 0}

    @config.retry_function(max_attempts=3, delay_secs=0)
    def sometimes_fails():
        calls["n"] += 1
        if calls["n"] < 2:
            raise ValueError("fail once")
        return "ok"

    result = sometimes_fails()
    assert result == "ok"
    assert calls["n"] == 2

def test_retry_function_fails_after_max_attempts():
    # this test decorator retry_function eventually failing
    @config.retry_function(max_attempts=2, delay_secs=0)
    def always_fails():
        raise ValueError("nope")

    with unittest.TestCase().assertRaises(ValueError):
        always_fails()

def test_raise_issue_to_caller_reraises():
    # this test decorator raise_issue_to_caller
    @config.raise_issue_to_caller()
    def bad():
        raise RuntimeError("problem")

    with unittest.TestCase().assertRaises(RuntimeError):
        bad()

def test_create_and_destroy_local_folder():
    # this test functions create_local_folder and destroy_local_folder
    config.create_local_folder()
    assert os.path.exists(config.TMPF)
    assert os.path.exists(config.TMPD)

    config.destroy_local_folder()
    assert not os.path.exists(config.TMPF)
    assert not os.path.exists(config.TMPD)

def test_multithreading_run_executes_in_parallel():
    # this test function multithreading_run with a function a+b
    def add(a, b):
        return a + b

    args = [(1, 2), (3, 4), (5, 6)]
    results = config.multithreading_run(add, args, thread_max_workers=2)
    assert sorted(results) == [3, 7, 11]


if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_onError_final_execute_parametrizes_file))
    test_suite.addTest(unittest.FunctionTestCase(test_exit_program_decorator_exits))
    test_suite.addTest(unittest.FunctionTestCase(test_retry_function_eventually_succeeds))
    test_suite.addTest(unittest.FunctionTestCase(test_retry_function_fails_after_max_attempts))
    test_suite.addTest(unittest.FunctionTestCase(test_raise_issue_to_caller_reraises))
    test_suite.addTest(unittest.FunctionTestCase(test_create_and_destroy_local_folder))
    test_suite.addTest(unittest.FunctionTestCase(test_multithreading_run_executes_in_parallel))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
