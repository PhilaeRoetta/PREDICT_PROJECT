'''
This tests file concern all functions in the output_actions module.
It units test the happy path for each function
'''
import unittest
from unittest.mock import patch
import pandas as pd
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import output_actions

def test_translate_param_string():
    # this test the function translate_param_for_country with a string
    translations = {'FR': {'hello': 'bonjour'}}
    result = output_actions.translate_param_for_country('hello world', 'FR', translations)
    assert result == 'bonjour world'

def test_translate_param_df():
    # this test the function translate_param_for_country with a dataframe
    df = pd.DataFrame({'hello': [1, 2]})
    translations = {'FR': {'hello': 'bonjour'}}
    result = output_actions.translate_param_for_country(df, 'FR', translations)
    expected = pd.DataFrame({'bonjour': [1, 2]})
    pd.testing.assert_frame_equal(result, expected)

def test_format_message():
    # this test the function format_message 
    msg = "Line1|2|\nLine2"
    result = output_actions.format_message(msg)
    expected = "Line1\n\nLine2"
    assert result == expected

def test_replace_conditionally_message_true():
    # this test the function replace_conditionally_message with a condition met
    text = "Hello [START]World[END]!"
    result = output_actions.replace_conditionally_message(text, "[START]", "[END]", True)
    expected = "Hello World!"
    assert result == expected

def test_replace_conditionally_message_false():
    # this test the function replace_conditionally_message with a condition not met
    text = "Hello [START]World[END]!"
    result = output_actions.replace_conditionally_message(text, "[START]", "[END]", False)
    expected = "Hello !"
    assert result == expected

def test_define_filename():
    # this test the function define_filename
    sr_gameday_output_init = pd.Series({'SEASON_ID': 'S1', 'GAMEDAY': '10eme journee'})
    result = output_actions.define_filename('forumoutput', sr_gameday_output_init, 'txt', country='FR')
    expected = 'forumoutput_s1_10emejournee_fr.txt'
    assert result == expected

def test_display_rank():
    # this test the function display_rank
    df = pd.DataFrame({
        'Name': ['Alice', 'Bob', 'Charlie', 'David'],
        'RANK': [1, 2, 2, 4]
    })
    
    expected_df = pd.DataFrame({
        'RANK': [1, 2, '-', 4],
        'Name': ['Alice', 'Bob', 'Charlie', 'David']
    })

    result_df = output_actions.display_rank(df, 'RANK')
    
    # Convert to same type to compare
    pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_calculate_and_display_rank():
    # this test the function calculate_and_display_rank

    df = pd.DataFrame({'score':[10, 20, 20]})
    result = output_actions.calculate_and_display_rank(df, ['score'])
    result['RANK'] = result['RANK'].astype(str)
    expected_df = pd.DataFrame({
        'RANK': ['1','-','3'],
        'score': [20,20,10]
    })
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_capture_df_oneheader():
    # this test the function capture_df_oneheader
    df = pd.DataFrame({
        "col1": [1, 2],
        "col2": [3, 4]
    })

    with patch("output_actions.fileA.create_jpg") as mock_create_jpg, \
         patch("output_actions.config.TMPF", "/tmp"):
        
        output_actions.capture_df_oneheader(df, "test_capture.jpg")
        
        # Ensure create_jpg is called with correct args
        mock_create_jpg.assert_called_once()
        args, kwargs = mock_create_jpg.call_args
        assert args[0] == os.path.join("/tmp", "test_capture.jpg")
        assert args[1] is not None   # should be a matplotlib figure

def test_generate_output_message_init_and_calc():
    # this test the function generate_output_message - first with INIT task, then with CALCULATE

    context_dict = {
        "sr_output_need": pd.Series({
            "TASK_RUN": "INIT", "SEASON_ID": 'S1', "SEASON_SPORT": "BASKETBALL", "SEASON_COUNTRY": "FRANCE", "SEASON_NAME": "2024-2025",
            "SEASON_DIVISION": "PROB", "COMPETITION_ID": "RS", "GAMEDAY": '1ere journee', "TS_TASK_UTC": "2024-01-01 10:00:00",
            "TS_TASK_LOCAL": "2024-01-01 12:00:00", "IS_TO_INIT": 1, "IS_TO_CALCULATE": 0, "IS_TO_DELETE": 0, "IS_TO_RECALCULATE": 0,
            "MESSAGE_ACTION": "AVOID", "GAME_ACTION": "RUN"
        }),
        "sr_snowflake_account_connect" : pd.Series({
            'ACCOUNT': 'my_account',
            'WAREHOUSE': 'my_wh',
            'DATABASE_PROD': 'my_dbprod',
            'DATABASE_TEST': 'my_dbtest'
        })
    }

    fake_df_gameday_output = pd.DataFrame([{"some": "data"}])

    TASK_RUN_MAP = {
        "UPDATEGAMES": "UPDATEGAMES",
        "CHECK": "CHECK",
        "CALCULATE": "CALCULATE",
        "INIT": "INIT"
    }

    with patch("output_actions.sqlQ.qGamedayOutput", "FAKE QUERY"), \
         patch("output_actions.snowflake_execute", return_value= fake_df_gameday_output), \
         patch("output_actions.config.TASK_RUN_MAP", TASK_RUN_MAP), \
         patch("output_actions.outputAI.process_output_message_inited") as mock_inited, \
         patch("output_actions.outputAC.process_output_message_calculated") as mock_calc:

        output_actions.generate_output_message(context_dict)

        mock_inited.assert_called_once()
        mock_calc.assert_not_called()

    # Now test CALCULATE + delete flag
    context_dict = {
        "sr_output_need": pd.Series({
            "TASK_RUN": "CALCULATE", "SEASON_ID": 'S1', "SEASON_SPORT": "BASKETBALL", "SEASON_COUNTRY": "FRANCE", "SEASON_NAME": "2024-2025",
            "SEASON_DIVISION": "PROB", "COMPETITION_ID": "RS", "GAMEDAY": '1ere journee', "TS_TASK_UTC": "2024-01-01 10:00:00",
            "TS_TASK_LOCAL": "2024-01-01 12:00:00", "IS_TO_INIT": 0, "IS_TO_CALCULATE": 0, "IS_TO_DELETE": 1, "IS_TO_RECALCULATE": 0,
            "MESSAGE_ACTION": "AVOID", "GAME_ACTION": "RUN"
        }),
        "sr_snowflake_account_connect" : pd.Series({
            'ACCOUNT': 'my_account',
            'WAREHOUSE': 'my_wh',
            'DATABASE_PROD': 'my_dbprod',
            'DATABASE_TEST': 'my_dbtest'
        })
    }

    with patch("output_actions.sqlQ.qGamedayOutput", "FAKE QUERY"), \
         patch("output_actions.snowflake_execute", return_value=fake_df_gameday_output), \
         patch("output_actions.config.TASK_RUN_MAP", TASK_RUN_MAP), \
         patch("output_actions.outputAI.process_output_message_inited") as mock_inited, \
         patch("output_actions.outputAC.process_output_message_calculated") as mock_calc:

        output_actions.generate_output_message(context_dict)

        mock_calc.assert_called_once()
        mock_inited.assert_not_called()

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_translate_param_string))
    test_suite.addTest(unittest.FunctionTestCase(test_translate_param_df))
    test_suite.addTest(unittest.FunctionTestCase(test_format_message))
    test_suite.addTest(unittest.FunctionTestCase(test_replace_conditionally_message_true))
    test_suite.addTest(unittest.FunctionTestCase(test_replace_conditionally_message_false))
    test_suite.addTest(unittest.FunctionTestCase(test_define_filename))
    test_suite.addTest(unittest.FunctionTestCase(test_display_rank))
    test_suite.addTest(unittest.FunctionTestCase(test_calculate_and_display_rank))
    test_suite.addTest(unittest.FunctionTestCase(test_capture_df_oneheader))
    test_suite.addTest(unittest.FunctionTestCase(test_generate_output_message_init_and_calc))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
