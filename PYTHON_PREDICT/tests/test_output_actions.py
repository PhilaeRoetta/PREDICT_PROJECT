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
    param_to_translate = 'hello world'
    country = 'FRANCE'
    translations = {'FRANCE': {'hello': 'bonjour'}}
    result = output_actions.translate_param_for_country(param_to_translate, country, translations)
    assert result == 'bonjour world'

def test_translate_param_df():
    
    # this test the function translate_param_for_country with a dataframe
    df = pd.DataFrame({'hello': [1, 2]})
    country = "FRANCE"
    translations = {'FRANCE': {'hello': 'bonjour'}}
    result = output_actions.translate_param_for_country(df, country, translations)
    expected = pd.DataFrame({'bonjour': [1, 2]})
    pd.testing.assert_frame_equal(result, expected)

def test_format_message():
    
    # this test the function format_message 
    message = "Line1|2|\nLine2"
    result = output_actions.format_message(message)
    expected = "Line1\n\nLine2"
    assert result == expected

def test_replace_conditionally_message_true():
    
    # this test the function replace_conditionally_message with a condition met
    output_text = "Hello [START]World[END]!"
    begin_tag = "[START]"
    end_tag = "[END]"
    condition = True
    result = output_actions.replace_conditionally_message(output_text, begin_tag, end_tag, condition)
    expected = "Hello World!"
    assert result == expected

def test_replace_conditionally_message_false():
    
    # this test the function replace_conditionally_message with a condition not met
    output_text = "Hello [START]World[END]!"
    begin_tag = "[START]"
    end_tag = "[END]"
    condition = False
    result = output_actions.replace_conditionally_message(output_text, begin_tag, end_tag, condition)
    expected = "Hello !"
    assert result == expected

def test_define_filename():

    # this test the function define_filename
    input_type = "forumoutput_inited"
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    extension = "txt"
    country = "FRANCE"
    result = output_actions.define_filename(input_type, sr_gameday_output_init, extension, country)
    expected = 'forumoutput_inited_s1_1erejournee_france.txt'
    assert result == expected

def test_display_rank():
    
    # this test the function display_rank
    df = pd.DataFrame({
        'Name': ['Alice', 'Bob', 'Charlie', 'David'],
        'RANK': [4, 2, 1, 2]
    })
    
    expected_df = pd.DataFrame({
        'RANK': [1, 2, '-', 4],
        'Name': ['Charlie', 'Bob', 'David', 'Alice']
    })

    result_df = output_actions.display_rank(df, 'RANK')
    
    pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_calculate_and_display_rank():
    
    # this test the function calculate_and_display_rank
    df = pd.DataFrame({'score':[10, 20, 20]})
    columns = ['score']
    result = output_actions.calculate_and_display_rank(df, columns)
    
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
    capture_name = "test_capture.jpg"

    with patch("output_actions.fileA.create_jpg"): 
        output_actions.capture_df_oneheader(df, capture_name)

def test_generate_output_message_init():
    
    # this test the function generate_output_message - with INIT task
    context_dict = {
        "sr_output_need": pd.read_csv("materials/output_need_init.csv").iloc[0],
        "sr_snowflake_account_connect": pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    }
    mock_df_gameday_output = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]

    with patch("output_actions.snowflake_execute", return_value= mock_df_gameday_output), \
         patch("output_actions.outputAI.process_output_message_inited") as mock_inited, \
         patch("output_actions.outputAC.process_output_message_calculated") as mock_calc:
        
        output_actions.generate_output_message(context_dict)
        mock_inited.assert_called_once()
        mock_calc.assert_not_called()

def test_generate_output_message_calculate():
    
    # this test the function generate_output_message -  with CALCULATE task
    context_dict = {
        "sr_output_need": pd.read_csv("materials/output_need_calculate.csv").iloc[0],
        "sr_snowflake_account_connect": pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    }
    mock_df_gameday_output = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]

    with patch("output_actions.snowflake_execute", return_value= mock_df_gameday_output), \
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
    test_suite.addTest(unittest.FunctionTestCase(test_generate_output_message_init))
    test_suite.addTest(unittest.FunctionTestCase(test_generate_output_message_calculate))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
