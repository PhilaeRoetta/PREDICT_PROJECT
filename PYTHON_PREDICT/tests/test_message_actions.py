'''
This tests file concern all functions in the message_actions module.
It units test the happy path for each function
'''

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import sys
import os
from pandas.testing import assert_frame_equal
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import message_actions


@patch('message_actions.urllib.urlopen')
@patch.dict('message_actions.messages_info_functions', {'BI': MagicMock()})
def test_extract_messages_from_topic(mock_urlopen):
    # this test the function extract_messages_from_topic
    mock_response = MagicMock()
    mock_response.read.return_value = b"<html></html>"
    mock_urlopen.return_value = mock_response

    # Configure messages_info_functions mock
    df_mock = pd.DataFrame({
        'MESSAGE_FORUM_ID': [1, 2],
        'CREATION_TIME_LOCAL': pd.to_datetime(['2025-08-22 00:00:00', '2025-08-22 01:00:00']),
        'EDITION_TIME_LOCAL': pd.to_datetime(['2025-08-22 00:30:00', '2025-08-22 01:30:00'])
    })
    message_actions.messages_info_functions['BI'].return_value = df_mock

    # Mock topic row
    topic_row = pd.Series({
        'FORUM_SOURCE': 'BI',
        'FORUM_TIMEZONE': 'UTC',
        'TOPIC_NUMBER': 123
    })

    result = message_actions.extract_messages_from_topic(
        topic_row,
        pd.Timestamp('2025-08-22 00:00:00'),
        pd.Timestamp('2025-08-22 02:00:00')
    )

    assert_frame_equal(result.reset_index(drop=True), df_mock.reset_index(drop=True))

@patch('message_actions.snowflakeA.snowflake_execute')
def test_get_list_topics_from_need(mock_snowflake):
    # this test the function get_list_topics_from_need
    mock_snowflake.return_value = pd.DataFrame({
        'FORUM_SOURCE': ['BI'],
        'FORUM_TIMEZONE': ['UTC'],
        'TOPIC_NUMBER': [123]
    })
    
    sr_snowflake_account = pd.Series()
    sr_output_need = pd.Series({'SEASON_ID': '2025'})
    
    df_topics = message_actions.get_list_topics_from_need(sr_snowflake_account, sr_output_need)
    
    expected_df = pd.DataFrame({
        "FORUM_SOURCE": ["BI"],
        "FORUM_TIMEZONE": ["UTC"],
        "TOPIC_NUMBER": [123]
    })
    
    assert_frame_equal(df_topics.reset_index(drop=True), expected_df.reset_index(drop=True))

@patch('message_actions.snowflakeA.snowflake_execute')
def test_get_extraction_time_range(mock_snowflake):
    # this test the function get_extraction_time_range
    mock_snowflake.return_value = pd.DataFrame({
        'END_DATE_UTC': ['2025-08-22'],
        'END_TIME_UTC': ['03:00:00']
    })
    
    sr_snowflake_account = pd.Series()
    sr_output_need = pd.Series({
        'SEASON_ID': '2025',
        'GAMEDAY': '1',
        'LAST_MESSAGE_CHECK_TS_UTC': '2025-08-22 00:00:00'
    })
    
    min_ts, max_ts = message_actions.get_extraction_time_range(sr_snowflake_account, sr_output_need)
    assert min_ts == pd.Timestamp('2025-08-22 00:00:00')
    assert max_ts == pd.Timestamp('2025-08-22 03:00:00')

# Test post_message happy path
@patch("message_actions.config.exit_program", lambda f, **kwargs: f)
@patch("message_actions.config.retry_function", lambda f, **kwargs: f)
@patch("message_actions.requests.Session")
@patch("message_actions.os.getenv", side_effect=lambda k: "fake")
@patch("message_actions.time.sleep", return_value=None)
def test_post_message_bi(mock_sleep, mock_getenv, mock_session):
    # this test the function post_message

    topic_row = pd.Series({'FORUM_SOURCE': 'BI', 'TOPIC_NUMBER': 123})
    message_content = "Test message"

    # Mock session instance
    mock_sess_instance = MagicMock()
    mock_session.return_value.__enter__.return_value = mock_sess_instance

    # Mock login page GET
    login_html = '''
    <input name="sid" value="sid123">
    <input name="form_token" value="token123">
    <input name="creation_time" value="time123">
    <input name="topic_cur_post_id" value="topic123">
    Déconnexion
    '''
    mock_sess_instance.get.return_value.text = login_html

    # Mock POST responses (login and message post)
    mock_sess_instance.post.return_value.text = "Déconnexion"

    # Should not raise SystemExit
    message_actions.post_message(topic_row, message_content)

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_extract_messages_from_topic))
    test_suite.addTest(unittest.FunctionTestCase(test_get_list_topics_from_need))
    test_suite.addTest(unittest.FunctionTestCase(test_get_extraction_time_range))
    test_suite.addTest(unittest.FunctionTestCase(test_post_message_bi))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)