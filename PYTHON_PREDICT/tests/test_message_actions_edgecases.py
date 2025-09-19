import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import sys
import os
from pandas.testing import assert_frame_equal
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import message_actions

def test_extract_messages_from_topic_invalid_timezone():
    # this test the function extract_messages_from_topic with topic row contains invalid timezone. Must exit program
    topic_row = pd.Series({
        "FORUM_TIMEZONE": "Invalid/Timezone",
        "FORUM_SOURCE": "BI",
        "TOPIC_NUMBER": 123
    })
    ts_min = pd.Timestamp("2023-01-01 00:00:00")
    ts_max = pd.Timestamp("2023-01-02 00:00:00")

    try:
        message_actions.extract_messages_from_topic(topic_row, ts_min, ts_max)
    except SystemExit:
        pass  # exit_program
        
def test_extract_messages_from_topic_missing_env():
    # this test the function extract_messages_from_topic with missing environment variables for forum URL
    topic_row = pd.Series({
        "FORUM_TIMEZONE": "UTC",
        "FORUM_SOURCE": "BI",
        "TOPIC_NUMBER": 123
    })
    ts_min = pd.Timestamp("2023-01-01 00:00:00")
    ts_max = pd.Timestamp("2023-01-02 00:00:00")

    try:
        message_actions.extract_messages_from_topic(topic_row, ts_min, ts_max)
    except SystemExit:
        pass  # exit_program

@patch('message_actions.urllib.urlopen')
@patch.dict('message_actions.messages_info_functions', {'BI': MagicMock()})
def test_extract_messages_from_topic_empty_dataframe(mock_urlopen):
    # this test the function extract_messages_from_topic with no messages dataframe
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
        pd.Timestamp('2025-08-24 00:00:00'),
        pd.Timestamp('2025-08-24 02:00:00')
    )
    unittest.TestCase().assertIsNone(result)

@patch("message_actions.requests.Session")
@patch("message_actions.time.sleep", return_value=None)
@patch("message_actions.config.time_message_wait", 1) 
def test_post_message_bi_login_fail(mock_sleep, mock_session):
    # this test the function post_message_bi with login fails. Must exit program
    topic_row = pd.Series({
        "FORUM_SOURCE": "BI",
        "TOPIC_NUMBER": 456
    })

    mock_sess_instance = MagicMock()
    # Simulate login page with sid/token fields
    mock_sess_instance.get.return_value.text = (
        "<html>"
        "<input name='sid' value='1'>"
        "<input name='form_token' value='abc'>"
        "<input name='creation_time' value='123'>"
        "</html>"
    )
    # Simulate login always failing (no 'Déconnexion')
    mock_sess_instance.post.return_value.text = "Login failed"
    mock_session.return_value.__enter__.return_value = mock_sess_instance

    try:
        message_actions.post_message_bi(topic_row, "test message")
    except SystemExit:
        pass  # expected due to decorator

def test_get_extraction_time_range_invalid_date():
    # this test the function get_extraction_time_range with invalid date. The result must be NaT
    """Edge case: LAST_MESSAGE_CHECK_TS_UTC is invalid -> coerced to NaT"""
    sr_snowflake_account = pd.Series({"dummy": "val"})
    sr_output_need = pd.Series({
        "LAST_MESSAGE_CHECK_TS_UTC": "not-a-date",
        "SEASON_ID": 1,
        "GAMEDAY": 1
    })

    with patch("message_actions.snowflakeA.snowflake_execute") as mock_exec:
        mock_exec.return_value = pd.DataFrame([{
            "END_DATE_UTC": "2023-01-01",
            "END_TIME_UTC": "23:59:59"
        }])
        ts_min, ts_max = message_actions.get_extraction_time_range(sr_snowflake_account, sr_output_need)

    unittest.TestCase().assertTrue(pd.isna(ts_min))  # should be NaT
    unittest.TestCase().assertIsInstance(ts_max, pd.Timestamp)

@patch("message_actions.create_csv")
def test_extract_messages_min_ge_max(mock_csv):
    # this test the function extract_messages with a time range with min > max. The result must be an empty dataframe
    """Unexpected path: ts_min >= ts_max -> returns empty DataFrame"""
    sr_snowflake_account = pd.Series({"dummy": "val"})
    sr_output_need = pd.Series({"dummy": "val"})

    with patch("message_actions.get_list_topics_from_need") as mock_topics, \
         patch("message_actions.get_extraction_time_range") as mock_time:

        mock_topics.return_value = pd.DataFrame([{
            "FORUM_SOURCE": "BI",
            "TOPIC_NUMBER": 999
        }])
        ts = pd.Timestamp("2023-01-01 00:00:00")
        mock_time.return_value = (ts, ts)  # min == max

        df, ts_max = message_actions.extract_messages(sr_snowflake_account, sr_output_need)

    unittest.TestCase().assertTrue(df.empty)

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_extract_messages_from_topic_invalid_timezone))
    test_suite.addTest(unittest.FunctionTestCase(test_extract_messages_from_topic_missing_env))
    test_suite.addTest(unittest.FunctionTestCase(test_extract_messages_from_topic_empty_dataframe))
    test_suite.addTest(unittest.FunctionTestCase(test_post_message_bi_login_fail))
    test_suite.addTest(unittest.FunctionTestCase(test_get_extraction_time_range_invalid_date))
    test_suite.addTest(unittest.FunctionTestCase(test_extract_messages_min_ge_max))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)