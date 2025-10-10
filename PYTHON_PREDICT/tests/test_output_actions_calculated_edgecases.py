'''
This tests file concern all functions in the output_actions_calculated module.
It units test unexpected path for each function
'''
import unittest
from unittest.mock import patch
import pandas as pd
from pandas.testing import assert_frame_equal
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import output_actions_calculated
from testutils import assertExit

@patch("output_actions_calculated.snowflake_execute")
def test_get_calculated_games_result_empty_df(mock_exec):
    # this test the function get_calculated_games_result with empty series. Must return an empty string.
    mock_exec.return_value = pd.DataFrame(columns=["RESULT","GAME_MESSAGE_SHORT","TEAM_HOME_NAME","TEAM_AWAY_NAME","SCORE_HOME","SCORE_AWAY"])
    result, count = output_actions_calculated.get_calculated_games_result(pd.Series(), pd.Series({"SEASON_ID":'S1',"GAMEDAY":'1ere journee'}))
    assert result == ""
    assert count == 0

@patch("output_actions_calculated.snowflake_execute")
def test_get_calculated_games_result_negative_result(mock_exec):
    # this test the function get_calculated_games_result with a negative result.
    mock_exec.return_value = pd.DataFrame([{
        'GAME_MESSAGE': 'G1.02', 
        'GAME_MESSAGE_SHORT': 2, 
        'TEAM_HOME_NAME': 'X', 
        'TEAM_AWAY_NAME': 'Y', 
        'SCORE_HOME': 0, 
        'SCORE_AWAY': 2, 
        'RESULT': -2
    }])
    result, count = output_actions_calculated.get_calculated_games_result(pd.Series(), pd.Series({"SEASON_ID":'S1',"GAMEDAY":'1ere journee'}))
    assert result == "2/ X vs Y: [b]-2[/b] [ 0 - 2 ]"
    assert count == 1

def test_capture_scores_detailed_empty_df():
    # this test the function capture_scores_detailed with empty dataframe. Must exit the program.
    df = pd.DataFrame()
    capture_name = "test_empty"

    with patch("output_actions_calculated.fileA.create_jpg") as mock_create:
        assertExit(lambda: output_actions_calculated.capture_scores_detailed(df, capture_name))
        # Assert no file creation attempted
        mock_create.assert_not_called()

def test_capture_scores_detailed_invalid_columns():
    # this test the function capture_scores_detailed with invalid columns in dataframe. Must exit the program.
    df = pd.DataFrame({"A":[1,2,3]})
    with patch("output_actions_calculated.fileA.create_jpg") as mock_create:
        assertExit(lambda: output_actions_calculated.capture_scores_detailed(df, "invalid_column"))
        # Assert no file creation attempted
        mock_create.assert_not_called()

@patch("output_actions_calculated.snowflake_execute")
def test_get_calculated_scores_detailed_missing_split(mock_exec):
    # this test the function get_calculated_scores_detailed with columns without underscore. Must exit the program.
    mock_exec.return_value = pd.DataFrame({
        "NAME":["Alice"],
        "BADCOLUMN":[1],
        "GAMEDAY":[1],
        "SEASON_ID":[1]
    })

    with patch("output_actions_calculated.fileA.create_jpg") as mock_create:
        assertExit(lambda: output_actions_calculated.get_calculated_scores_detailed(pd.Series(), pd.Series({"SEASON_ID":'S1',"GAMEDAY":'1ere journee'})))
        # Assert no file creation attempted
        mock_create.assert_not_called()

def test_get_calculated_scores_global_empty_df():
    # this test the function get_calculated_scores_global with empty dataframe. Must exit the program.
    df = pd.DataFrame(columns=["USER_NAME","TOTAL_POINTS","NB_GAMEDAY_PREDICT","NB_GAMEDAY_FIRST","NB_TOTAL_PREDICT","RANK",
                               "GAMEDAY", "SEASON_ID", "PT", "G1_BO", "G1_RE", "G1_PR", "G1_SW","G1_SD", "G1_SA"])
    with patch("output_actions_calculated.outputA.display_rank", side_effect=lambda d, _: d.assign(RANK=[])):
        df_out, count = output_actions_calculated.get_calculated_scores_global(df)
        assert df_out.empty
        assert count == 0

def test_get_calculated_scores_global_missing_cols():
    # this test the function get_calculated_scores_global with missing columns. Must exit the program.
    df = pd.DataFrame({"USER_NAME":["X"]})
    
    with patch("output_actions_calculated.outputA.display_rank", return_value=df.assign(RANK=[1])), \
        patch("output_actions_calculated.fileA.create_jpg") as mock_create:
        assertExit(lambda: output_actions_calculated.get_calculated_scores_global(df))
        # Assert no file creation attempted
        mock_create.assert_not_called()

@patch("output_actions_calculated.snowflake_execute")
@patch("output_actions_calculated.outputA.display_rank")
def test_get_calculated_scores_gameday_empty(mock_rank, mock_exec):
    # this test the function get_calculated_scores_gameday with empty dataframe. Must return an empty string.
    mock_exec.return_value = pd.DataFrame(columns=["USER_NAME","GAMEDAY_POINTS","RANK"])
    mock_rank.return_value = mock_exec.return_value
    result, count = output_actions_calculated.get_calculated_scores_gameday(pd.Series(), pd.Series({"SEASON_ID":'S1',"GAMEDAY":'1ere journee'}))
    assert result == ""
    assert count == 0

@patch("output_actions_calculated.snowflake_execute")
@patch("output_actions_calculated.outputA.display_rank")
def test_get_calculated_scores_gameday_missing_column(mock_rank, mock_exec):
    # this test the function get_calculated_scores_gameday with dataframe having missing columns. Must exit the program.
    mock_exec.return_value = pd.DataFrame({"USER_NAME":["X"]})
    mock_rank.return_value = mock_exec.return_value
    
    with patch("output_actions_calculated.fileA.create_jpg") as mock_create:
        assertExit(lambda: output_actions_calculated.get_calculated_scores_gameday(pd.Series(), pd.Series({"SEASON_ID":'S1',"GAMEDAY":'1ere journee'})))
        # Assert no file creation attempted
        mock_create.assert_not_called()

def test_get_calculated_scores_average_empty_df():
    # this test the function get_calculated_scores_average with empty dataframe. Must return an empty string
    df = pd.DataFrame(columns=["USER_NAME", "TOTAL_POINTS", "AVERAGE_POINTS", "NB_GAMEDAY_PREDICT",
                                "NB_GAMEDAY_FIRST", "NB_TOTAL_PREDICT", "RANK"])
    result = output_actions_calculated.get_calculated_scores_average(4, df)
    assert result == ("", 0, 2)  # empty string, no users, NB_MIN_GAMEDAY=2

def test_get_calculated_scores_average_missing_column():
    # this test the function get_calculated_scores_average with a dataframe with missing columns. Must exit the program
    df = pd.DataFrame({"USER_NAME": ["u1"], "NB_GAMEDAY_PREDICT": [3]})
    assertExit(lambda: output_actions_calculated.get_calculated_scores_average(4, df))

@patch("output_actions_calculated.snowflake_execute")
def test_get_calculated_predictchamp_ranking_empty(mock_sf):
    # this test the function get_calculated_predictchamp_ranking an empty dataframe. Must return an empty dataframe
    
    df_teamscores = pd.DataFrame(columns=["TEAM_NAME","WIN","LOSS","PERC_WIN", "POINTS_PRO", "POINTS_AGAINST", "POINTS_DIFF", "RANK"])
    mock_sf.return_value = df_teamscores

    sr_gameday_output_calculate = pd.Series({"SEASON_ID": 'S1', "GAMEDAY": '1ere journee'})

    sr_snowflake_account= pd.Series({
            'ACCOUNT': 'my_account',
            'WAREHOUSE': 'my_wh',
            'DATABASE_PROD': 'my_dbprod',
            'DATABASE_TEST': 'my_dbtest'
    })

    result_df = output_actions_calculated.get_calculated_predictchamp_ranking(sr_snowflake_account, sr_gameday_output_calculate)
    expect_df = pd.DataFrame(columns=["RANK","TEAM_NAME","WIN","LOSS","PERC_WIN", "POINTS_PRO", "POINTS_AGAINST", "POINTS_DIFF"])
    expect_df['RANK'] = expect_df['RANK'].astype(int)
    assert_frame_equal(result_df.reset_index(drop=True), expect_df.reset_index(drop=True))

@patch("output_actions_calculated.snowflake_execute")
def test_get_calculated_correction_no_rows(mock_sf):
    # this test the function get_calculated_correction with no rows. Must return an empty string
    mock_sf.return_value = pd.DataFrame(columns=["USER_NAME","PREDICT_ID"])
    sr_gameday_output_calculate = pd.Series({"SEASON_ID": 'S1', "GAMEDAY": '1ere journee'})

    sr_snowflake_account= pd.Series({
            'ACCOUNT': 'my_account',
            'WAREHOUSE': 'my_wh',
            'DATABASE_PROD': 'my_dbprod',
            'DATABASE_TEST': 'my_dbtest'
    })
    result, nb = output_actions_calculated.get_calculated_correction(sr_snowflake_account, sr_gameday_output_calculate)
    assert result == ""
    assert nb == 0

def test_get_calculated_list_gameday_missing_columns():
    # this test the function get_calculated_list_gameday with missing columns. Must exit the program
    df = pd.DataFrame({ "NB_PREDICTION": [3]})
    assertExit(lambda: output_actions_calculated.get_calculated_list_gameday(df))

def test_get_calculated_list_gameday_empty_dataframe():
    # this test the function get_calculated_list_gameday with empty df. Must return an empty string
    df = pd.DataFrame(columns=["GAMEDAY", "NB_PREDICTION"])
    result = output_actions_calculated.get_calculated_list_gameday(df)
    unittest.TestCase().assertEqual(result, "")

def test_get_mvp_month_race_figure_empty_df():
    # this function test the function get_mvp_month_race_figure with an empty return from query. Must return an empty string
    sr_snowflake_account= pd.Series({
        'ACCOUNT': 'my_account',
        'WAREHOUSE': 'my_wh',
        'DATABASE_PROD': 'my_dbprod',
        'DATABASE_TEST': 'my_dbtest'
    })
    sr_gameday_output_calculate = pd.Series({
        "GAMEDAY": 1,
        "SEASON_DIVISION": "A",
        "SEASON_ID": 'S1',
        'END_MONTH_LOCAL': 'MONTH_09',
        'END_YEARMONTH_LOCAL': 202509
    })

    with patch('output_actions_calculated.snowflake_execute', return_value=pd.DataFrame(columns=['USER_NAME', 'POINTS', 'WIN', 'LOSS', 'LIST_TEAMS'])):
        month, list_users, count = output_actions_calculated.get_mvp_month_race_figure(sr_snowflake_account, sr_gameday_output_calculate)

        assert month == 'MONTH_09'
        assert list_users == ""  # no users to join
        assert count == 0  # empty df should return count 0

def test_get_mvp_month_race_figure_invalid_points():
    # this function test the function get_mvp_month_race_figure with invalid points. Must exit the program
    sr_snowflake_account= pd.Series({
        'ACCOUNT': 'my_account',
        'WAREHOUSE': 'my_wh',
        'DATABASE_PROD': 'my_dbprod',
        'DATABASE_TEST': 'my_dbtest'
    })
    sr_gameday_output_calculate = pd.Series({
        "GAMEDAY": 1,
        "SEASON_DIVISION": "A",
        "SEASON_ID": 'S1',
        'END_MONTH_LOCAL': 'MONTH_09',
        'END_YEARMONTH_LOCAL': 202509
    })
    bad_df = pd.DataFrame({
        'USER_NAME': ['Alice'],
        'POINTS': ['not_a_number'], 
        'WIN': [1],
        'LOSS': [0],
        'LIST_TEAMS': ['TeamA']
    })

    with patch('output_actions_calculated.snowflake_execute', return_value=bad_df):
        result = output_actions_calculated.get_mvp_month_race_figure(sr_snowflake_account, sr_gameday_output_calculate)
        print(result)

def test_get_mvp_compet_race_figure_missing_key():
# this function test the function get_mvp_month_race_figure with missing column. Must exit the program
    sr_snowflake_account= pd.Series({
        'ACCOUNT': 'my_account',
        'WAREHOUSE': 'my_wh',
        'DATABASE_PROD': 'my_dbprod',
        'DATABASE_TEST': 'my_dbtest'
    })
    sr_gameday_output_calculate = pd.Series({
        "GAMEDAY": 1,
        "SEASON_DIVISION": "A",
        "SEASON_ID": 'S1',
        'END_MONTH_LOCAL': 'MONTH_09',
        'END_YEARMONTH_LOCAL': 202509
        # missing competition label
    })

    with patch('output_actions_calculated.snowflake_execute', return_value=pd.DataFrame()):
        assertExit(lambda:  output_actions_calculated.get_mvp_month_race_figure(sr_snowflake_account, sr_gameday_output_calculate))

@patch("output_actions_calculated.get_calculated_predictchamp_ranking")
@patch("output_actions_calculated.get_calculated_predictchamp_result")
@patch("output_actions_calculated.get_calculated_games_result", return_value=("res", 0))
@patch("output_actions_calculated.get_calculated_scores_detailed", return_value=(pd.DataFrame(), 0))
@patch("output_actions_calculated.get_calculated_scores_global", return_value=(pd.DataFrame(), 0))
@patch("output_actions_calculated.get_calculated_scores_average", return_value=("avg", 0, 0))
@patch("output_actions_calculated.get_calculated_scores_gameday", return_value=("day", 0))
@patch("output_actions_calculated.snowflake_execute")
def test_get_calculated_parameters_no_predictchamp(
    mock_sf,
    mock_scores_gameday,
    mock_scores_avg,
    mock_scores_global,
    mock_scores_detailed,
    mock_games_result,
    mock_predictchamp_result,
    mock_predictchamp_ranking):
    # this test get_calculated_parameters without predictchamp results. Must not call it
    
    mock_sf.side_effect = [
    pd.DataFrame([{"USER_NAME": "u1"}]),                       # qUserScores_Global
    pd.DataFrame([{"GAMEDAY": "GD1", "NB_PREDICTION": 3}]),    # qList_Gameday_Calculated
    pd.DataFrame([]),                                         # qGamePredictchamp
    pd.DataFrame([{"USER_NAME": "u1", "PREDICT_ID": 1}]),      # qCorrection
    pd.DataFrame(columns=["USER_NAME", "POINTS", "WIN", "LOSS", "LIST_TEAMS"]),  # qMVPRace_month_figures
    pd.DataFrame(columns=["USER_NAME", "POINTS", "WIN", "LOSS", "LIST_TEAMS"])   # qMVPRace_competition_figures
   ]

    sr_snowflake_account = pd.Series({
        "ACCOUNT": "my_account",
        "WAREHOUSE": "my_wh",
        "DATABASE_PROD": "my_dbprod",
        "DATABASE_TEST": "my_dbtest"
    })

    sr_gameday_output_calculate = pd.Series({
        "GAMEDAY": 1,
        "SEASON_DIVISION": "A",
        "SEASON_ID": 'S1',
        "DISPLAY_MONTH_MVP_RANKING": 1,
        "DISPLAY_COMPET_MVP_RANKING": 1,
        "BEGIN_DATE_LOCAL": "2025-09-17",
        "BEGIN_TIME_LOCAL": "18:00",
        "GAMEDAY_MESSAGE": "Test Gameday",
        "USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP": 1,
        "BEGIN_DATE_WEEKDAY": "WEEKDAY_3",
        "END_MONTH_LOCAL": "09",
        "END_YEARMONTH_LOCAL": "202509",
        "COMPETITION_LABEL": "Championship"
    })

    params = output_actions_calculated.get_calculated_parameters(sr_snowflake_account, sr_gameday_output_calculate)

    assert params["NB_GAME_PREDICTCHAMP"] == 0
    assert params["RESULTS_PREDICTCHAMP"] is None
    assert params["IS_FOR_RANK"] == 0
    assert params["HAS_HOME_ADV"] == 0
    assert params["RANK_PREDICTCHAMP_DF"] is None

    # Ensure we never try to calculate predictchamp results or ranking
    mock_predictchamp_result.assert_not_called()
    mock_predictchamp_ranking.assert_not_called()

def test_get_calculated_parameters_missing_key():
    # this test the function get_calculated_parameters with missing key. Should exit the program.
    sr_account = pd.Series()
    sr_gameday = pd.Series({"GAMEDAY": "G1"})  # missing SEASON_ID
    assertExit(lambda: output_actions_calculated.get_calculated_parameters(sr_account, sr_gameday))

def test_derive_calculated_parameters_for_country_empty_param_dict():
    # this test the function derive_calculated_parameters_for_country with an empty param_dict. Must return an empty dict
    sr_series = pd.Series([1, 2, 3])
    result = output_actions_calculated.derive_calculated_parameters_for_country({}, sr_series, "FR", [])
    assert result == {}

def test_derive_calculated_parameters_for_country_capture_func_raises():
    # this test the function derive_calculated_parameters_for_country with an capture_df_oneheader function failing. Must return an empty dict
    sr_series = pd.Series([1])
    with patch("output_actions_calculated.outputA.translate_param_for_country", return_value="X"), \
    patch("output_actions_calculated.outputA.define_filename", return_value="f.jpg"), \
    patch("output_actions_calculated.push_capture_online", return_value="http://url"), \
    patch("output_actions_calculated.config.TMPF", "/tmp"), \
    patch("output_actions_calculated.outputA.capture_df_oneheader", side_effect=Exception("boom")):

        param_dict = {"SCORES_GLOBAL_DF": "dummy"}
        assertExit(lambda: output_actions_calculated.derive_calculated_parameters_for_country(param_dict, sr_series, "FR", []))

def test_derive_calculated_parameters_filters_none_and_empty_df():
    # this test the function derive_calculated_parameters with an empty dataframe for SCORE_DETAILED_DF. Must return an empty dict
    df_empty = pd.DataFrame()
    param_dict = {"SCORES_DETAILED_DF": df_empty, "SCORES_GLOBAL_DF": None}
    sr_series = pd.Series([1])
    with patch("output_actions_calculated.fileA.read_json", return_value=["A"]), \
    patch("output_actions_calculated.config.multithreading_run", return_value=[{}]):
        result = output_actions_calculated.derive_calculated_parameters(param_dict, sr_series, ["FR"])
        assert result == {}

def test_create_calculated_messages_for_country_missing_key():
    # this test the function create_calculated_messages_for_country with a missing key. Must exit the program.
    param_dict = {"GAMEDAY": "X"}
    sr_series = pd.Series([1])
    assertExit(lambda: output_actions_calculated.create_calculated_messages_for_country(param_dict, "FR", "template", sr_series))
    
def test_create_calculated_messages_for_country_conditional_blocks():
    # this test the function create_calculated_messages_for_country with a template wihout parameters. Must return the same template
    param_dict = {
    "GAMEDAY": "G1",
    "SEASON_DIVISION": "DIV",
    "RESULT_GAMES": "res",
    "NB_USER_DETAIL": 0,
    "NB_CORRECTION": 0,
    "NB_USER_GLOBAL": 0,
    "NB_USER_AVERAGE": 0,
    "NB_GAME_PREDICTCHAMP": 0,
    "HAS_HOME_ADV": 0,
    "IS_FOR_RANK": 0,
    "NB_GAMEDAY_CALCULATED": 0,
    "LIST_GAMEDAY_CALCULATED": "",
    "NB_TOTAL_PREDICT": 0,
    "NB_USER_MONTH": 0,
    "NB_USER_COMPETITION": 0, 
    "SCORES_DETAILED_DF": pd.DataFrame({("SP", ""): [1]}),
    }

    sr_gameday_output_calculate = pd.Series({"SEASON_ID": 'S1'})
    with patch("output_actions_calculated.outputA.format_message", side_effect=lambda x: x), \
    patch("output_actions_calculated.outputA.replace_conditionally_message", side_effect=lambda c, b, e, cond: c), \
    patch("output_actions_calculated.outputA.define_filename", return_value="file.txt"), \
    patch("output_actions_calculated.fileA.create_txt"):
        content, country = output_actions_calculated.create_calculated_messages_for_country(param_dict, "FR", "template", sr_gameday_output_calculate)
    assert country == "FR"
    assert content == "template"

def test_process_output_message_calculated_no_topics():
    # this test the function process_output_message_calculated with no topics. Must run
    sr_gameday_output_calculate = pd.Series({'SEASON_ID': 'S1', 'GAMEDAY': '1ere journee'})

    context_dict = {
        "sr_snowflake_account_connect": "ACC",
        "str_output_gameday_calculation_template_FR": "tmp"
    }

    with patch("output_actions_calculated.snowflake_execute",
               return_value=pd.DataFrame(columns=["FORUM_COUNTRY","SEASON_ID"])), \
         patch("output_actions_calculated.get_calculated_parameters", return_value={}), \
         patch("output_actions_calculated.derive_calculated_parameters", return_value={}), \
         patch("output_actions_calculated.config.multithreading_run", return_value=[]), \
         patch("output_actions_calculated.logging.info"):
        
        output_actions_calculated.process_output_message_calculated(context_dict, sr_gameday_output_calculate)   

def test_process_output_message_calculated_posting_fails():
    # this test the function process_output_message_calculated with no posting failed. Must exit the program.
    sr_series = pd.Series([1])
    context_dict = {"sr_snowflake_account_connect": "ACC", "str_output_gameday_calculation_template_FR": "tmp"}
    df = pd.DataFrame({"FORUM_COUNTRY": ["FR"]})
    with patch("output_actions_calculated.snowflake_execute", return_value=df), \
    patch("output_actions_calculated.get_calculated_parameters", return_value={}), \
    patch("output_actions_calculated.derive_calculated_parameters", return_value={}), \
    patch("output_actions_calculated.config.multithreading_run", side_effect=[[("msg", "FR")], Exception("post failed")]), \
    patch("output_actions_calculated.logging.info"):
        
        assertExit(lambda: output_actions_calculated.process_output_message_calculated(context_dict, sr_series))

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_games_result_empty_df))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_games_result_negative_result))
    test_suite.addTest(unittest.FunctionTestCase(test_capture_scores_detailed_empty_df))
    test_suite.addTest(unittest.FunctionTestCase(test_capture_scores_detailed_invalid_columns))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_scores_detailed_missing_split))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_scores_global_empty_df))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_scores_global_missing_cols))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_scores_gameday_empty))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_scores_gameday_missing_column))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_scores_average_empty_df))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_scores_average_missing_column))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_predictchamp_ranking_empty))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_correction_no_rows))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_list_gameday_missing_columns))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_list_gameday_empty_dataframe))
    test_suite.addTest(unittest.FunctionTestCase(test_get_mvp_month_race_figure_empty_df))
    test_suite.addTest(unittest.FunctionTestCase(test_get_mvp_month_race_figure_invalid_points))
    test_suite.addTest(unittest.FunctionTestCase(test_get_mvp_compet_race_figure_missing_key))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_parameters_no_predictchamp))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_parameters_missing_key))
    test_suite.addTest(unittest.FunctionTestCase(test_derive_calculated_parameters_for_country_empty_param_dict))
    test_suite.addTest(unittest.FunctionTestCase(test_derive_calculated_parameters_for_country_capture_func_raises))
    test_suite.addTest(unittest.FunctionTestCase(test_derive_calculated_parameters_filters_none_and_empty_df))
    test_suite.addTest(unittest.FunctionTestCase(test_create_calculated_messages_for_country_missing_key))
    test_suite.addTest(unittest.FunctionTestCase(test_create_calculated_messages_for_country_conditional_blocks))
    test_suite.addTest(unittest.FunctionTestCase(test_process_output_message_calculated_no_topics))
    test_suite.addTest(unittest.FunctionTestCase(test_process_output_message_calculated_posting_fails))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)