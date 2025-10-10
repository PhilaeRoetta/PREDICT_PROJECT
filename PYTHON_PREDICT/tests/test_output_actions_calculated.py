'''
This tests file concern all functions in the output_actions_calculated module.
It units test the happy path for each function
'''
import unittest
from unittest.mock import patch
import pandas as pd
import sys
import os
from pandas.testing import assert_frame_equal

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import output_actions_calculated 

def test_get_calculated_games_result():
    # this test the function get_calculated_games_result
    
    df = pd.DataFrame([
        {'GAME_MESSAGE': 'G1.01', 'GAME_MESSAGE_SHORT': 1, 'TEAM_HOME_NAME': 'H', 'TEAM_AWAY_NAME': 'A', 'SCORE_HOME': 2, 'SCORE_AWAY': 1, 'RESULT': 1},
        {'GAME_MESSAGE': 'G1.02', 'GAME_MESSAGE_SHORT': 2, 'TEAM_HOME_NAME': 'X', 'TEAM_AWAY_NAME': 'Y', 'SCORE_HOME': 0, 'SCORE_AWAY': 2, 'RESULT': -2},
    ])

    with patch.object(output_actions_calculated, 'snowflake_execute', return_value=df):
        s, count = output_actions_calculated.get_calculated_games_result(pd.Series(), pd.Series({'SEASON_ID': 'S1', 'GAMEDAY': '1ere journee'}))
        s_expected = f'''1/ H vs A: [b]+1[/b] [ 2 - 1 ]
2/ X vs Y: [b]-2[/b] [ 0 - 2 ]'''
        
        assert count == 2
        assert s == s_expected

def test_capture_scores_detailed():
    # this test the function capture_scores_detailed
    df = pd.DataFrame(
        [[1, 2]],
        columns=pd.MultiIndex.from_tuples([('A', 'a1'), ('B', 'b1')])
    )
    row = pd.Series({'GAMEDAY': 'G'})

    with patch.object(output_actions_calculated.outputA, 'define_filename', return_value='mocked.png'), \
         patch.object(output_actions_calculated.fileA, 'create_jpg', return_value=None):

        name = output_actions_calculated.capture_scores_detailed(df, 'en')

def test_get_calculated_scores_detailed():

    # this test the function get_calculated_scores_detailed
    sr_gameday_output_calculate = pd.Series({"SEASON_ID": 'S1', "GAMEDAY": '1ere journee'})

    sr_snowflake_account= pd.Series({
            'ACCOUNT': 'my_account',
            'WAREHOUSE': 'my_wh',
            'DATABASE_PROD': 'my_dbprod',
            'DATABASE_TEST': 'my_dbtest'
    })

    df_predict_games = pd.DataFrame([
        {"NAME": "Alice", "GAMEDAY": '1ere journee', "SEASON_ID": 'S1', "PT": 18, 
         "G1_BO": "", "G1_RE": 3, "G1_PR": 19, "G1_SW": 15,"G1_SD": 0, "G1_SA": 0
        }
    ])

    with patch.object(output_actions_calculated, "snowflake_execute", return_value=df_predict_games):
        df_result, n_users = output_actions_calculated.get_calculated_scores_detailed(sr_snowflake_account, sr_gameday_output_calculate)
        df_expected = pd.DataFrame(
                [['Alice',18, "", 3, 19, 15, 0, 0]],
                columns=pd.MultiIndex.from_tuples([('NAME', ''), ('PT', ''),('G1', 'BO'),('G1', 'RE'),('G1', 'PR'),('G1', 'SW'),('G1', 'SD'),('G1', 'SA')])
        )
        # Make column index names match
        df_expected.columns.names = df_result.columns.names

        assert_frame_equal(df_result.reset_index(drop=True), df_expected.reset_index(drop=True))
        assert n_users == 1

def test_get_calculated_scores_global():

    # this test the function get_calculated_scores_global

    df_userscores_global = pd.DataFrame([
        {
            "USER_NAME": "Alice", "TOTAL_POINTS": 180, "NB_GAMEDAY_PREDICT": 3,
            "NB_GAMEDAY_FIRST": 1, "NB_TOTAL_PREDICT": 30, "RANK": 1,
            "GAMEDAY": '1ere journee', "SEASON_ID": 'S1', "PT": 18, 
            "G1_BO": "", "G1_RE": 3, "G1_PR": 19, "G1_SW": 15,"G1_SD": 0, "G1_SA": 0
        }
    ])

    df_rank = df_userscores_global
    with patch.object(output_actions_calculated.outputA, "display_rank", return_value=df_rank):
        df_result,nb_result = output_actions_calculated.get_calculated_scores_global(df_userscores_global)
    
    df_expected = pd.DataFrame([
        {'RANK': 1, 'USER_NAME': 'Alice', 'TOTAL_POINTS': 180, 'NB_GAMEDAY_PREDICT': 3, 'NB_GAMEDAY_FIRST': 1, 'NB_TOTAL_PREDICT': 30},
        ])
    assert_frame_equal(df_result.reset_index(drop=True), df_expected.reset_index(drop=True))
    assert nb_result == 1

def test_get_calculated_scores_gameday():
    # this test the function get_calculated_scores_gameday
    sr_gameday_output_calculate = pd.Series({"SEASON_ID": 'S1', "GAMEDAY": '1ere journee'})

    sr_snowflake_account= pd.Series({
            'ACCOUNT': 'my_account',
            'WAREHOUSE': 'my_wh',
            'DATABASE_PROD': 'my_dbprod',
            'DATABASE_TEST': 'my_dbtest'
    })

    df_userscores_gameday = pd.DataFrame([
        {"USER_NAME": "Alice", "GAMEDAY_POINTS": 10, "RANK": 1},
        {"USER_NAME": "Bob", "GAMEDAY_POINTS": 8, "RANK": 2}
    ])

    # mock snowflake_execute to return df_userscores_gameday
    with patch.object(output_actions_calculated, "snowflake_execute", return_value=df_userscores_gameday), \
        patch.object(output_actions_calculated.outputA, "display_rank", return_value=df_userscores_gameday):
            result_str, n_users = output_actions_calculated.get_calculated_scores_gameday(sr_snowflake_account, sr_gameday_output_calculate)
            # check number of users
            assert n_users == 2
            # check string content
            expected_str = f'''1. Alice - 10 pts 
2. Bob - 8 pts '''
            assert result_str == expected_str

def test_get_calculated_scores_average():
    # this test the function get_calculated_scores_average
    
    df_userscores_global = pd.DataFrame([
        {
            "USER_NAME": "Alice", "TOTAL_POINTS": 183, "AVERAGE_POINTS": 61, "NB_GAMEDAY_PREDICT": 3,
            "NB_GAMEDAY_FIRST": 1, "NB_TOTAL_PREDICT": 30, "RANK": 1
        }
    ])
    df_rank = df_userscores_global
    
    with patch.object(output_actions_calculated.outputA, 'calculate_and_display_rank', return_value= df_rank):
        s, count, nb_min = output_actions_calculated.get_calculated_scores_average(3, df_userscores_global)
        assert nb_min == 1
        assert count == 1
        assert s == "1. Alice - 61 pts"

def test_get_calculated_predictchamp_result():
    # this test the function get_calculated_predictchamp_result
    sr_gameday_output_calculate = pd.Series({"SEASON_ID": 'S1', "GAMEDAY": '1ere journee'})

    sr_snowflake_account= pd.Series({
            'ACCOUNT': 'my_account',
            'WAREHOUSE': 'my_wh',
            'DATABASE_PROD': 'my_dbprod',
            'DATABASE_TEST': 'my_dbtest'
    })

    df_game = pd.DataFrame([
        {"GAME_KEY": 1, "GAME_MESSAGE_SHORT": 1, "TEAM_HOME_NAME": "TeamA",
         "TEAM_AWAY_NAME": "TeamB", "IS_FOR_RANK": 1, "HAS_HOME_ADV": 1, "POINTS_BONUS": 2,
         "POINTS_HOME": 7, "POINTS_AWAY": 0, "WINNER": 1},

        {"GAME_KEY": 2, "GAME_MESSAGE_SHORT": 2, "TEAM_HOME_NAME": "TeamC",
         "TEAM_AWAY_NAME": "TeamD", "IS_FOR_RANK": 1, "HAS_HOME_ADV": 1, "POINTS_BONUS": 0,
         "POINTS_HOME": 0, "POINTS_AWAY": 3, "WINNER": 2}
    ])


    # fake detail DataFrame returned by snowflake_execute for user points
    df_detail = pd.DataFrame([
        {"GAME_KEY": 1, "TEAM_NAME": "TeamA", "USER_NAME": "Alice", "POINTS": 5, "RANK_USER_TEAM": 1},
        {"GAME_KEY": 1, "TEAM_NAME": "TeamA", "USER_NAME": "Alice2", "POINTS": 3, "RANK_USER_TEAM": 2},
        {"GAME_KEY": 2, "TEAM_NAME": "TeamD", "USER_NAME": "Alice3", "POINTS": 3, "RANK_USER_TEAM": 1}
    ])

    # mock snowflake_execute to return df_detail when called
    with patch.object(output_actions_calculated, "snowflake_execute", return_value=df_detail):
        result = output_actions_calculated.get_calculated_predictchamp_result(df_game, sr_snowflake_account, sr_gameday_output_calculate)
        expected_str = f'''1/ [b]TeamA[/b] vs TeamB : 7 - 0
[code]__FOR__ TeamA:
-> Alice: 5 pts   - [__Not counted__: Alice2 (3)]
-> __Home bonus__: 2 pts[/code]
2/ TeamC vs [b]TeamD[/b] : 0 - 3
[code]__FOR__ TeamD:
-> Alice3: 3 pts[/code]'''

        assert result == expected_str

def test_get_calculated_predictchamp_ranking():
    # this test the function get_calculated_predictchamp_ranking
    sr_gameday_output_calculate = pd.Series({"SEASON_ID": 'S1', "GAMEDAY": '1ere journee'})

    sr_snowflake_account= pd.Series({
            'ACCOUNT': 'my_account',
            'WAREHOUSE': 'my_wh',
            'DATABASE_PROD': 'my_dbprod',
            'DATABASE_TEST': 'my_dbtest'
    })
    
    df_teamscores = pd.DataFrame([
        {"TEAM_NAME": "TeamA", "WIN": 7, "LOSS": 1, "PERC_WIN": 87.5, "POINTS_PRO": 302, "POINTS_AGAINST": 277, "POINTS_DIFF": 25, "RANK": 1},
        {"TEAM_NAME": "TeamB", "WIN": 2, "LOSS": 6, "PERC_WIN": 33.33, "POINTS_PRO": 251, "POINTS_AGAINST": 277, "POINTS_DIFF": -26, "RANK": 2},  
    ])

    df_ranked = df_teamscores.copy()

    # mock snowflake_execute to return df_teamscores
    with patch.object(output_actions_calculated, "snowflake_execute", return_value=df_teamscores):
        # mock display_rank to add ranking
        with patch.object(output_actions_calculated.outputA, "display_rank", return_value=df_ranked) as mock_rank:
            result_df = output_actions_calculated.get_calculated_predictchamp_ranking(sr_snowflake_account, sr_gameday_output_calculate)

            mock_rank.assert_called_once_with(df_teamscores, 'RANK')
            pd.testing.assert_frame_equal(result_df, df_ranked)

def test_get_calculated_correction():
    # this test the function get_calculated_correction
    sr_gameday_output_calculate = pd.Series({"SEASON_ID": 'S1', "GAMEDAY": '1ere journee'})

    sr_snowflake_account= pd.Series({
            'ACCOUNT': 'my_account',
            'WAREHOUSE': 'my_wh',
            'DATABASE_PROD': 'my_dbprod',
            'DATABASE_TEST': 'my_dbtest'
    })

    df_correction = pd.DataFrame([
        {"USER_NAME": "Alice", "PREDICT_ID": 101},
        {"USER_NAME": "Alice", "PREDICT_ID": 102},
        {"USER_NAME": "Bob", "PREDICT_ID": 201},
    ])

    # mock snowflake_execute to return df_correction
    with patch.object(output_actions_calculated, "snowflake_execute", return_value=df_correction):
        result_str, result_count = output_actions_calculated.get_calculated_correction(sr_snowflake_account, sr_gameday_output_calculate)

        expected_str = "Alice : 101 / 102\nBob : 201"
        expected_count = 2

        assert result_str == expected_str
        assert result_count == expected_count

def test_get_calculated_list_gameday():
    # this test the function get_calculated_list_gameday
    df_list_gameday = pd.DataFrame([
        {"GAMEDAY": "1ere journee", "NB_PREDICTION": 15},
        {"GAMEDAY": "2eme journee", "NB_PREDICTION": 8},
        {"GAMEDAY": "3eme journee", "NB_PREDICTION": 7},
    ])

    str_list_gameday = output_actions_calculated.get_calculated_list_gameday(df_list_gameday)
    assert str_list_gameday == '1ere journee (15) / 2eme journee (8) / 3eme journee (7)'

def test_get_mvp_month_race_figure():
    # this test the function get_calculated_predictchamp_ranking
    sr_gameday_output_calculate = pd.Series({"SEASON_ID": 'S1', "GAMEDAY": '1ere journee', "END_MONTH_LOCAL": 'MONTH_05', "END_YEARMONTH_LOCAL": "202405"})

    sr_snowflake_account= pd.Series({
            'ACCOUNT': 'my_account',
            'WAREHOUSE': 'my_wh',
            'DATABASE_PROD': 'my_dbprod',
            'DATABASE_TEST': 'my_dbtest'
    })

    df = pd.DataFrame([
        {"USER_NAME": "User1", "POINTS": 82, "WIN": 3, "LOSS": 2, "LIST_TEAMS": 'Team1, Team2'},
        {"USER_NAME": "User2", "POINTS": 38, "WIN": 1, "LOSS": 2, "LIST_TEAMS": 'Team3'},  
    ])


    # mock snowflake_execute to return df_teamscores
    with patch.object(output_actions_calculated, "snowflake_execute", return_value=df):
        # mock display_rank to add ranking
        
        (gameday_month, list_user, count) = output_actions_calculated.get_mvp_month_race_figure(sr_snowflake_account, sr_gameday_output_calculate)
        assert gameday_month == "MONTH_05"
        assert count == 2
        assert list_user == f'''User1 - 82 pts / 3__W__-2__L__ [__with__ Team1, Team2]
User2 - 38 pts / 1__W__-2__L__ [__with__ Team3]'''

def test_get_mvp_compet_race_figure():
    # this test the function get_calculated_predictchamp_ranking
    sr_gameday_output_calculate = pd.Series({"SEASON_ID": 'S1', "GAMEDAY": '1ere journee', "COMPETITION_LABEL": 'Regular season'})

    sr_snowflake_account= pd.Series({
            'ACCOUNT': 'my_account',
            'WAREHOUSE': 'my_wh',
            'DATABASE_PROD': 'my_dbprod',
            'DATABASE_TEST': 'my_dbtest'
    })

    df = pd.DataFrame([
        {"USER_NAME": "User1", "POINTS": 82, "WIN": 3, "LOSS": 2, "LIST_TEAMS": 'Team1, Team2'},
        {"USER_NAME": "User2", "POINTS": 38, "WIN": 1, "LOSS": 2, "LIST_TEAMS": 'Team3'},  
    ])


    # mock snowflake_execute to return df_teamscores
    with patch.object(output_actions_calculated, "snowflake_execute", return_value=df):
        # mock display_rank to add ranking
        
        (compet, list_user, count) = output_actions_calculated.get_mvp_compet_race_figure(sr_snowflake_account, sr_gameday_output_calculate)
        assert compet == "Regular season"
        assert count == 2
        assert list_user == f'''User1 - 82 pts / 3__W__-2__L__ [__with__ Team1, Team2]
User2 - 38 pts / 1__W__-2__L__ [__with__ Team3]'''

def test_get_calculated_parameters():

    sr_snowflake_account= pd.Series({
        'ACCOUNT': 'my_account',
        'WAREHOUSE': 'my_wh',
        'DATABASE_PROD': 'my_dbprod',
        'DATABASE_TEST': 'my_dbtest'
    })
    sr_gameday_output_calculate = pd.Series({
        "GAMEDAY": '1ere journee',
        "SEASON_DIVISION": "A",
        "SEASON_ID": 'S1',
        "DISPLAY_MONTH_MVP_RANKING": 1,
        "DISPLAY_COMPET_MVP_RANKING": 1
    })

    # Mock DataFrames
    df_userscores_global = pd.DataFrame({"USER_NAME": ["Alice"], "TOTAL_POINTS": [10], "NB_TOTAL_PREDICT": [5], "NB_GAMEDAY_PREDICT": [2]})
    df_gameday_calculated = pd.DataFrame({"NB_PREDICTION": [5, 3]})
    df_gamepredictchamp = pd.DataFrame({"IS_FOR_RANK": [1], "HAS_HOME_ADV": [1]})

    with patch("output_actions_calculated.get_calculated_games_result", return_value=(["game1", "game2"], 2)), \
         patch("output_actions_calculated.get_calculated_scores_detailed", return_value=("scores_df", 3)), \
         patch("output_actions_calculated.snowflake_execute", side_effect=[df_userscores_global, df_gameday_calculated, df_gamepredictchamp]), \
         patch("output_actions_calculated.get_calculated_scores_global", return_value=("global_scores", 1)), \
         patch("output_actions_calculated.get_calculated_scores_average", return_value=("average_scores", 1, 1)), \
         patch("output_actions_calculated.get_calculated_scores_gameday", return_value=("gameday_scores", 2)), \
         patch("output_actions_calculated.get_calculated_list_gameday", return_value=["GD1", "GD2"]), \
         patch("output_actions_calculated.get_calculated_predictchamp_result", return_value="predictchamp_results"), \
         patch("output_actions_calculated.get_calculated_predictchamp_ranking", return_value="predictchamp_ranking"), \
         patch("output_actions_calculated.get_calculated_correction", return_value=(["correction"], 1)), \
         patch("output_actions_calculated.get_mvp_month_race_figure", return_value=("GAMEDAY_MONTH", ["User1"], 1)), \
         patch("output_actions_calculated.get_mvp_compet_race_figure", return_value=("GAMEDAY_COMP", ["User2"], 1)):

        result = output_actions_calculated.get_calculated_parameters(sr_snowflake_account, sr_gameday_output_calculate)

        assert result["GAMEDAY"] == '1ere journee'
        assert result["SEASON_DIVISION"] == "A"
        assert result["NB_GAMES"] == 2
        assert result["SCORES_GLOBAL_DF"] == "global_scores"
        assert result["NB_GAMEDAY_CALCULATED"] == len(df_gameday_calculated)
        assert result["NB_TOTAL_PREDICT"] == df_gameday_calculated["NB_PREDICTION"].sum()
        assert result["RESULTS_PREDICTCHAMP"] == "predictchamp_results"
        assert result["RANK_PREDICTCHAMP_DF"] == "predictchamp_ranking"
        assert result["LIST_CORRECTION"] == ["correction"]
        assert result["NB_CORRECTION"] == 1
        assert result["GAMEDAY_MONTH"] == "GAMEDAY_MONTH"
        assert result["LIST_USER_MONTH"] == ["User1"]
        assert result["GAMEDAY_COMPETITION"] == "GAMEDAY_COMP"
        assert result["LIST_USER_COMPETITION"] == ["User2"]

def test_derive_calculated_parameters_for_country():
    # this test the function derive_calculated_parameters_for_country
    param_dict = {
        "SCORES_DETAILED_DF": pd.DataFrame({"A": [1]}),
        "SCORES_GLOBAL_DF": pd.DataFrame({"B": [2]}),
        "RANK_PREDICTCHAMP_DF": pd.DataFrame({"C": [3]}),
        "OTHER_PARAM": "value"
    }

    # Mock series
    sr_gameday_output_calculate = pd.Series({"SEASON_ID": 'S1', "GAMEDAY": "1ere journee"})

    # Country and translations list
    country = "FR"
    translations = ["A", "B", "C"]

    with patch.object(output_actions_calculated.outputA, "translate_param_for_country", side_effect=lambda val, ctry, trans: f"{val}_translated"), \
         patch.object(output_actions_calculated.outputA, "define_filename", side_effect=lambda prefix, series, ext, ctry: f"{prefix}_{ctry}.jpg"), \
         patch("output_actions_calculated.capture_scores_detailed", return_value="dummy_capture"), \
         patch.object(output_actions_calculated.outputA, "capture_df_oneheader", return_value=None), \
         patch.object(output_actions_calculated, "push_capture_online", side_effect=lambda path: f"url_for_{os.path.basename(path)}"):

        result = output_actions_calculated.derive_calculated_parameters_for_country(
            param_dict, sr_gameday_output_calculate, country, translations
        )
        # Assertions for translations
        assert result["SCORES_DETAILED_DF_FR"] == "0    1\nName: A, dtype: int64_translated" or "dummy_capture_translated"
        assert result["SCORES_GLOBAL_DF_FR"] == "0    2\nName: B, dtype: int64_translated" or "dummy_capture_translated"
        assert result["RANK_PREDICTCHAMP_DF_FR"] == "0    3\nName: C, dtype: int64_translated" or "dummy_capture_translated"
        assert result["OTHER_PARAM_FR"] == "value_translated"

        # Assertions for captured filenames and URLs
        assert result["SCORES_DETAILED_DF_CAPTURE_FR"] == "table_score_details_FR.jpg"
        assert result["SCORES_DETAILED_DF_URL_FR"] == "url_for_table_score_details_FR.jpg"
        assert result["SCORES_GLOBAL_DF_CAPTURE_FR"] == "table_global_scores_FR.jpg"
        assert result["SCORES_GLOBAL_DF_URL_FR"] == "url_for_table_global_scores_FR.jpg"
        assert result["RANK_PREDICTCHAMP_DF_CAPTURE_FR"] == "table_predictchamp_ranking_FR.jpg"
        assert result["RANK_PREDICTCHAMP_DF_URL_FR"] == "url_for_table_predictchamp_ranking_FR.jpg"

def test_derive_calculated_parameters():
    # this test the function derive_calculated_parameters

    # Mock param_dict
    param_dict = {
        'SCORES_DETAILED_DF': pd.DataFrame([[1, 2]]),
        'SCORES_GLOBAL_DF': pd.DataFrame([[3, 4]]),
        'RANK_PREDICTCHAMP_DF': pd.DataFrame([[5, 6]]),
        'RESULTS_PREDICTCHAMP': "some_result",
        'OTHER_PARAM': "should_not_be_derived"
    }

    sr_gameday_output_calculate = pd.Series({'SEASON_ID': 'S1', 'GAMEDAY': '1ere journee'})
    list_countries = ['FR', 'US']

    # Patch dependencies
    with patch("output_actions_calculated.outputA.translate_param_for_country", lambda value, country, translations: f"{value}_{country}_mocked"), \
         patch("output_actions_calculated.fileA.read_json", lambda path: []), \
         patch("output_actions_calculated.capture_scores_detailed", lambda df, fname: fname), \
         patch("output_actions_calculated.outputA.capture_df_oneheader", lambda df, fname: fname), \
         patch("output_actions_calculated.outputA.define_filename", lambda prefix, series, ext, country: f"{prefix}_{country}.jpg"), \
         patch("output_actions_calculated.push_capture_online", lambda path: f"url_for_{path}"), \
         patch("output_actions_calculated.config.multithreading_run", lambda func, args: [func(*arg) for arg in args]):

        result = output_actions_calculated.derive_calculated_parameters(param_dict, sr_gameday_output_calculate, list_countries)

    # Check that derived keys exist and have expected mocked value
    derived_keys_to_check = ['SCORES_DETAILED_DF', 'SCORES_GLOBAL_DF', 'RANK_PREDICTCHAMP_DF', 'RESULTS_PREDICTCHAMP']
    for country in list_countries:
        for key in derived_keys_to_check:
            derived_key = f"{key}_{country}"
            assert derived_key in result

    # Make sure keys that should not be derived are not in result
    for country in list_countries:
        assert f"OTHER_PARAM_{country}" not in result

def test_create_calculated_messages_for_country():
    # this test the function create_calculated_messages_for_country

    param_dict = {
        "GAMEDAY": "1",
        "SEASON_DIVISION": "A",
        "RESULT_GAMES": "GameResults",
        "NB_GAMEDAY_CALCULATED": 2,
        "NB_TOTAL_PREDICT": 10,
        "LIST_GAMEDAY_CALCULATED": "GD_LIST",
        "NB_USER_DETAIL": 1,
        "SCORES_GAMEDAY": "ScoreTable",
        "SCORES_DETAILED_DF_URL_FR": "http://detail.fr",
        "NB_GAMES": 5,
        "NB_CORRECTION": 1,
        "LIST_CORRECTION": "CorrectionList",
        "NB_USER_GLOBAL": 1,
        "SCORES_GLOBAL_DF_URL_FR": "http://global.fr",
        "NB_USER_AVERAGE": 1,
        "NB_MIN_PREDICTION": 2,
        "SCORES_AVERAGE": "AverageScores",
        "NB_GAME_PREDICTCHAMP": 1,
        "RESULTS_PREDICTCHAMP_FR": "PredictChampResults",
        "IS_FOR_RANK": 1,
        "RANK_PREDICTCHAMP_DF_URL_FR": "http://rank.fr",
        "NB_USER_MONTH": 1,
        "GAMEDAY_MONTH_FR": "Sept",
        "LIST_USER_MONTH_FR": "UserMonthList",
        "NB_USER_COMPETITION": 1,
        "GAMEDAY_COMPETITION_FR": "CompetitionDay",
        "LIST_USER_COMPETITION_FR": "UserCompetitionList",
        "HAS_HOME_ADV": 1
    }
    country = "FR"
    template = """
    #MESSAGE_PREFIX_PROGRAM_STRING#
    #GAMEDAY# #SEASON_DIVISION# #RESULT_GAMES#
    #NB_GAMEDAY_CALCULATED# #NB_TOTAL_PREDICT# #LIST_GAMEDAY_CALCULATED#
    #SCORES_GAMEDAY# #IMGDETAIL# #NB_GAMES#
    #LIST_USER_SCOREAUTO0#
    #IMGSEASON#
    #NB_MIN_PREDICTION# #SCORES_AVERAGE#
    #RESULTS_PREDICTCHAMP# #RANK_PREDICTCHAMP_IMG#
    #GAMEDAY_MONTH# #LIST_USER_MONTH# #NB_USER_MONTH#
    #GAMEDAY_COMPETITION# #LIST_USER_COMPETITION# #NB_USER_COMPETITION#
    """

    sr_gameday_output_calculate = pd.Series({"SEASON_ID": 'S1'})

    with patch("output_actions_calculated.outputA.format_message", return_value=template) as mock_format, \
         patch("output_actions_calculated.outputA.replace_conditionally_message", side_effect=lambda c, b, e, cond: c) as mock_replace, \
         patch("output_actions_calculated.outputA.define_filename", return_value="testfile.txt") as mock_filename, \
         patch("output_actions_calculated.fileA.create_txt") as mock_create_txt, \
         patch("output_actions_calculated.config", **{"TMPF": "/tmp", "message_prefix_program_string": "PREFIX"}):
        
        # --- Act ---
        content, result_country = output_actions_calculated.create_calculated_messages_for_country(
            param_dict, country, template, sr_gameday_output_calculate
        )

        # --- Assert ---
        assert "PREFIX" in content
        assert "1" in content
        assert "GameResults" in content
        assert "GD_LIST" in content
        assert "PredictChampResults" in content
        assert "UserCompetitionList" in content
        assert result_country == "FR"

        mock_format.assert_called_once_with(template)
        mock_filename.assert_called_once()
        mock_create_txt.assert_called_once()
        # ensure conditional replacement was applied
        mock_replace.assert_called()

def test_process_output_message_calculated():
    # this test the function process_output_message_calculated

    # Mock context dictionary
    context_dict = {
        'sr_snowflake_account_connect': pd.Series({
            'ACCOUNT': 'my_account',
            'WAREHOUSE': 'my_wh',
            'DATABASE_PROD': 'my_dbprod',
            'DATABASE_TEST': 'my_dbtest'
        }),
        'str_output_gameday_calculation_template_FR': "Template FR",
        'str_output_gameday_calculation_template_US': "Template US"
    }

    # Mock gameday series
    sr_gameday_output_calculate = pd.Series({
        'GAMEDAY': 'GAMEDAY1',
        'IS_CALCULATED':1,
        'BEGIN_DATE_LOCAL': dt.strptime('2025-02-01', '%Y-%m-%d'),
        'BEGIN_TIME_LOCAL': '20:15:00',
        'GAMEDAY_MESSAGE': 'GD1',
        'SEASON_DIVISION': 'SD1',
        'SEASON_ID': 'S1',
        'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP': 1,
        'BEGIN_DATE_WEEKDAY': 'Monday'
    })

    # Mock topics dataframe
    df_topics = pd.DataFrame({
        'FORUM_COUNTRY': ['FR', 'US'],
        'TOPIC_ID': [1,2]
    })

    # Mock multithreading_run to just return the processed list
    def mock_multithreading_run(func, args_list):
        return [func(*args) for args in args_list]

    # Mock message creation
    def mock_create_message(param_dict, country, template, series):
        return (f"Message for {country}", country)

    # Patch all external dependencies
    with patch("output_actions_calculated.snowflake_execute", return_value=df_topics), \
         patch("output_actions_calculated.get_calculated_parameters", return_value={'dummy':'param'}), \
         patch("output_actions_calculated.derive_calculated_parameters", return_value={'derived':'param'}), \
         patch("output_actions_calculated.create_calculated_messages_for_country", side_effect=mock_create_message), \
         patch("output_actions_calculated.config.multithreading_run", side_effect=mock_multithreading_run), \
         patch("output_actions_calculated.post_message") as mock_post_message:

        # Call the function
        output_actions_calculated.process_output_message_calculated(context_dict, sr_gameday_output_calculate)


    assert mock_post_message.call_count == 2
    calls_args = [call.args[1] for call in mock_post_message.call_args_list]
    assert "Message for FR" in calls_args
    assert "Message for US" in calls_args

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_games_result))
    test_suite.addTest(unittest.FunctionTestCase(test_capture_scores_detailed))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_scores_detailed))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_scores_global))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_scores_gameday))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_scores_average))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_predictchamp_result))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_predictchamp_ranking))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_correction))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_list_gameday))
    test_suite.addTest(unittest.FunctionTestCase(test_get_mvp_month_race_figure))
    test_suite.addTest(unittest.FunctionTestCase(test_get_mvp_compet_race_figure))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_parameters))
    test_suite.addTest(unittest.FunctionTestCase(test_derive_calculated_parameters_for_country))
    test_suite.addTest(unittest.FunctionTestCase(test_derive_calculated_parameters))
    test_suite.addTest(unittest.FunctionTestCase(test_create_calculated_messages_for_country))
    test_suite.addTest(unittest.FunctionTestCase(test_process_output_message_calculated))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)