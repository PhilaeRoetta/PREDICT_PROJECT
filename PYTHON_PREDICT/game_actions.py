''' 
    The purpose of this module is to interact with game website by:
    - getting the scope of game we need to extract 
    - then extract the games, and their details
'''

import logging
logging.basicConfig(level=logging.INFO)
import pandas as pd
import os

from snowflake_actions import snowflake_execute
import config
from file_actions import create_csv
from get_game_details_lnb import get_game_details_lnb
import sql_queries as sqlQ

game_info_functions = {
    "LNB": get_game_details_lnb
}

@config.exit_program(log_filter=lambda args: dict(args))
def extract_game(game_row: pd.Series) -> pd.DataFrame:
     
    """
        Extracts infos from a game by calling the appropriate get_game_details_* function
        Args:
            game_row (series - one row): the game with its few basic details
        Returns:
            dataframe: contains all games details extracted
        Raises:
            Exits the program if error running the function (using decorator)
    """

    logging.info(f"GAME -> EXTRACTING COMPETITION {str(game_row['COMPETITION_SOURCE'])} / ID {str(game_row['GAME_SOURCE_ID'])} [START]")
    
    # we get details from game with the url
    game_url = (os.getenv(game_row['COMPETITION_SOURCE'] + '_URL')
                            + "/?id="+str(game_row['GAME_SOURCE_ID']))
    get_game_details = game_info_functions.get(game_row['COMPETITION_SOURCE'])
    df_game_details = get_game_details(game_url,game_row)

    logging.info(f"GAME -> EXTRACTING COMPETITION {str(game_row['COMPETITION_SOURCE'])} / ID {str(game_row['GAME_SOURCE_ID'])} [DONE]")
    return df_game_details  

@config.exit_program(log_filter=lambda args: {})
def extract_games(games_scope_id: pd.DataFrame) -> pd.DataFrame:

    """
        Extracts games details we need - in parallel
        Args:
            games_scope_id (dataframe): games we need to extract with few basic infos
        Returns:
            dataframe: contains all games one by one with their complete info we need
        Raises:
            Exits the program if error running the function (using decorator)
    """
    logging.info("GAMES -> EXTRACTING [START]")

    games_extracted = []
    # We parallelize the extraction of each of games    
    game_args = [(row,) for _, row in games_scope_id.iterrows()]
    results = config.multithreading_run(extract_game, game_args)
    games_extracted = [r for r in results if r is not None]

    if len(games_extracted) > 0:
        df_game = pd.concat(games_extracted, ignore_index=True) 
    #if there is no game to extract we return an empty dataframe
    else:
         df_game = pd.DataFrame(columns=['COMPETITION_SOURCE','COMPETITION_ID','SEASON_ID',
                                            'GAMEDAY','DATE_GAME_LOCAL',
                                            'TIME_GAME_LOCAL','TEAM_HOME','SCORE_HOME',
                                            'TEAM_AWAY','SCORE_AWAY','GAME_SOURCE_ID'])
    
    # We create the file of games locally
    create_csv(os.path.join(config.TMPF,'game.csv'),df_game,config.game_encapsulated) 

    logging.info("GAMES -> EXTRACTING [END]")

    return df_game

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('df_competition',)})
def get_list_games_from_competition(sr_snowflake_account: pd.Series, df_competition: pd.DataFrame) -> pd.DataFrame:
    
    """
        Gets list of games we need to extract - for all initialized competitions
        Args:
            sr_snowflake_account (series - one row) : Contains snowflake paramaters to run the query
            df_competition (dataframe): the list of competition we process
        Returns:
            dataframe: contains all games id (with their competition an season ids) not already extracted once      
        Raises:
            Exits the program if error running the function (using decorator)
    """
    logging.info(f"GAME -> GETTING GAMES LIST [START]")
    
    games_id_extracted = snowflake_execute(sr_snowflake_account,
                                            sqlQ.game_actions_qGames_already_extracted)
    # We get all games in the range of df_competition between min and max
    df_competition_modified = df_competition.copy()
    df_competition_modified['GAME_SOURCE_ID'] = df_competition_modified.apply(
        lambda row: list(range(row['GAME_SOURCE_ID_MIN'], 
                                row['GAME_SOURCE_ID_MAX'] + 1)),
        axis=1)
    df_competition_games = (df_competition_modified[df_competition_modified['GAME_SOURCE_ID'].map(lambda x: len(x) > 0)]
                            .explode('GAME_SOURCE_ID')
                            .drop(columns=['GAME_SOURCE_ID_MIN', 'GAME_SOURCE_ID_MAX'])
                            .drop_duplicates()
                            .reset_index(drop=True))

    # We remove the games already extracted
    if games_id_extracted.empty:
        games_scope_id = df_competition_games
    else:
        merged = pd.merge(df_competition_games, games_id_extracted, how='left', indicator=True)
        games_scope_id = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
    
    logging.info(f"Number of games to extract: {len(games_scope_id)}")
    logging.info(f"GAME -> GETTING GAMES LIST [DONE]")

    return games_scope_id

@config.exit_program(log_filter=lambda args: {})
def get_list_games_from_need(sr_snowflake_account: pd.Series, sr_output_need: pd.Series) -> pd.DataFrame:
    
    """
        Gets list of games we need to extract - for a given output_need
        Args:
            sr_snowflake_account (series - one row) : Contains snowflake paramaters to run the query
            sr_output_need (series - one row): the output_need we process - we extract only games from its gameday
        Returns:
            dataframe: contains all games id (with their competition an season ids) not already extracted once
        Raises:
            Exits the program if error running the function (using decorator)
    """

    logging.info(f"GAME -> GETTING GAMES LIST [START]")

    games_scope_id = snowflake_execute(sr_snowflake_account,sqlQ.game_actions_qGames_to_extract,(sr_output_need['SEASON_ID'],sr_output_need['GAMEDAY']))

    logging.info(f"Number of games to extract: {len(games_scope_id)}")
    logging.info(f"GAME -> GETTING GAMES LIST [DONE]")

    return games_scope_id
