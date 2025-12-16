import pandas as pd
import statsapi
import datetime as dt
import time

from Lib.Executor import Executor


class DimensionImporter:

    def __init__(self, dateEntry = None):

        if dateEntry is None:
            self.evaluationDate = dt.date.today() - dt.timedelta(days = 1)
        else:
            self.evaluationDate = dateEntry

        self.currentTeams = pd.read_parquet('M:\\Bennett\\Private\\Bookr\\ETL\\App\\MLBExtractor\\bin\\DimMLBTeams.parquet')


    ### Class method for checking the games at the evaluation date
    #### Pull all Team IDs and Team Names
    #### If Team ID Team Name does not exist in source table then insert that row to the table
    def ImportTeams(self, table: str):

        games = statsapi.schedule(date = self.evaluationDate)
        dimTab = pd.read_parquet(table, engine = 'pyarrow')
        Executor.send(f'Running DimensionImporter.ImportTeams from MLB-StatsAPI: Evaluating at {self.evaluationDate}')

        st = time.time()

        try:
            ### Select Current Team List on Evaluation Date
            for game in games:
                
                ### Reset Index to avoid double counting
                dimTab.index = range(len(dimTab))
                
                ### Pull Home and Away names and away
                homeID = game['home_id']
                homeName = game['home_name']
                awayID = game['away_id']
                awayName = game['away_name']
                Executor.send(f'Running DimensionImporter.ImportTeams from MLB-StatsAPI: Running for GameID {game['game_id']}')

                
                ### Check Home values if already in table if not, update name or add entire row
                if (homeName in dimTab.TeamName.values) and (homeID in dimTab.TeamID.values):
                    continue
                else:
                    if homeID in dimTab.TeamID.values:
                        priorIndex = dimTab[dimTab.TeamID.values == homeID].index
                        dimTab.drop(priorIndex)
                    
                    iter = {'TeamID': [homeID], 'TeamName': [homeName], 'Modified': [dt.date.today()]}
                    dimTab = pd.concat([dimTab, pd.DataFrame(iter)])
                    Executor.send(f'Running DimensionImporter.ImportTeams from MLB-StatsAPI: Instance Added: {homeID}, {homeName}')

                ### Run again for Away values
                if (awayName in dimTab.TeamName.values) and (awayID in dimTab.TeamID.values):
                    continue
                else:
                    if awayID in dimTab.TeamID.values:
                        priorIndex = dimTab[dimTab.TeamID.values == awayID].index
                        dimTab.drop(priorIndex)
                    
                    iter = {'TeamID': [awayID], 'TeamName': [awayName], 'Modified': [dt.date.today()]}
                    dimTab = pd.concat([dimTab, pd.DataFrame(iter)])
                    Executor.send(f'Running DimensionImporter.ImportTeams from MLB-StatsAPI: Instance Added: {awayID}, {awayName}')

            ### Write to Parquet - this will be updated to a table
            dimTab.to_parquet(table, index = False)
            et = time.time()
            Executor.send(f'DimensionImporter.ImportTeams from MLB-StatsAPI: Succeeded in {et - st:.2f}')

        except Exception as e:

            et = time.time()
            Executor.send(f'DimensionImporter.ImportTeams from MLB-StatsAPI: Failed in {et - st:.2f}')
            print(f'Error: {e}')



    ### Class method for checking the players at the evaluation date
    #### Pull all Player IDs and Player Names
    #### If Player ID Player Name does not exist in source table then insert that row to the table
    def ImportPlayers(self, table: str, batterIDs: list, pitcherIDs: list):

        Executor.send(f'Running DimensionImporter.ImportPlayers from MLB-StatsAPI: Evaluating at {self.evaluationDate}')
        st = time.time()

        try:
            players = pd.DataFrame()
            for i in batterIDs:
                
                Executor.send(f'Running DimensionImporter.ImportPlayers from MLB-StatsAPI: Running for PlayerID {i}')
                playerInfo = statsapi.player_stat_data(i)

                playerIter = {
                    'PlayerID': [i],
                    'PlayerName': [f'{playerInfo['first_name']} {playerInfo['last_name']}'],
                    'PlayerNameFirst': [playerInfo['first_name']],
                    'PlayerNameLast': [playerInfo['last_name']],
                    'CurrentTeamID': [self.getTeamID(playerInfo['current_team'])],
                    'Position': [playerInfo['position']],
                    'BatHand': [self.toHandedness(playerInfo['bat_side'])],
                    'ThrowHand': [self.toHandedness(playerInfo['pitch_hand'])],
                    'IsActive': [int(playerInfo['active'])]
                }

                players = pd.concat([players, pd.DataFrame(playerIter)])

            for i in pitcherIDs:

                Executor.send(f'Running DimensionImporter.ImportPlayers from MLB-StatsAPI: Running for PlayerID {i}')
                playerInfo = statsapi.player_stat_data(i)

                playerIter = {
                    'PlayerID': [i],
                    'PlayerName': [f'{playerInfo['first_name']} {playerInfo['last_name']}'],
                    'PlayerNameFirst': [playerInfo['first_name']],
                    'PlayerNameLast': [playerInfo['last_name']],
                    'CurrentTeamID': [self.getTeamID(playerInfo['current_team'])],
                    'Position': [playerInfo['position']],
                    'BatHand': [self.toHandedness(playerInfo['bat_side'])],
                    'ThrowHand': [self.toHandedness(playerInfo['pitch_hand'])],
                    'IsActive': [int(playerInfo['active'])]
                }

                players = pd.concat([players, pd.DataFrame(playerIter)])
                
            players.to_parquet(table, index = False)
            et = time.time()
            Executor.send(f'DimensionImporter.ImportPlayers from MLB-StatsAPI: Succeeded in {et - st:.2f}')

        except Exception as e:

            et = time.time()
            Executor.send(f'DimensionImporter.ImportPlayers from MLB-StatsAPI: Failed in {et - st:.2f}')
            print(f'Error: {e}')


        

    @staticmethod
    def toHandedness(val):
        if val == 'Right':
            return 'R'
        elif val == 'Left':
            return 'L'
        elif val == 'Switch':
            return 'S'
        else:
            raise Exception(f'Invalid Handedness: {val}')
    
    def getTeamID(self, val):
        
        return self.currentTeams[self.currentTeams['TeamName'] == val]['TeamID'].values