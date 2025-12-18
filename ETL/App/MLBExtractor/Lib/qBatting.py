
import statsapi
import pandas as pd
import time

from datetime import date, timedelta
from Lib.Executor import Executor


class Batting:

    def __init__(self, dateEntry = None):

        
        if dateEntry is None:
            self.evaluationDate = date.today() - timedelta(days = 1)
            Executor.send('Starting Batting Extractor from MLB-StatsAPI: No date provided - Running for current date')
        else:
            self.evaluationDate = dateEntry
            Executor.send(f'Starting Batting Extractor from MLB-StatsAPI: Evaluating at {self.evaluationDate}')
    
    def getDaily(self):

        st = time.time()
        try:
            games = statsapi.schedule(date = self.evaluationDate)

            Batters = pd.DataFrame()
            for game in games:

                gameID = game['game_id']
                gameDate = game['game_date']
                awayTeam = game['away_id']
                homeTeam = game['home_id']
                Executor.send(f'Extracting Batting Data for GameID {gameID}')
                
                ### Pull Box Score for game iteration
                box = statsapi.boxscore_data(gameID)

                ### Pull Data for Home Players
                homePlayers = pd.DataFrame(box['homeBatters']).iloc[1:]
                homePlayers['TeamID'] = homeTeam
                homePlayers['Date'] = gameDate
                homePlayers['IsHome'] = 1
                homePlayers['OpposingTeamID'] = awayTeam
                homePlayers['GameID'] = gameID
                Batters = pd.concat([Batters, homePlayers])
                del homePlayers

                ### Pull Data for Away Players
                awayPlayers = pd.DataFrame(box['awayBatters']).iloc[1:]
                awayPlayers['TeamID'] = awayTeam
                awayPlayers['Date'] = gameDate
                awayPlayers['IsHome'] = 0
                awayPlayers['OpposingTeamID'] = homeTeam
                awayPlayers['GameID'] = gameID
                Batters = pd.concat([Batters, awayPlayers])
                del awayPlayers 

            et = time.time()
            Executor.send(f'Batting Extractor from MLB-StatsAPI: Succeeded in {et - st:.2f}')
            return self.frameDaily(Batters)

        except Exception as e:
            et = time.time()
            Executor.send(f'Batting Extractor from MLB-StatsAPI: Failed in {et - st:.2f}')
            print(f'Error: {e}')
    
    @staticmethod
    def frameDaily(dat: pd.DataFrame):

        ### Drop Unneeded String Fields and Set Column Order
        dat = dat[[
            'personId', 'Date', 'GameID', 'TeamID', 'OpposingTeamID', 'IsHome', 'position', 'battingOrder', 'ab', 'r',
            'h', 'doubles', 'triples', 'hr', 'rbi', 'sb', 'bb', 'k', 'lob', 'avg', 'obp', 'slg', 'ops'
        ]]

        ### Rename Columns
        dat = dat.rename(
            columns = {
                'personId': 'PlayerID',
                'position': 'Position',
                'battingOrder': 'BattingOrder',
                'ab': 'AB',
                'r': 'Runs',
                'h': 'Hits',
                'doubles': 'Doubles',
                'triples': 'Triples',
                'hr': 'HR',
                'rbi': 'RBI',
                'sb': 'SB',
                'bb': 'BB',
                'k': 'K',
                'lob': 'LOB',
                'avg': 'AVG',
                'obp': 'OBP',
                'slg': 'SLG',
                'ops': 'OPS'

            }
        )

        return dat