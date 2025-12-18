
import statsapi
import pandas as pd
import time

from datetime import date, timedelta
from Lib.Executor import Executor


class Pitching:

    def __init__(self, dateEntry = None):

        if dateEntry is None:
            self.evaluationDate = date.today() - timedelta(days = 1)
            Executor.send('Starting Pitching Extractor from MLB-StatsAPI: No date provided - Running for current date')
        else:
            self.evaluationDate = dateEntry
            Executor.send(f'Starting Batting Extractor from MLB-StatsAPI: Evaluating at {self.evaluationDate}')
    
    def getDaily(self):

        st = time.time()
        try:
            games = statsapi.schedule(date = self.evaluationDate)

            Pitchers = pd.DataFrame()
            for game in games:


                gameID = game['game_id']
                gameDate = game['game_date']
                awayTeam = game['away_id']
                homeTeam = game['home_id']
                Executor.send(f'Extracting Pitching Data for GameID {gameID}')

                ### Pull Box Score for game iteration
                box = statsapi.boxscore_data(gameID)

                ### Pull Data for Home Players
                homePlayers = pd.DataFrame(box['homePitchers']).iloc[1:]
                homePlayers['TeamID'] = homeTeam
                homePlayers['Date'] = gameDate
                homePlayers['IsHome'] = 1
                homePlayers['GameID'] = gameID
                homePlayers['OpposingTeamID'] = awayTeam

                Pitchers = pd.concat([Pitchers, homePlayers])
                del homePlayers

                ### Pull Data for Away Players
                awayPlayers = pd.DataFrame(box['awayPitchers']).iloc[1:]
                awayPlayers['TeamID'] = awayTeam
                awayPlayers['Date'] = gameDate
                awayPlayers['IsHome'] = 0
                awayPlayers['GameID'] = gameID
                awayPlayers['OpposingTeamID'] = homeTeam
                Pitchers = pd.concat([Pitchers, awayPlayers])
                del awayPlayers

            et = time.time()
            Executor.send(f'Pitching Extractor from MLB-StatsAPI: Succeeded in {et - st:.2f}')
            return self.frameDaily(Pitchers)
        
        except Exception as e:
            et = time.time()
            Executor.send(f'Pitching Extractor from MLB-StatsAPI: Failed in {et - st:.2f}')
            print(f'Error: {e}')

    @staticmethod
    def frameDaily(dat: pd.DataFrame):

        ### Drop Unneeded String Fields and Set Column Order
        dat = dat[[
            'personId', 'Date', 'GameID', 'TeamID', 'OpposingTeamID', 'IsHome', 'ip', 'h', 'r',
            'er', 'bb', 'k', 'hr', 'era', 'p', 's'
        ]]

        ### Rename Columns
        dat = dat.rename(
            columns = {
                'personId': 'PlayerID',
                'ip': 'IP',
                'h': 'Hits',
                'r': 'Runs',
                'er': 'EarnedRuns',
                'bb': 'BB',
                'k': 'K',
                'hr': 'HR',
                'era': 'ERA',
                'p': 'Pitches',
                's': 'Strikes'
            }
        )

        return dat