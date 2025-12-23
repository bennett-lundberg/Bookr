import pandas as pd
import statsapi
import datetime as dt
import time

from Lib.Executor import Executor
from Lib.Engine import TableReader, TableWriter


class DimensionImporter:

    def __init__(self, credentials = dict, dateEntry = None):

        if dateEntry is None:
            self.evaluationDate = dt.date.today() - dt.timedelta(days = 1)
        else:
            self.evaluationDate = dateEntry

        self.writer = TableWriter(credentials)
        self.reader = TableReader(credentials)

        self.currentTeams = self.reader.read('SELECT * FROM MLB.DimTeams')


    ### Class method for checking the games at the evaluation date
    #### Pull all Team IDs and Team Names
    #### If Team ID Team Name does not exist in source table then insert that row to the table
    def ImportTeams(self):

        games = statsapi.schedule(date = self.evaluationDate)
        dimTab = self.reader.read('SELECT * FROM MLB.DimTeams')
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

                try:
                    self.writer.run(f"INSERT INTO MLB.DimTeams SELECT {homeID}, '{homeName}', '{dt.date.today()}'")
                except Exception as e:

                    self.writer.run(f"UPDATE MLB.DimTeams SET TeamName = '{homeName}', Modified = '{dt.date.today()}' WHERE TeamID = {homeID}")

                try:
                    self.writer.run(f"INSERT INTO MLB.DimTeams SELECT {awayID}, '{awayName}', '{dt.date.today()}'")
                except Exception as e:
                    self.writer.run(f"UPDATE MLB.DimTeams SET TeamName = '{awayName}', Modified = '{dt.date.today()}' WHERE TeamID = {awayID}")


            et = time.time()
            Executor.send(f'DimensionImporter.ImportTeams from MLB-StatsAPI: Succeeded in {et - st:.2f}')

        except Exception as e:

            et = time.time()
            Executor.send(f'DimensionImporter.ImportTeams from MLB-StatsAPI: Failed in {et - st:.2f}')
            print(f'Error: {e}')



    ### Class method for checking the players at the evaluation date
    #### Pull all Player IDs and Player Names
    #### If Player ID Player Name does not exist in source table then insert that row to the table
    def ImportPlayers(self, batterIDs: list, pitcherIDs: list):

        Executor.send(f'Running DimensionImporter.ImportPlayers from MLB-StatsAPI: Evaluating at {self.evaluationDate}')
        self.currentTeams = self.reader.read('SELECT * FROM MLB.DimTeams')
        st = time.time()

        try:

            for i in batterIDs:
                
                Executor.send(f'Running DimensionImporter.ImportPlayers from MLB-StatsAPI: Running for PlayerID {i}')
                playerInfo = statsapi.player_stat_data(i)

                PlayerID = i
                IsActive = int(playerInfo['active'])
                PlayerName = f'{playerInfo['first_name'].replace("'", "")} {playerInfo['last_name'].replace("'", "")}'
                PlayerNameFirst = playerInfo['first_name'].replace("'", "")
                PlayerNameLast = playerInfo['last_name'].replace("'", "")
                CurrentTeamID = self.getTeamID(playerInfo['current_team'])
                Position = playerInfo['position']
                BatHand = self.toHandedness(playerInfo['bat_side'])
                ThrowHand = self.toHandedness(playerInfo['pitch_hand'])
                IsActive = int(playerInfo['active'])

                try:
                    self.writer.run(f"INSERT INTO MLB.DimPlayers SELECT {PlayerID}, '{PlayerName}', '{PlayerNameFirst}', '{PlayerNameLast}', {CurrentTeamID}, '{Position}', '{BatHand}', '{ThrowHand}', {IsActive}")
                except Exception as e:
                    self.writer.run(
                        f"""
                        UPDATE MLB.DimPlayers
                        SET
                            PlayerName = '{PlayerName}',
                            PlayerNameFirst = '{PlayerNameFirst}',
                            PlayerNameLast = '{PlayerNameLast}',
                            CurrentTeamID = '{CurrentTeamID}',
                            Position = '{Position}',
                            BatHand = '{BatHand}',
                            ThrowHand = '{ThrowHand}',
                            IsActive = '{IsActive}'
                        WHERE PlayerID = '{PlayerID}'
                        """ 
                    )

            for i in pitcherIDs:

                Executor.send(f'Running DimensionImporter.ImportPlayers from MLB-StatsAPI: Running for PlayerID {i}')
                playerInfo = statsapi.player_stat_data(i)


                PlayerID = i
                PlayerName = f'{playerInfo['first_name'].replace("'", "")} {playerInfo['last_name'].replace("'", "")}'
                PlayerNameFirst = playerInfo['first_name'].replace("'", "")
                PlayerNameLast = playerInfo['last_name'].replace("'", "")
                CurrentTeamID = self.getTeamID(playerInfo['current_team'])
                Position = playerInfo['position']
                BatHand = self.toHandedness(playerInfo['bat_side'])
                ThrowHand = self.toHandedness(playerInfo['pitch_hand'])
                IsActive = int(playerInfo['active'])

                try:
                    self.writer.run(f"INSERT INTO MLB.DimPlayers SELECT {PlayerID}, '{PlayerName}', '{PlayerNameFirst}', '{PlayerNameLast}', {CurrentTeamID}, '{Position}', '{BatHand}', '{ThrowHand}', {IsActive}")
                except Exception as e:
                    self.writer.run(
                        f"""
                        UPDATE MLB.DimPlayers
                        SET
                            PlayerName = '{PlayerName}',
                            PlayerNameFirst = '{PlayerNameFirst}',
                            PlayerNameLast = '{PlayerNameLast}',
                            CurrentTeamID = '{CurrentTeamID}',
                            Position = '{Position}',
                            BatHand = '{BatHand}',
                            ThrowHand = '{ThrowHand}',
                            IsActive = '{IsActive}'
                        WHERE PlayerID = '{PlayerID}'
                        """ 
                    )
                
            et = time.time()
            Executor.send(f'DimensionImporter.ImportPlayers from MLB-StatsAPI: Succeeded in {et - st:.2f}')

        except Exception as e:

            et = time.time()
            Executor.send(f'DimensionImporter.ImportPlayers from MLB-StatsAPI: Failed in {et - st:.2f}')
            print(f'Error: {e}')

    ### Class method for import game ids and information
    #### Pull all GameIDs and Team and Outcome information
    #### If GameID does not exist in source table then insert that row to the table
    def ImportGames(self):

        Executor.send(f'Running DimensionImporter.ImportGames from MLB-StatsAPI: Evaluating at {self.evaluationDate}')
        games = statsapi.schedule(date = self.evaluationDate)
        st = time.time()

        try:
            
            for game in games:

                if game['game_type'] not in ['S', 'R', 'F', 'D', 'L', 'W', 'P']: continue

                gameID = game['game_id']
                homeID = game['home_id']
                awayID = game['away_id']
                homeScore = game['home_score']
                awayScore = game['away_score']
                gameTypeID = game['game_type']
                gameDate = game['game_date']
                Innings = game['current_inning']
                VenueID = game['venue_id']

                try:
                    self.writer.run(f"INSERT INTO MLB.DimGames SELECT {gameID}, {homeID}, {awayID}, {homeScore}, {awayScore}, {self.toGameTypeID(gameTypeID)}, '{gameDate}', {Innings}, {VenueID}")
                except Exception as e:
                    self.writer.run(
                        f"""
                        UPDATE MLB.DimGames
                        SET
                            HomeTeamID = {homeID},
                            AwayTeamID = {awayID},
                            HomeScore = {homeScore},
                            AwayScore = {awayScore},
                            GameTypeID = {self.toGameTypeID(gameTypeID)},
                            GameDate = '{gameDate}',
                            Innings = {Innings},
                            VenueID = {VenueID}
                        WHERE GameID = {gameID}
                        """ 
                    )
                    print(e)

                et = time.time()
                Executor.send(f'DimensionImporter.ImportGames from MLB-StatsAPI: Succeeded in {et - st:.2f}')

        except Exception as e:
            
            et = time.time()
            Executor.send(f'DimensionImporter.ImportGames from MLB-StatsAPI: Failed in {et - st:.2f}')
            print(f'Error: {e}')

    def ImportVenues(self):
        
        Executor.send(f'Running DimensionImporter.ImportVenues from MLB-StatsAPI: Evaluating at {self.evaluationDate}')
        games = statsapi.schedule(date = self.evaluationDate)
        st = time.time()

        try:
            
            for game in games:

                venueID = game['venue_id']
                venueName = game['venue_name']

                try:
                    self.writer.run(f"INSERT INTO MLB.DimVenues SELECT {venueID}, '{venueName}'")
                except Exception as e:
                    self.writer.run(
                        f"""
                        UPDATE MLB.DimVenues
                        SET
                            VenueName = '{venueName}'
                        WHERE VenueID = {venueID}
                        """ 
                    )

                et = time.time()
                Executor.send(f'DimensionImporter.ImportVenues from MLB-StatsAPI: Succeeded in {et - st:.2f}')

        except Exception as e:
            
            et = time.time()
            Executor.send(f'DimensionImporter.ImportVenues from MLB-StatsAPI: Failed in {et - st:.2f}')
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
        
    @staticmethod
    def toGameTypeID(val):
        if val in ['R']:
            return 1
        elif val in ['F', 'D', 'L', 'W', 'P']:
            return 2
        elif val in ['S']:
            return 4
        else:
            raise Exception(f'Invalid GameType: {val}')

    def getTeamID(self, val):
        
        try: teamID = self.currentTeams[self.currentTeams['TeamName'] == val]['TeamID'].values[0]
        except: teamID = 0
        return teamID