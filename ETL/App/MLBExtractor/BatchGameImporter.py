
import statsapi
import datetime
import pandas as pd
import pyodbc
from Lib.Engine import TableWriter

class BatchGameImporter:

    def __init__(self, gameData, args):

        self.GameID = gameData['game_id']
        self.GameDate = gameData['game_date']
        self.GameType = f"{gameData['game_type']}"
        self.AwayName = f"'{gameData['away_name']}'"
        self.HomeName = f"'{gameData['home_name']}'"
        self.AwayID = gameData['away_id']
        self.HomeID = gameData['home_id']
        self.AwayScore = gameData['away_score']
        self.HomeScore = gameData['home_score']
        self.Innings = gameData['current_inning']
        self.VenueID = gameData['venue_id']
        self.VenueName = f"{gameData['venue_name']}"
        self.BoxScore = statsapi.boxscore_data(self.GameID)

        self.Writer = TableWriter(args)
        
    def ImportGame(self):

        try:
            self.Writer.run(
                f"""
                MERGE INTO mlb.DimGames AS target 
                USING ( 
                    SELECT {self.GameID} as GameID, 
                    {self.HomeID} as HomeTeamID,
                    {self.AwayID} as AwayTeamID,
                    {self.HomeScore} as HomeScore,
                    {self.AwayScore} as AwayScore,
                    {self.toGameTypeID(self.GameType)} as GameTypeID, 
                    '{self.GameDate}' as GameDate,
                    {self.Innings} as Innings,
                    {self.VenueID} as VenueID
                )
                AS source 
                ON target.GameID = source.GameID 
                
                WHEN MATCHED THEN 
                UPDATE SET 
                    HomeTeamID = source.HomeTeamID, 
                    AwayTeamID = source.AwayTeamID, 
                    HomeScore = source.HomeScore,
                    AwayScore = source.AwayScore,
                    GameTypeID = source.GameTypeID,
                    GameDate = source.GameDate,
                    Innings = source.Innings,
                    VenueID = source.VenueID
                    
                WHEN NOT MATCHED THEN 
                INSERT (GameID, HomeTeamID, AwayTeamID, HomeScore, AwayScore, GameTypeID, GameDate, Innings, VenueID)
                VALUES (source.GameID, source.HomeTeamID, source.AwayTeamID, source.HomeScore, source.AwayScore, source.GameTypeID, source.GameDate, source.Innings, source.VenueID);
                """ 
            )

        except Exception as e:
            print(f'BatchGameImporter.ImportGame() failed on GameID {self.GameID}: {e}')
    
    def ImportTeams(self):

        try:
            self.Writer.run(
                f"""
                MERGE INTO mlb.DimTeams AS target 
                USING ( 
                    SELECT {self.HomeID} as TeamID,
                    {self.HomeName} as TeamName,
                    '{datetime.date.today()}' as Modified
                )
                AS source 
                ON target.TeamID = source.TeamID
                
                WHEN MATCHED THEN 
                UPDATE SET 
                    TeamID = source.TeamID, 
                    TeamName = source.TeamName,
                    Modified = source.Modified
                    
                WHEN NOT MATCHED THEN 
                INSERT (TeamID, TeamName, Modified)
                VALUES (source.TeamID, source.TeamName, source.Modified);
                """ 
            )

        except Exception as e:
            print(f'BatchGameImporter.ImportTeams() failed on TeamID {self.HomeID}, TeamName {self.HomeName}: {e}')

        try:
            self.Writer.run(
                f"""
                MERGE INTO mlb.DimTeams AS target 
                USING ( 
                    SELECT {self.AwayID} as TeamID,
                    {self.AwayName} as TeamName,
                    '{datetime.date.today()}' as Modified
                )
                AS source 
                ON target.TeamID = source.TeamID
                
                WHEN MATCHED THEN 
                UPDATE SET 
                    TeamID = source.TeamID, 
                    TeamName = source.TeamName,
                    Modified = source.Modified
                    
                WHEN NOT MATCHED THEN 
                INSERT (TeamID, TeamName, Modified)
                VALUES (source.TeamID, source.TeamName, source.Modified);
                """ 
            )

        except Exception as e:
            print(f'BatchGameImporter.ImportTeams() failed on TeamID {self.AwayID}, TeamName {self.AwayName}: {e}')
        
        self.CurrentTeams = self.Writer.run("SELECT * FROM mlb.DimTeams")

    def ImportVenues(self):

        try:
            self.Writer.run(
                f"""
                MERGE INTO mlb.DimVenues AS target 
                USING ( 
                    SELECT {self.VenueID} as VenueID,
                    '{self.VenueName}' as VenueName
                )
                AS source 
                ON target.VenueID = source.VenueID
                
                WHEN MATCHED THEN 
                UPDATE SET 
                    VenueID = source.VenueID, 
                    VenueName = source.VenueName
                    
                WHEN NOT MATCHED THEN 
                INSERT (VenueID, VenueName)
                VALUES (source.VenueID, source.VenueName);
                """ 
            )

        except Exception as e:
            print(f'BatchGameImporter.ImportVenues() failed on VenueID {self.VenueID}, VenueName {self.VenueName}: {e}')

    def DailyBattingStats(self):

        ### Pull Data for Home Players
        homePlayers = pd.DataFrame(self.BoxScore['homeBatters']).iloc[1:]
        homePlayers['TeamID'] = self.HomeID
        # set as a proper datetime on the dataframe
        homePlayers['Date'] = pd.to_datetime(self.GameDate)
        homePlayers['IsHome'] = 1
        homePlayers['OpposingTeamID'] = self.AwayID
        homePlayers['GameID'] = self.GameID
        homePlayers = self.frameBatters(homePlayers)
        self.homeBatterIDs = homePlayers['PlayerID'].tolist()
        if homePlayers is not None and len(homePlayers) > 0:
            try: 
                homePlayers.to_sql('StagingGameLogBatting', con=self.Writer.bulkConnection, schema='MLB', if_exists='append', index=False)
            except pyodbc.IntegrityError: 
                print(f'BatchGameImporter.DailyBattingStats() failed on GameID {self.GameID}, HomeTeamID {self.HomeID}: Staging Data Already Exists. Moving on.')
            except Exception as e: 
                print(f'BatchGameImporter.DailyBattingStats() failed on GameID {self.GameID}, HomeTeamID {self.HomeID}: {e}')

        del homePlayers

        ### Pull Data for Away Players
        awayPlayers = pd.DataFrame(self.BoxScore['awayBatters']).iloc[1:]
        awayPlayers['TeamID'] = self.AwayID
        awayPlayers['Date'] = pd.to_datetime(self.GameDate)
        awayPlayers['IsHome'] = 0
        awayPlayers['OpposingTeamID'] = self.HomeID
        awayPlayers['GameID'] = self.GameID
        awayPlayers = self.frameBatters(awayPlayers)
        self.awayBatterIDs = awayPlayers['PlayerID'].tolist()
        if awayPlayers is not None and len(awayPlayers) > 0:
            try: 
                awayPlayers.to_sql('StagingGameLogBatting', con=self.Writer.bulkConnection, schema='MLB', if_exists='append', index=False)
            except pyodbc.IntegrityError: 
                print(f'BatchGameImporter.DailyBattingStats() failed on GameID {self.GameID}, AwayTeamID {self.AwayID}: Staging Data Already Exists. Moving on.')
            except Exception as e: 
                print(f'BatchGameImporter.DailyBattingStats() failed on GameID {self.GameID}, AwayTeamID {self.AwayID}: {e}')

        del awayPlayers 

    def DailyPitchingStats(self):
        
        ### Pull Data for Home Pitchers
        homePitchers = pd.DataFrame(self.BoxScore['homePitchers']).iloc[1:]
        homePitchers['TeamID'] = self.HomeID
        homePitchers['Date'] = pd.to_datetime(self.GameDate)
        homePitchers['IsHome'] = 1
        homePitchers['OpposingTeamID'] = self.AwayID
        homePitchers['GameID'] = self.GameID
        homePitchers = self.framePitchers(homePitchers)
        self.homePicherIDs = homePitchers['PlayerID'].tolist()
        if homePitchers is not None and len(homePitchers) > 0:
            try: 
                homePitchers.to_sql('StagingGameLogPitching', con=self.Writer.bulkConnection, schema='MLB', if_exists='append', index=False)
            except pyodbc.IntegrityError: 
                print(f'BatchGameImporter.DailyPitchingStats() failed on GameID {self.GameID}, HomeTeamID {self.HomeID}: Staging Data Already Exists. Moving on.')
            except Exception as e:
                print(f'BatchGameImporter.DailyPitchingStats() failed on GameID {self.GameID}, HomeTeamID {self.HomeID}: {e}')

        del homePitchers

        ### Pull Data for Away Pitchers
        awayPitchers = pd.DataFrame(self.BoxScore['awayPitchers']).iloc[1:]
        awayPitchers['TeamID'] = self.AwayID
        awayPitchers['Date'] = pd.to_datetime(self.GameDate)
        awayPitchers['IsHome'] = 0
        awayPitchers['OpposingTeamID'] = self.HomeID
        awayPitchers['GameID'] = self.GameID
        awayPitchers = self.framePitchers(awayPitchers)
        self.awayPicherIDs = awayPitchers['PlayerID'].tolist()
        if awayPitchers is not None and len(awayPitchers) > 0:
            try: 
                awayPitchers.to_sql('StagingGameLogPitching', con=self.Writer.bulkConnection, schema='MLB', if_exists='append', index=False)
            except pyodbc.IntegrityError: 
                print(f'BatchGameImporter.DailyPitchingStats() failed on GameID {self.GameID}, AwayTeamID {self.AwayID}: Staging Data Already Exists. Moving on.')
            except Exception as e: 
                print(f'BatchGameImporter.DailyPitchingStats() failed on GameID {self.GameID}, AwayTeamID {self.AwayID}: {e}')

        del awayPitchers

    def ImportPlayers(self):

        for i in self.homeBatterIDs + self.awayBatterIDs + self.homePicherIDs + self.awayPicherIDs:
                
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
                self.Writer.run(
                    f"""
                    MERGE INTO mlb.DimPlayers AS target 
                    USING ( 
                        SELECT {PlayerID} as PlayerID,
                        '{PlayerName}' as PlayerName,
                        '{PlayerNameFirst}' as PlayerNameFirst,
                        '{PlayerNameLast}' as PlayerNameLast,
                        {CurrentTeamID} as CurrentTeamID,
                        '{Position}' as Position,
                        '{BatHand}' as BatHand,
                        '{ThrowHand}' as ThrowHand,
                        {IsActive} as IsActive
                    )
                    AS source 
                    ON target.PlayerID = source.PlayerID
                
                    WHEN MATCHED THEN 
                    UPDATE SET 
                        PlayerID = source.PlayerID, 
                        PlayerName = source.PlayerName,
                        PlayerNameFirst = source.PlayerNameFirst,
                        PlayerNameLast = source.PlayerNameLast,
                        CurrentTeamID = source.CurrentTeamID,
                        Position = source.Position,
                        BatHand = source.BatHand,
                        ThrowHand = source.ThrowHand,
                        IsActive = source.IsActive
                    
                    WHEN NOT MATCHED THEN 
                    INSERT (PlayerID, PlayerName, PlayerNameFirst, PlayerNameLast, CurrentTeamID, Position, BatHand, ThrowHand, IsActive)
                    VALUES (source.PlayerID, source.PlayerName, source.PlayerNameFirst, source.PlayerNameLast, source.CurrentTeamID, source.Position, source.BatHand, source.ThrowHand, source.IsActive);
                    """ 
                )

            except Exception as e:
                print(f'BatchGameImporter.ImportPlayers() failed on PlayerID {PlayerID}: {e}')

    @staticmethod
    def toGameTypeID(val):
        if val == 'R':
            return 1
        elif val in ['F', 'D', 'L', 'W', 'P']:
            return 2
        elif val in ['S']:
            return 4
        else:
            raise Exception(f'Invalid GameType: {val}')

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
    def frameBatters(dat: pd.DataFrame):

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
    
    @staticmethod
    def framePitchers(dat: pd.DataFrame):

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


    def getTeamID(self, val):
        
        try: teamID = self.currentTeams[self.currentTeams['TeamName'] == val]['TeamID'].values[0]
        except: teamID = 0
        return teamID