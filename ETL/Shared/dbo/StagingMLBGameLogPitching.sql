
DROP TABLE IF EXISTS StagingMLBGameLogPitching

CREATE TABLE StagingMLBGameLogPitching (
    PlayerID INT NOT NULL,
    [Date] Date NOT NULL,
    GameID INT NOT NULL,
    TeamID INT NOT NULL,
    OpposingTeamID INT NOT NULL,
    IsHome INT NOT NULL,
    IP FLOAT,
    Hits INT,
    Runs INT,
    EarnedRuns INT,
    BB INT,
    K INT,
    HR INT,
    ERA FLOAT,
    Pitches INT,
    Strikes INT
)

ALTER TABLE StagingMLBGameLogPitching ADD CONSTRAINT UqPlayerGamePitchingConstraint UNIQUE (PlayerID, GameID)