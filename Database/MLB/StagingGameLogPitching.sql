
DROP TABLE IF EXISTS MLB.StagingGameLogPitching

CREATE TABLE MLB.StagingGameLogPitching (
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

ALTER TABLE MLB.StagingGameLogPitching ADD CONSTRAINT UqPlayerGamePitchingConstraint UNIQUE (PlayerID, GameID)