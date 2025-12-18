
DROP TABLE IF EXISTS StagingMLBGameLogBatting

CREATE TABLE StagingMLBGameLogBatting (
    PlayerID INT NOT NULL,
    [Date] Date NOT NULL,
    GameID INT NOT NULL,
    TeamID INT NOT NULL,
    OpposingTeamID INT NOT NULL,
    IsHome INT NOT NULL,
    Position VARCHAR(5),
    BattingOrder INT,
    AB INT,
    Runs INT,
    Hits INT,
    Doubles INT,
    Triples INT,
    HR INT,
    RBI INT,
    SB INT,
    BB INT,
    K INT,
    LOB INT,
    AVG FLOAT,
    OBP FLOAT,
    SLG FLOAT,
    OPS FLOAT
)

ALTER TABLE StagingMLBGameLogBatting ADD CONSTRAINT UqPlayerGameBattingConstraint UNIQUE (PlayerID, GameID)