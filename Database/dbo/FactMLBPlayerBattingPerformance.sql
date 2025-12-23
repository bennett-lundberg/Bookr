
DROP TABLE IF EXISTS FactMLBPlayerBattingPerformance

CREATE TABLE FactMLBPlayerBattingPerformance (
    PlayerID INT,
    Season INT,
    [Date] Date,
    GamesPlayed INT,
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
    AVG FLOAT,
    OBP FLOAT,
    SLG FLOAT,
    OPS FLOAT
)

ALTER TABLE FactMLBPlayerBattingPerformance ADD CONSTRAINT UQ_FactMLBPlayerBattingPerformance UNIQUE (PlayerID, [Date])