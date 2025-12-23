
DROP TABLE IF EXISTS FactMLBPlayerPitchingPerformance

CREATE TABLE FactMLBPlayerPitchingPerformance (
    PlayerID INT,
    Season INT,
    [Date] Date,
    GamesPlayed INT,
    IP FLOAT,
    Hits INT,
    Runs INT,
    EarnedRuns INT,
    BB INT,
    K INT,
    HR INT,
    Pitches INT,
    Strikes INT,
    ERA FLOAT,
    WHIP FLOAT,
    BBPercent FLOAT,
    KPercent FLOAT
)

ALTER TABLE FactMLBPlayerPitchingPerformance ADD CONSTRAINT UQ_FactMLBPlayerPitchingPerformance UNIQUE (PlayerID, [Date])
