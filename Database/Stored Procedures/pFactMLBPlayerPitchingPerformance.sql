CREATE PROCEDURE pFactMLBPlayerPitchingPerformance
    @PlayerID INT,
    @Season INT
AS
BEGIN
    -- Delete existing records for the specified player and season
    DELETE FROM FactMLBPlayerPitchingPerformance
    WHERE PlayerID = @PlayerID AND Season = @Season

    -- Create a temporary table to hold staging data for the specified player and season
    SELECT A.*
    INTO #stagingTemp
    FROM mlb.StagingGameLogPitching A
    JOIN mlb.DimGames B ON A.GameID = B.GameID
    WHERE PlayerID = @PlayerID AND YEAR(Date) = @Season

    -- Create a temporary table to hold cumulative data
    SELECT PlayerID,
        YEAR(Date) 'Season',
        Date,
        COUNT(GameID) OVER (ORDER BY Date) 'GamesPlayed',
        SUM(IP) OVER (ORDER BY Date) 'AB',
        SUM(Hits) OVER (ORDER BY Date) 'Hits',
        SUM(Runs) OVER (ORDER BY Date) 'Runs',
        SUM(EarnedRuns) OVER (ORDER BY Date) 'EarnedRuns',
        SUM(BB) OVER (ORDER BY Date) 'BB',
        SUM(K) OVER (ORDER BY Date) 'K',
        SUM(HR) OVER (ORDER BY Date) 'HR',
        SUM(Pitches) OVER (ORDER BY Date) 'Pitches',
        SUM(Strikes) OVER (ORDER BY Date) 'Strikes'
    INTO #cumulativeTemp
    FROM #stagingTemp
    ORDER BY Date

    SELECT *,
        EarnedRuns*9.0 / IP 'ERA',
        CONVERT(FLOAT, BB + Hits) / IP 'WHIP',
        CONVERT(FLOAT, BB) / IP 'BBPercent',
        CONVERT(FLOAT, K) / IP 'KPercent'
    INTO #finalTemp
    FROM #cumulativeTemp

    -- Insert the cumulative data into the FactMLBPlayerBattingPerformance table
    INSERT INTO FactMLBPlayerPitchingPerformance
    SELECT * FROM #finalTemp

    DROP TABLE IF EXISTS #stagingTemp
    DROP TABLE IF EXISTS #cumulativeTemp
    DROP TABLE IF EXISTS #finalTemp
        
END