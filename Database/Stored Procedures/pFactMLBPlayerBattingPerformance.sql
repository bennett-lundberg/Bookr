CREATE PROCEDURE pFactMLBPlayerBattingPerformance
    @PlayerID INT,
    @Season INT
AS
BEGIN
    -- Delete existing records for the specified player and season
    DELETE FROM FactMLBPlayerBattingPerformance
    WHERE PlayerID = @PlayerID AND Season = @Season

    -- Create a temporary table to hold staging data for the specified player and season
    SELECT A.*
    INTO #stagingTemp
    FROM mlb.StagingGameLogBatting A
    JOIN mlb.DimGames B ON A.GameID = B.GameID
    WHERE PlayerID = @PlayerID AND YEAR(Date) = @Season

    -- Create a temporary table to hold cumulative data
    SELECT PlayerID,
        YEAR(Date) 'Season',
        Date,
        COUNT(GameID) OVER (ORDER BY Date) 'GamesPlayed',
        SUM(AB) OVER (ORDER BY Date) 'AB',
        SUM(Runs) OVER (ORDER BY Date) 'Runs',
        SUM(Hits) OVER (ORDER BY Date) 'Hits',
        SUM(Doubles) OVER (ORDER BY Date) 'Doubles',
        SUM(Triples) OVER (ORDER BY Date) 'Triples',
        SUM(HR) OVER (ORDER BY Date) 'HR',
        SUM(RBI) OVER (ORDER BY Date) 'RBI',
        SUM(SB) OVER (ORDER BY Date) 'SB',
        SUM(BB) OVER (ORDER BY Date) 'BB',
        SUM(K) OVER (ORDER BY Date) 'K'
    INTO #cumulativeTemp
    FROM #stagingTemp
    ORDER BY Date

    SELECT *,
        CONVERT(DECIMAL(18,3), Hits) / (AB + BB) 'AVG',
        CONVERT(DECIMAL(18,3), Hits) / (AB + BB) 'OBP',
        CONVERT(decimal(18,3), Hits + Doubles + 2*Triples + 3*HR) / (AB + BB) 'SLG'
    INTO #finalTemp
    FROM #cumulativeTemp

    -- Insert the cumulative data into the FactMLBPlayerBattingPerformance table
    INSERT INTO FactMLBPlayerBattingPerformance
    SELECT * FROM #finalTemp

    DROP TABLE IF EXISTS #stagingTemp
    DROP TABLE IF EXISTS #cumulativeTemp
    DROP TABLE IF EXISTS #finalTemp
        
END