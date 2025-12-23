
DROP TABLE IF EXISTS MLB.DimGames

CREATE TABLE MLB.DimGames (
    GameID INT PRIMARY KEY,
    HomeTeamID INT NOT NULL,
    AwayTeamID INT NOT NULL,
    HomeScore INT NOT NULL,
    AwayScore INT NOT NULL,
    GameTypeID INT NOT NULL,
    GameDate DATE NOT NULL,
    Innings INT NOT NULL,
    VenueID INT NOT NULL
)