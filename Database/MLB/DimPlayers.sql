
DROP TABLE IF EXISTS MLB.DimPlayers

CREATE TABLE MLB.DimPlayers (
    PlayerID INT PRIMARY KEY,
    PlayerName VARCHAR(100) NOT NULL,
    PlayerNameFirst VARCHAR(50) NOT NULL,
    PlayerNameLast VARCHAR(50) NOT NULL,
    CurrentTeamID INT,
    Position VARCHAR(5),
    BatHand VARCHAR(1),
    ThrowHand VARCHAR(1),
    IsActive INT
)