DROP TABLE IF EXISTS MLB.DimGameType

CREATE TABLE MLB.DimGameType (
    GameTypeID INT PRIMARY KEY,
    GameType NVARCHAR(100) NOT NULL,
)

INSERT INTO MLB.DimGameType SELECT 1, 'Regular Season'
INSERT INTO MLB.DimGameType SELECT 2, 'Postseason'
INSERT INTO MLB.DimGameType SELECT 4, 'Spring Training'