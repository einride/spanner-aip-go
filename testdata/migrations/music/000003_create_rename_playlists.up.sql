CREATE TABLE Mixtapes (
   Id     INT64 NOT NULL,
) PRIMARY KEY (Id);

ALTER TABLE Mixtapes
    RENAME TO Playlists;
