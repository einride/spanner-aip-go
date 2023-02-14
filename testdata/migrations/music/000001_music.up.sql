CREATE TABLE Labels (
   LabelId    INT64 NOT NULL,
   LabelName  STRING(MAX),
) PRIMARY KEY (LabelId);

CREATE TABLE Singers (
  SingerId   INT64 NOT NULL,
  LabelId    INT64,
  FirstName  STRING(1024),
  LastName   STRING(1024),
  SingerInfo BYTES(MAX),
) PRIMARY KEY (SingerId);

CREATE TABLE Albums (
  SingerId     INT64 NOT NULL,
  AlbumId      INT64 NOT NULL,
  AlbumTitle   STRING(MAX),
) PRIMARY KEY (SingerId, AlbumId),
  INTERLEAVE IN PARENT Singers ON DELETE CASCADE;

CREATE TABLE Songs (
  SingerId     INT64 NOT NULL,
  AlbumId      INT64 NOT NULL,
  TrackId      INT64 NOT NULL,
  SongName     STRING(MAX),
) PRIMARY KEY (SingerId, AlbumId, TrackId),
  INTERLEAVE IN PARENT Albums ON DELETE CASCADE;

ALTER TABLE Singers
    ADD CONSTRAINT FK_LabelSinger FOREIGN KEY (LabelId) REFERENCES Labels (LabelId);
