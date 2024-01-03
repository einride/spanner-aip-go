-- This migration should result in no schema changes.
CREATE TABLE Genres (
                        SingerId     INT64 NOT NULL,
                        GenreId      INT64 NOT NULL,
) PRIMARY KEY (SingerId, GenreId),
  INTERLEAVE IN PARENT Singers ON DELETE CASCADE;

DROP TABLE Genres;
