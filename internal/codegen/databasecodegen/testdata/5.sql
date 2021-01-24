CREATE TABLE UserAccessLog (
  UserId     INT64 NOT NULL,
  LastAccess TIMESTAMP NOT NULL,
) PRIMARY KEY (UserId, LastAccess);
