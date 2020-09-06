CREATE KEYSPACE IF NOT EXISTS epl
WITH replication = {
'class': 'SimpleStrategy', 'replication_factor': 1
};

use epl;

CREATE TABLE IF NOT EXISTS epl_match_summary (
Division text,
Date text,
HomeTeam text,
AwayTeam text,
FullTimeHomeGoal double,
FullTimeAwayGoal double,
FullTimeResult text,
HalfTimeHomeGoal double,
HalfTimeAwayGoal double,
HalfTimeResult text,
PRIMARY KEY(Division,Date,HomeTeam,AwayTeam)
)WITH CLUSTERING ORDER BY (date DESC);


