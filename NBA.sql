DROP SCHEMA IF EXISTS NBA CASCADE;


create schema NBA;
drop table if exists NBA.teams CASCADE;
drop table if exists NBA.players CASCADE;
drop table if exists NBA.current_season_result CASCADE;
drop table if exists NBA.current_career_result CASCADE;

CREATE TABLE NBA.teams(
  "team_code" varchar PRIMARY KEY,
  "team_name" varchar,
  "wins" int,
  "loss" int,
  "playoff" int,
  "is_eastern" int
);

CREATE TABLE NBA.players(
  "id" Varchar PRIMARY KEY,
  "name" varchar,
  "team_code" varchar references NBA.teams("team_code"),
  "birth_year" int,
  "age" int,
  "number" int,
  "possition" varchar,
  "guaranteed" int,
  "height" float,
  "weight" float,
  "exp" float
);


CREATE TABLE NBA.current_season_result(
  "id" varchar references NBA.players("id"),
  "G" float,
  "GS" float,
  "MP" float,
  "FGperc" float,
  "3Pperc" float,
  "2Pperc" float,
  "eFGperc" float,
  "FTperc" float,
  "ORB" float,
  "DRB" float,
  "AST" float,
  "STL" float,
  "BLK" float,
  "TOV" float,
  "PF" float,
  "PTS" float
);

CREATE TABLE NBA.current_career_result(
  "id" varchar references NBA.players("id"),
  "G" float,
  "GS" float,
  "MP" float,
  "FGperc" float,
  "3Pperc" float,
  "2Pperc" float,
  "eFGperc" float,
  "FTperc" float,
  "ORB" float,
  "DRB" float,
  "AST" float,
  "STL" float,
  "BLK" float,
  "TOV" float,
  "PF" float,
  "PTS" float
);
