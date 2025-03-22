# Database Documentation

## Overview

- This is a MLB database that I have designed for game logs and player stats in the world series era.
- Generally attempted to normalize the database to reduce redundancy due to scale of the dataset.

## Tables

### Table: Games

- **Description**: General game entry most tables are linked to this in some form or another
- **Columns**:
  - **h_team_id**: Integer, Not null - ID of the home team
  - **a_team_id**: Integer, Not null - ID of the away team
  - **season_year**: Integer, GENERATED ALWAYS AS (EXTRACT(year FROM date)) STORED - Calculated field for the season year
- **Keys and Indexes**:
  - **Primary Key**
    - ***game_id***: Identifier of each individual game, reused mlbs gamePK
  - **Foreign Keys**
    -***h_team_id***: references teams(team_id)

### Table: Teams

- **Description**: Contains information about MLB teams
- **Columns**:
  - **team_name**: Text, Not null - Full name of the team(Ex. Boston Red Sox)
  - **club_name**: Text, Not null - Name of the club(Ex. Red Sox)
  - **team_abr**: Text, Not null - Abbreviation of the team(Ex. BOS)
  - **team_location**: Text, Not null - Location of the team(Ex. Boston)
  - **first_year**: Integer - First year of team operation
- **Keys and Indexes**:
  - **Primary Key**
    - ***team_id***: Integer, Not null - Unique identifier for each team

### Table: Team_Seasoons

- **Description**: Contains information about MLB teams for a specific season
- **Columns**:
  - **team_name**: Text - Teams name for the season
  - **venue_id**: Integer - Identifier for the teams home venue
  - **league_id**: Integer - Identifier for the teams league
  - **spring_venue_id**: Integer - Identifier for the teams home spring training venue
  - **spring_league_id**: Integer - Indetifier for the teams spring training league
  - **division_id**: Integer - Identifier for the teams division
  - **level_id**: Integer - Identifier for the level the team playes in
- **Keys and Indexes**:
  - **Primary Keys**
    - ***team_id***: Integer, Not null - Unique Identifier for the team
    - ***season_year***: Integer, Not null - Year the season took place
  - **Foreign Keys**
    - ***team_id***: References teams(team_id)
    - ***venue_id***: References venues(venue_id)
    - ***league_id***: References leagues(league_id)
    - ***spring_venue_id***: References venues(venue_id)
    - ***spring_league_id***: References leagues(league_id)
    - ***division_id***: References divisions(division_id)
    - ***level_id***: References levels(level_id)

### Table: Players

- **Description**: Player information and attributes
- **Columns**:
  - **player_id**: Integer, NOT NULL - Unique identifier for each player
  - **sz_bot**: Numeric, NOT NULL - Bottom of strike zone measurement
  - **[Additional player fields not shown in schema excerpt]**
- **Keys and Indexes**:
  - **Primary Key**: player_id

### Table: Venues

- **Description**: Basic name and identifier for each venue
- **Columns**:
  - **venue_name**: Text - Name of the venue
- **Keys and Indexes**:
  - **Primary Key**
    - ***venue_id***: Integer - Unique identifier for the Venue

### Table: Innings

- **Description**: Data about individual innings in games
- **Columns**:
  - **inning_num**: Integer, NOT NULL - Inning number
  - **inning_half**: CHAR(1) - 'T' for top or 'B' for bottom
  - **[Additional inning fields not shown in schema excerpt]**
- **Keys and Indexes**:
  - **Constraints**: inning_half must be either 'T' or 'B'

### Table: Pitches

- **Description**: Individual pitch information
- **Columns**:
  - **inning_num**: Integer, NOT NULL
  - **inning_half**: CHAR(1) - 'T' for top or 'B' for bottom
  - **[Additional pitch fields not shown in schema excerpt]**
- **Keys and Indexes**:
  - **Constraints**: inning_half must be either 'T' or 'B'

### Table: At_Bats

- **Description**: Information about individual at-bats
- **Columns**:
  - **inning_num**: Integer, NOT NULL
  - **inning_half**: CHAR(1) - 'T' for top or 'B' for bottom
  - **[Additional at-bat fields not shown in schema excerpt]**
- **Keys and Indexes**:
  - **Constraints**: inning_half must be either 'T' or 'B'

### Table: Pitch_Type

- **Description**: Lookup table for pitch types
- **Columns**:
  - **pitch_type_id**: Integer, NOT NULL - Unique identifier
  - **pitch_type_name**: Text, NOT NULL - Name of the pitch type (fastball, slider, etc.)
- **Keys and Indexes**:
  - **Primary Key**: pitch_type_id

### Table: Pitch_Data

- **Description**: Advanced metrics for individual pitches
- **Columns**:
  - **pitch_data_id**: Integer, NOT NULL - Unique identifier
  - **extension**: Numeric - Pitcher's extension measurement
  - **[Additional pitch data fields not shown in schema excerpt]**
- **Keys and Indexes**:
  - **Primary Key**: pitch_data_id

### Table: Bip_Data

- **Description**: Ball-in-play data
- **Columns**:
  - **bip_data_id**: Integer, NOT NULL - Unique identifier
  - **bat_speed**: Numeric - Bat speed measurement
  - **[Additional BIP data fields not shown in schema excerpt]**
- **Keys and Indexes**:
  - **Primary Key**: bip_data_id

### Table: Divisions

- **Description**: MLB divisions (American League East, etc.)
- **Columns**:
  - **division_name**: Text, Not null - Name of the division
- **Keys and Indexes**:
  - **Primary Key**
    - ***division_id***: Integer, Not null - Unique identifier for the division

### Table: Leagues

- **Description**: MLB leagues (American League, National League)
- **Columns**:
  - **league_name**: Text, Not null - Name of the league
- **Keys and Indexes**:
  - **Primary Key**
    - ***league_id***: Integer, Not null - Unique identifier for the league

### Table: Levels

- **Description**: Level of Play (MLB, AAA, AA, A+, A)
- **Columns**
  - **level_abbreviation**: Char, Not null - Abbreviation of the level
- **Keys and Indexes**:
  - **Primary Key**
    - ***level_id***: Integer, Not null - Unique identifier for the level

### Table: Game_Types

- **Description**: Type of game (Spring Training, Regular Season, etc)
- **Columns**
  - **type_name**: Text, Not null - Name of the game type
- **Keys and Indexes**:
  - **Primary Key**
    - ***type_id***: Integer, Not null - Unique identifier for the type

## Views

### View: [View Name]

- **Description**: Explain the purpose of this view.
- **Source Tables**: List tables that the view relies on.
- **Key Columns**:
  - **[Column Name]**: Derived from [Table Name] (include any transformations)

## Functions & Stored Procedures

### Function: update_was_used()

- **Description**: Trigger function that updates the was_used field in the roster_games table.
- **Parameters**: None (triggered function)
- **Returns**: trigger
- **Logic Summary**: Updates roster_games table, presumably to mark when a player has been used in a game.

## Additional Notes

- The database makes extensive use of normalized tables for different aspects of baseball games, including separate tables for pitching, hitting, and fielding statistics.
- Some tables appear to track advanced metrics such as pitch data and ball-in-play data.
- The schema supports multiple organizational levels (divisions, leagues) and seasons.
