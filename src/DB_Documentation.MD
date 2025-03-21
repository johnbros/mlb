# Database Documentation

## Overview

- This is a MLB database that I have designed for game logs and player stats in the world series era.
- Generally attempted to normalize the database to reduce redundancy due to scale of the dataset.

## Tables

### Table: Games

- **Description**: General game entry most tables are linked to this in some form or another
- **Columns**:
  - **h_team_id**: Integer, NOT NULL - ID of the home team
  - **season_year**: Integer, GENERATED ALWAYS AS (EXTRACT(year FROM date)) STORED - Calculated field for the season year
  - **[Additional game fields not shown in schema excerpt]**
- **Keys and Indexes**:
  - **Primary Key**: Likely a game_id field (referenced in other tables but not shown in the excerpt)
  - **Foreign Keys**: h_team_id references teams(team_id)

### Table: Teams

- **Description**: Contains information about MLB teams
- **Columns**:
  - **team_id**: Integer, NOT NULL - Unique identifier for each team
  - **first_year**: Integer - First year of team operation
  - **[Additional team fields not shown in schema excerpt]**
- **Keys and Indexes**:
  - **Primary Key**: team_id

### Table: Players

- **Description**: Player information and attributes
- **Columns**:
  - **player_id**: Integer, NOT NULL - Unique identifier for each player
  - **sz_bot**: Numeric, NOT NULL - Bottom of strike zone measurement
  - **[Additional player fields not shown in schema excerpt]**
- **Keys and Indexes**:
  - **Primary Key**: player_id

### Table: Venues

- **Description**: Information about baseball stadiums and venues
- **Columns**:
  - **venue_id**: Integer, NOT NULL - Unique identifier for each venue
  - **venue_name**: Text - Name of the venue
- **Keys and Indexes**:
  - **Primary Key**: venue_id

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

- **Description**: MLB divisions
- **Columns**:
  - **division_id**: Integer, NOT NULL - Unique identifier
  - **division_name**: Text, NOT NULL - Name of the division
- **Keys and Indexes**:
  - **Primary Key**: division_id

### Table: Leagues

- **Description**: MLB leagues (AL, NL)
- **Columns**:
  - **league_id**: Integer, NOT NULL - Unique identifier
  - **league_name**: Text, NOT NULL - Name of the league
- **Keys and Indexes**:
  - **Primary Key**: league_id

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
