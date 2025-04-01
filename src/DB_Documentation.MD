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
  - **season_year**: Integer, Not null - Year of the game
  - **venue_id**: Integer, Not null - ID of the Venue
  - **level_id**: Integer, Not null - ID of the level of play (Ex. MLB, AAA, AA, etc.)
  - **type_id**: Integer, Not null - ID of the type of game (Ex. Regular Season)
  - **date**: Date, Not null - Official date of the game
  - **game_time**: Time, Not null - Time the game was played
  - **weather**: Text - Description of the weather
  - **wind**: Text - Speed and Direction of the wind
- **Keys and Indexes**:
  - **Primary Key**
    - ***game_id***: Identifier of each individual game, reused mlbs gamePK
  - **Foreign Keys**
    -***(h_team_id, season_year)***: references team_seasons(team_id, season_year)
    -***(a_team_id, season_year)***: references team_seasons(team_id, season_year)
    -***venue_id***: references venues(venue_id)
    -***level_id***: references levels(level_id)
    -***type_id***: references types(type_id)

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

### Table: Team_Seasons

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

### Table: Team_Games

- **Description**: Contains basic information about a teams game stats
- **Columns**:
  - **runs_scored**: Integer - number of runs the team scored
  - **hits**: Integer - number of hits the team had
  - **errors**: Integer - number of errors the team had
  - **earned_runs_allowed**: Integer - number of earned runs the team allowed(1912 onward? double check this)
  - **total_pitches_thrown**: Integer - number of pitches thrown(1950 onward? double check this)
  - **innings_pitched**: Integer - number of innings pitched by the team
  - **runs_allowed**: Integer - total number of runs allowed earned and unearned
  - **rbis**: Integer - total number of runs batted in
  - **strikeouts**: Integer - number of times the team struck out
  - **walks**: Integer - number of times the team walked
  - **home_runs**: Integer - number of home runs the team hit in the game
  - **doubles**: Integer - number of doubles the team hit in the game
  - **triples**: Integer - number of triples the team hit in the game
  - **struck_out**: Integer - number of batters the teams pitchers struck out
  - **walked**: Integer - number of batters the teams pitchers walked
  - **home_runs_allowed**: Integer - number of home runs given up
  - **doubles_allowed**: Integer - number of doubles given up
  - **triples_allowed**: Integer - number of triples given up
  - **hits_allowed**: Integer - number of hits given up
- **Keys and Indexes**:
  - **Primary Keys**
    - ***game_id***: Integer, Not null - identifier of the game
    - ***team_id***: Integer, Not null - identifier of the team
  - **Foreign Keys**
    - ***game_id***: References games(game_id)
    - ***team_id***: References teams(team_id)

### Table: Roster_Games

- **Description**: Contains player ids for players that were on the roster for a game and if they were used
- **Columns**:
  - **was_used**: Boolean - indicator of whether or not the player was used
- **Keys and Indexes**:
  - **Primary Keys**
    - ***player_id***: Integer, Not null - Unique identifier of the player
    - ***game_id***: Integer, Not null - Unique identifier of the game
    - ***team_id***: Integer, Not null - Unique identifier of the team the player was on in this game
  - **Foreign Keys**
    - ***player_id***: References players(player_id)
    - ***game_id***: References games(game_id)
    - ***team_id***: References teams(team_id)

### Table: Player_Games_Fielding

- **Description**: Contains players stats in the field for a specific game
- **Columns**:
  - **putouts**: Integer - Number of putouts the player is credited with
  - **assists**: Integer - Number of assists the player is credited with
  - **errors**: Integer - Number of errors the player is credited with
  - **chances**: Integer - Calculated from Putouts + Assists + Errors
  - **caught_stealing**: Integer - Number of players the player threw out stealing
  - **stolen_bases_allowed**: Integer - Number of players the player allowed to steal a base
  - **passed_balls**: Integer - Number of balls the player let by (Imagining this is a catcher only stat)
  - **pickoffs**: Integer - Number of times a player picked off a player on base (Imagining this is a pitcher only stat)
  - **position_id**: Integer, Not null - Identifier of the position the player was playing
- - **Keys and Indexes**:
  - **Primary Keys**
    - ***player_id***: Integer, Not null - Unique identifier of the player
    - ***game_id***: Integer, Not null - Unique identifier of the game
    - ***team_id***: Integer, Not null - Unique identifier of the team the player was on in this game
  - **Foreign Keys**
    - ***player_id***: References players(player_id)
    - ***game_id***: References games(game_id)
    - ***team_id***: References teams(team_id)
    - ***position_id***: References positions(position_id)

### Table: Player_Games_Hitting

- **Description**: Contains players offensive stats for a specific game
- **Columns**:
  - **at_bats**: Integer - Number of at bats the player had
  - **hits**: Integer - Number of hits the player had
  - **walks**: Integer - Number of walks the player had
  - **total_bases**: Integer - Sum of bases the player had from hits (Single = 1, Double = 2, etc)
  - **rbis**: Integer - Number of RBIs the player had
  - **hbp**: Integer - Number of times the player was hit by a pitch
  - **hrs**: Integer - Number of home runs the player hit
  - **doubles**: Integer - Number of doubles the player hit
  - **triples**: Integer - Number of triples the player hit
  - **strikeouts**: Integer - Number of strikeouts the player had
  - **plate_appearances**: Integer - Number of plate appearances the player had
  - **runs**: Integer - Number of runs the player scored
  - **stolen_bases**: Integer - Number of bases the player stole
  - **caught_stealing**: Integer - Number of times the player was caught stealing
  - **ground_into_double_play**: Integer - Number of times the player grounded into a double play
  - **sac_bunts**: Integer - Number of sacrifice bunts the player had
  - **sac_flies**: Integer - Number of sacrifice flies the player had
  - **left_on_base**: Integer - Number of times the player was left on base
  - **position_id**: Integer, Not null - Identifier of the position the player was playing
  - **ground_into_triple_play**: Integer - Number of times the player grounded into a triple play
  - **line_outs**: Integer - Number of times the player lined out
  - **pop_outs**: Integer - Number of times the player popped out
  - **fly_outs**: Integer - Number of times the player flied out
  - **air_outs**: Integer - Number of times the player aired out
  - **ground_outs**: Integer - Number of times the player grounded out
  - **catchers_interference**: Integer - Number of times the at bat ended in catchers interference
- - **Keys and Indexes**:
  - **Primary Keys**
    - ***player_id***: Integer, Not null - Unique identifier of the player
    - ***game_id***: Integer, Not null - Unique identifier of the game
    - ***team_id***: Integer, Not null - Unique identifier of the team the player was on in this game
  - **Foreign Keys**
    - ***player_id***: References players(player_id)
    - ***game_id***: References games(game_id)
    - ***team_id***: References teams(team_id)
    - ***position_id***: References positions(position_id)

### Table: Player_Games_Pitching

- **Description**: Contains players offensive stats for a specific game
- **Columns**:
  - **innings_pitched**: Numeric - Number of innings the player pitched
  - **batters_faced**: Integer - Number of batters the player faced
  - **earned_runs**: Integer - Number of earned runs the pitcher gave up
  - **runs_allowed**: Integer - Number of runs allowed while the player was pitching
  - **hits_allowed**: Integer - Number of hits the pitcher gave up
  - **strikeouts**: Integer - Number of strikeouts the player got
  - **walks**: Integer - Number of walks the player gave up
  - **home_runs_allowed**: Integer - Number of home runs the player gave up
  - **hit_by_pitch**: Integer - Number of players the player hit with a pitch
  - **wild_pitches**: Integer - Number of wild pitches the player threw
  - **balks**: Integer - Number of balks the player had
  - **pickoffs**: Integer - Number of players the player picked off
  - **complete_game**: Boolean - True if it was a complete game, false otherwise
  - **shutout**: Boolean - True if it was a shutout, false otherwise
  - **save_opportunity**: Boolean - True if it was a save opportunity, false otherwise
  - **inherited_runners**: Integer - Number of runners the player inherited on the bases
  - **inherited_runners_scored**: Integer - Number of inherited runners that scored
  - **pitches_thrown**: Integer - Number of pitches the player threw
  - **strikes_thrown**: Integer - Number of strikes the player threw
  - **balls_thrown**: Integer - Number of balls the player threw
  - **save**: Boolean - True if it was a save, false otherwise
  - **doubles_allowed**: Integer - Number of doubles the pitcher gave up
  - **triples_allowed**: Integer - Number of triples the pitcher gave up
  - **air_outs**: Integer - Number of air outs the pitcher got
  - **line_outs**: Integer - Number of line outs the pitcher got
  - **fly_outs**: Integer - Number of fly outs the pitcher got
  - **pop_outs**: Integer - Number of pop outs the pitcher got
  - **ground_outs**: Integer - Number of ground outs the pitcher got
  - **win**: Boolean - True if it was a win, false otherwise
  - **loss**: Boolean - True if it was a loss, false otherwise
  - **hold**: Boolean - True if it was a hold, false otherwise
  - **game_started**: Boolean - True if the game was started by the pitcher, false otherwise
- **Keys and Indexes**:
  - **Primary Keys**
    - ***player_id***: Integer, Not null - Unique identifier of the player
    - ***game_id***: Integer, Not null - Unique identifier of the game
    - ***team_id***: Integer, Not null - Unique identifier of the team the player was on in this game
  - **Foreign Keys**
    - ***player_id***: References players(player_id)
    - ***game_id***: References games(game_id)
    - ***team_id***: References teams(team_id)

### Table: Players

- **Description**: Player information and attributes
- **Columns**:
  - **player_name**: Text, Not null - Name of the player
  - **player_birthday**: Date - Birthday of the player
  - **position_id**: Integer - Primary position of the player
  - **bat_side**: Char(1) - Side the player hits from
  - **throw_side**: Char(1) - Side the player throws from
  - **sz_top**: Numeric - Top of the players strike zone
  - **sz_bot**: Numeric - Bottom of the players strike zone
- **Keys and Indexes**:
  - **Primary Key**
    - ***player_id***: Integer, Not null - Unique identifier for each player
  - **Foreign Keys**
    - **position_id**: References positions(position_id)

### Table: Positions

- **Description**: Positions of players
- **Columns**:
  - **position_name**: Text, Not null - Name of the position
- **Keys and Indexes**:
  - **Primary Key**
    - ***position_id***: Integer, Not null - Indentifier of the position (as you would expect 1 = Pitcher, 2 = Catcher, etc.)

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
  - **runs_scored**: Integer - Number of runs scored
  - **hits**: Integer - Number of hits
  - **errors**: Integer - Number of errors committed by the defense
  - **left_on_base**: Integer - Number of runners left on base
- **Keys and Indexes**:
  - **Primary Keys**
    - ***inning_num***: Integer, Not null - Inning number
    - ***inning_half***: Char(1) - 'T' for top or 'B' for bottom
    - ***game_id***: Integer, Not null - Identifier for the game
  - **Foreign Keys**
    - ***game_id***: References games(game_id)
  - **Constraints**: inning_half must be either 'T' or 'B'

### Table: Pitches

- **Description**: Individual pitch information
- **Columns**:
  - **pitch_outcome_id**: Integer - Unique identifier for the outcome of the pitch
  - **pitch_data_id**: Integer - Unique identifier for statcast data from the pitch
  - **balls**: Integer - Number of balls before the pitch was thrown
  - **strikes**: Integer - Number of strikes before the pitch was thrown
  - **outs**: Integer - Number of outs after the play finished
- **Keys and Indexes**:
  - **Primary Keys**
    - ***game_id***: Integer, Not null - Identifier of the game
    - ***inning_num***: Integer, Not null - Inning number the at bat was in
    - ***inning_half***: Char(1) - 'T' for top or 'B' for bottom
    - ***at_bat_num***: Integer, Not null - At bat in the inning
    - ***pitch_num***: Integer, Not null - Number of the pitch during the at bat (1,2,3, etc)
  - **Foreign Keys**
    - ***(game_id, inning_num, inning_half, at_bat_num)***: References at_bats(game_id, inning_num, inning_half, at_bat_num)
    - ***pitch_outcome_id***: References pitch_outcomes(p_outcome_id)
    - ***pitch_data_id***: References pitch_data(pitch_data_id)
  - **Constraints**: inning_half must be either 'T' or 'B'

### Table: Pitch_Data

- **Description**: Statcast information for pitches
- **Columns**:
  - **initial_speed**: Numeric - Speed of pitch at release
  - **final_speed**: Numeric - Speed of pitch at arrival
  - **x0**: Numeric - X Location of pitch (Distance towards first base)
  - **y0**: Numeric - Y location of pitch (Distance away from home plate)
  - **z0**: Numeric - Z location of pitch (Height)
  - **vx0**: Numeric - Initial X velocity of the pitch
  - **vy0**: Numeric - Initial Y velocity of the pitch
  - **vz0**: Numeric - Initial Z velocity of the pitch
  - **ax**: Numeric - Initial X acceleration of the pitch
  - **ay**: Numeric - Initial Y acceleration of the pitch
  - **az**: Numeric - Initial Z acceleration of the pitch
  - **px**: Numeric - Horizontal position of the pitch when it crosses homeplate
  - **pz**: Numeric - Vertical position of the pitch when it crosses homeplate
  - **spin_rate**: Numeric - RPM of the pitch
  - **sz_top**: Numeric - Top of the strike zone(From the ground)
  - **sz_bot**: Numeric - Bottom of the strike zone(From the ground)
  - **pfxx**: Numeric - Horizontal movement of the pitch (no gravity)
  - **pfxz**: Numeric - Vertical movement of the pitch (no gravity)
  - **pfxzwithgravity**: Numeric - Vertical movement of the pitch
  - **pfxxwithgravity**: Numeric - Horizontal movement of the pitch
  - **extension**: Numeric - Extension of the pitcher on release
  - **breakz**: Numeric - Vertical movement of the pitch(inches)
  - **breakx**: Numeric - Horizontal Movement of the pitch(inches)
- **Keys and Indexes**
  - **Primary Key**
    - ***pitch_data_id***: Unique identifier for the pitch data
  - **Foreign Keys**
    - ***pitch_type_id***: References pitch_types(pitch_type_id)

### Table: At_Bats

- **Description**: Information about individual at-bats
- **Columns**:
  - **batter_id**: Integer - ID of the batter
  - **pitcher_id**: Integer - ID of the pitcher
  - **ab_outcome_id**: Integer - ID that references the ID of the outcome
  - **at_bat_des**: Text - Description of the at bat outcome
  - **bip_data_id**: Integer - ID that references data of a ball hit in play
- **Keys and Indexes**:
  - **Primary Keys**
    - ***game_id***: Integer, Not null - Identifier of the game
    - ***inning_num***: Integer, Not null - Inning number the at bat was in
    - ***inning_half***: Char(1) - 'T' for top or 'B' for bottom
    - ***at_bat_num***: Integer, Not null - At bat in the inning
  - **Foreign Keys**
    - ***(game_id, inning_num, inning_half)***: References innings(game_id, inning_num, inning_half)
    - ***bip_data_id***: References bip_data(bip_data_id)
    - ***ab_outcome_id***: References ab_outcomes(ab_outcome_id)
    - ***batter_id***: References players(player_id)
    - ***pitcher_id***: References players(player_id)
  - **Constraints**: inning_half must be either 'T' or 'B'

### Table: Bip_Data

- **Description**: Statcast data for Balls hit in play
- **Columns**:
  - **hit_angle**: Numeric - Angle the ball was hit at
  - **hit_speed**: Numeric - Speed the ball was hit at
  - **hc_y_ft**: Numeric - Y Coordinate in feet on the field of where the hit landed
  - **hc_x_fr**: Numeric - X coordinate in feet on the field of where the hit landed
  - **isBarrel**: Boolean - True/False value if the ball was hit on the barrel
  - **distance**: Numeric - Distance the hit traveled in feet
  - **bat_speed**: Numeric - Speed of the bat when it made contact
- **Keys and Indexes**:
  - **Primary Key**
    - ***bip_data_id***: Integer - Unique identifier for this ball in play
  - **Foreign Key**:
    - ***pitch_data_id***: References pitch_data(pitch_data_id)

### Table: Pitch_Type

- **Description**: Lookup table for pitch types
- **Columns**:
  - **pitch_type_name**: Text, NOT NULL - Name of the pitch type (fastball, slider, etc.)
- **Keys and Indexes**:
  - **Primary Key**
    - ***pitch_type_id***: Integer, Not null - Unique identifier of the pitch type

### Table: Pitch_Outcomes

- **Description**: Table that holds common pitch outcomes
- **columns**:
  - **p_description**: Text - description of the outcome
  - **p_result_code**: Text - code of the outcome
  - **p_call_name**: Text - name of the call
  - **p_call**: Text - The actual call
- **Keys and Indexes**
  - **Primary Key**
    - ***p_outcome_id***: Integer - Identifier for the outcome

### Table: Ab_Outcomes

- **Description**: Table that holds common at bat outcomes
- **columns**:
  - **ab_outcome_label**: Text - description of the outcome
- **Keys and Indexes**
  - **Primary Key**
    - ***ab_outcome_id***: Integer - Identifier for the outcome

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
