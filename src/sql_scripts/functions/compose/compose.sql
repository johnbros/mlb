SELECT
    count(p.*) AS game_count
FROM pitches p
JOIN games g ON p.game_id = g.game_id
LEFT JOIN cold_start c
  ON p.game_id = c.game_id
 AND p.inning_num = c.inning_num
 AND p.inning_half_order = c.inning_half  -- key match
 AND p.at_bat_num = c.at_bat_num
 AND p.pitch_num = c.pitch_num
WHERE g.season_year BETWEEN 2015 AND 2024
  AND g.type_id != 1  -- regular season only
  AND g.level_id = 1  -- MLB level
  AND c.game_id IS NULL;
