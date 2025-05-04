CREATE OR REPLACE FUNCTION find_postseason_walkoff_hr_leaders()
RETURNS TABLE(
    player_name TEXT,
    num_walkoff_hrs INT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        p.player_name,
        COUNT(*)::INT AS num_walkoff_hrs
    FROM
        games g
    JOIN at_bats ab ON g.game_id = ab.game_id
    JOIN players p ON ab.batter_id = p.player_id
    WHERE
        g.type_id NOT IN (1, 2)  -- Exclude spring training and regular season
        AND ab.inning_half = 'B'
        AND ab.ab_outcome_id = 25  -- Home run
        AND NOT EXISTS (
            SELECT 1
            FROM at_bats ab2
            WHERE ab2.game_id = g.game_id
              AND (
                    ab2.inning_num > ab.inning_num OR
                    (ab2.inning_num = ab.inning_num AND ab2.at_bat_num > ab.at_bat_num)
                  )
        )
    GROUP BY p.player_name
    ORDER BY num_walkoff_hrs DESC;
END;
$$ LANGUAGE plpgsql;
