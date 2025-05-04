CREATE OR REPLACE FUNCTION resolve_game_id_by_utc(
    _game_date DATE,
    _utc_time TIME,
    _home_abbr TEXT,
    _away_abbr TEXT,
    _tolerance_minutes INTEGER DEFAULT 60
)
RETURNS INTEGER AS $$
DECLARE
    result_id INTEGER;
BEGIN
    SELECT g.game_id
    INTO result_id
    FROM games g
    JOIN team_abbreviation_map hmap ON g.h_team_id = hmap.team_id
    JOIN team_abbreviation_map amap ON g.a_team_id = amap.team_id
    WHERE hmap.external_abbr = _home_abbr
      AND amap.external_abbr = _away_abbr
      AND g.date = _game_date
      AND ABS(EXTRACT(EPOCH FROM (g.game_time - _utc_time))) <= (_tolerance_minutes * 60)
    ORDER BY ABS(EXTRACT(EPOCH FROM (g.game_time - _utc_time))) ASC
    LIMIT 1;

    RETURN result_id;
END;
$$ LANGUAGE plpgsql;
