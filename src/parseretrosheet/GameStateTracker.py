class GameStateTracker:
    def __init__(self, game_id):
        self.game_id = game_id
        self.current_inning = 1
        self.inning_half = 'T'
        self.at_bat_num = 0
        self.pitch_num = 0
        self.bases = {"1B": None, "2B": None, "3B": None}
        self.lineups = {"0": {}, "1": {}}  # team 0 = away, team 1 = home
        self.defense = {"0": {}, "1": {}}  # position_id -> player_id
        self.substitutions = []
        self.steals = []

    def handle_start(self, tokens):
        player_id = tokens[1]
        team_flag = tokens[3]  # '0' = away, '1' = home
        batting_order = int(tokens[4])
        position_id = int(tokens[5])
        self.lineups[team_flag][batting_order] = player_id
        self.defense[team_flag][position_id] = player_id

    def handle_sub(self, tokens):
        player_id = tokens[1]
        team_flag = tokens[3]
        batting_order = int(tokens[4])
        position_id = int(tokens[5])
        # update lineup and defense
        old_player_id = self.lineups[team_flag].get(batting_order, None)
        self.lineups[team_flag][batting_order] = player_id
        self.defense[team_flag][position_id] = player_id
        self.substitutions.append({
            "game_id": self.game_id,
            "team": team_flag,
            "inning": self.current_inning,
            "half": self.inning_half,
            "in": player_id,
            "out": old_player_id,
            "batting_order": batting_order,
            "position": position_id,
            "at_bat": self.at_bat_num,
            "pitch": self.pitch_num
        })

    def handle_play(self, tokens):
        self.current_inning = int(tokens[1])
        self.inning_half = tokens[2]
        # you'll need logic here to track at_bat and pitch_num
        play_details = tokens[6]
        if 'SB' in play_details or 'CS' in play_details:
            self.record_steal(tokens, play_details)

    def record_steal(self, tokens, detail):
        # very rough example
        for steal in ['SB2', 'SB3', 'SBH', 'CS2', 'CS3', 'CSH']:
            if steal in detail:
                success = 'CS' not in steal
                base = steal[-1] + "B" if 'H' not in steal else "Home"
                runner_id = tokens[3]
                batter_id = tokens[4]
                self.steals.append({
                    "game_id": self.game_id,
                    "inning": self.current_inning,
                    "half": self.inning_half,
                    "runner": runner_id,
                    "batter": batter_id,
                    "base": base,
                    "success": success,
                    "at_bat": self.at_bat_num,
                    "pitch": self.pitch_num
                })
