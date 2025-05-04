import uuid
import re

_PARENS = re.compile(r'\(([^)]*)\)')
SEG_RE = re.compile(r'(\d+)(?:\((\d+|H)\))?')

def split_advancement(adv: str):
    # Find all parenthetical params
    params = _PARENS.findall(adv)      # e.g. ["E4/TH", "UR", "NR"]

    # Remove the params from the string, leaving the core (e.g. "2-H")
    core = _PARENS.sub("", adv)        # strips out all "(...)" pieces

    return core, params

_TOKENIZER = re.compile(r'\d+|[A-Z]+\d+|\([^)]+\)')

def tokenize_play(play: str) -> list[str]:
    """
    Split a string like "16(1)3" into ['16', '(1)', '3'].
    """
    return _TOKENIZER.findall(play)

def sort_by_from_base(advs):
    def from_base(adv):
        # grab the leading number (one or more digits)
        m = re.match(r'(\d+)', adv)
        return int(m.group(1)) if m else -1
    return sorted(advs, key=from_base, reverse=True)

class GameStateTracker:
    def __init__(self, game_id, player_dict):
        self.game_id = game_id
        self.current_inning = 1
        self.inning_half = 'T'
        self.at_bat_num = 0
        self.pitch_num = 0
    
        self.batter_id = None
        self.last_no_play = None
        self.player_dict = player_dict
        self.player_dict_retro = {}
        self.last_play = (1,'T',1,1)
        self.bases = {'0': None, '1': None, '2': None, '3': None}
        self.lineups = {"0": {}, "1": {}}  # team 0 = away, team 1 = home lineup_spot -> player_id
        self.current_lineup = {'T': None, 'B': None}
        self.defense = {"0": {}, "1": {}}  # position_id -> player identifier
        self.current_defense = {'B': None, 'T': None} # The away team plays defense in the bottom of the inning
        self.last_home_sub = (1,'T',1,0) # Inning, Half, AtBatNum, PitchNum
        self.last_away_sub = (1,'T',1,0) # Inning, Half, AtBatNum, PitchNum
        self.steals = []
        

        self.home_score = 0
        self.away_score = 0
        self.cur_outs = 0

        # Stuff that goes into the database
        self.game_comments = [] # list of tuples (game_id, inning, half, at_bat_num, pitch_num, comment)
        self.adjustments = [] # list of tuples (game_id, inning, half, at_bat_num, pitch_num, player_id, adjustment, type)
        self.base_states = [(self.game_id, 1, 'T', 1, 1, None, None, None)] # list of tuples (game_id, inning, half, at_bat_num, pitch_num, base_state)
        self.update_pitch_metadata = [] # Update outs and score at each pitch
        self.fielding_sequences = [] # Fielding events that occur (throw_id, self.game_id, inn, half, ab, pn, out_at, error_on, fielder_id, target_id, next_throw_id)
        self.base_running_events = [] # Base running events that occur
        self.game_subs = [] # Substitutions that occur

    def handle_start(self, tokens):
        player_name = tokens[2].replace('"', '').strip()
        player_id = self.player_dict[player_name]
        self.player_dict_retro[tokens[1]] = player_id
        team_flag = tokens[3]
        batting_order = int(tokens[4])
        position_id = int(tokens[5])
        start_time = (1, 'T', 1, 0)

        if start_time not in self.lineups[team_flag]:
            self.lineups[team_flag][start_time] = {}
        if start_time not in self.defense[team_flag]:
            self.defense[team_flag][start_time] = {}

        self.lineups[team_flag][start_time][batting_order] = player_id
        self.defense[team_flag][start_time][position_id] = player_id

        if team_flag == '1':
            self.last_home_sub = start_time
            self.current_lineup['B'] = self.lineups[team_flag][start_time]
            self.current_defense['T'] = self.lineups[team_flag][start_time]
        else:
            self.last_away_sub = start_time
            self.current_lineup['T'] = self.lineups[team_flag][start_time]
            self.current_defense['B'] = self.lineups[team_flag][start_time]

    def handle_sub(self, tokens):
        player_name = tokens[2].replace('"', '').strip()
        player_id = self.player_dict[player_name]
        self.player_dict_retro[tokens[1]] = player_id
        team_flag = tokens[3]
        batting_order = int(tokens[4])
        position_id = int(tokens[5])
        game_time = self.last_no_play
        if team_flag == '1':
            self.lineups[team_flag][game_time] = self.lineups[team_flag][self.last_home_sub].copy()
            self.defense[team_flag][game_time] = self.defense[team_flag][self.last_home_sub].copy()
            self.last_home_sub = game_time
        else:
            self.lineups[team_flag][game_time] = self.lineups[team_flag][self.last_away_sub].copy()
            self.defense[team_flag][game_time] = self.defense[team_flag][self.last_away_sub].copy()
            self.last_away_sub = game_time
        
        old_player = self.lineups[team_flag][game_time][batting_order]
        if position_id == 12:
            for base in self.bases:
                if self.bases[base] == old_player:
                    self.bases[base] = player_id
        self.lineups[team_flag][game_time][batting_order] = player_id
        
        self.defense[team_flag][game_time][position_id] = player_id
        game_sub_entry = (self.game_id,) + game_time + (player_id, old_player, batting_order, position_id)
        self.game_subs.append(game_sub_entry)

        if team_flag == '1':
            self.current_lineup['B'] = self.lineups[team_flag][game_time].copy()
            self.current_defense['T'] = self.lineups[team_flag][game_time].copy()
        else:
            self.current_lineup['T'] = self.lineups[team_flag][game_time].copy()
            self.current_defense['B'] = self.lineups[team_flag][game_time].copy()



    def handle_com(self, tokens):
        com = tokens[1].replace('"', '').strip()
        com_entry = (self.game_id,)+ self.last_play + (com,)
        self.game_comments.append(com_entry)

    def handle_radj(self, tokens):
        runner_id = self.player_dict_retro[tokens[1]]
        self.bases = {'0': None, '1': None, '2': runner_id, '3': None}
        inn = self.current_inning
        ab = 1
        pn = 1
        half = self.inning_half
        if self.cur_outs == 3:
            self.cur_outs = 0
            if self.inning_half == 'T':
                half = 'B'
            else:
                inn = inn + 1
                half = 'T'
            self.inning_half = half
            self.at_bat_num = 0
        start_inn_state = (self.game_id, inn, half, ab, pn, None, runner_id, None)
        self.base_states.append(start_inn_state)
        update_pitch_data = (self.home_score, self.away_score, self.cur_outs, self.game_id, inn, half, ab, pn)
        self.update_pitch_metadata.append(update_pitch_data)
        


    def handle_padj(self, tokens):
        pitcher_id = self.player_dict_retro[tokens[1]]
        adjustment = tokens[2]
        adjustment_entry = (self.game_id,)+ self.last_play + (pitcher_id, adjustment, 'P')
        self.adjustments.append(adjustment_entry)
        

    def handle_badj(self, tokens):
        batter_id = self.player_dict_retro[tokens[1]]
        adjustment = tokens[2]
        adjustment_entry = (self.game_id,)+ self.last_play + (batter_id, adjustment, 'B')
        self.adjustments.append(adjustment_entry)

    def get_pitch_num(self, pitch_sequence):
        non_pitch_events = {'N', '+', '.', '*', '1', '2', '3', '>', 'V', 'H', 'A'}
        return sum(1 for char in pitch_sequence if char not in non_pitch_events)

    def split_play(self, play_details):
        if '.' in play_details:
            left, right = play_details.split('.', 1)
            advancements = right.split(';')
        else:
            left = play_details
            advancements = []
        return left, advancements
    
    

    def parse_play(self, play_details):
        left, advancements = self.split_play(play_details)
        play_chunks = left.split('/')
        basic_play = play_chunks[0]
        if '(' in basic_play and ')' not in basic_play:
            basic_play = basic_play + '/' + play_chunks[1]
            modifiers = play_chunks[2:]
        else:
            modifiers = play_chunks[1:]
            
        return basic_play, modifiers, advancements
    
    def emit_throw_and_out(self, sequence, out_base, throw_id):
        # Replace 'H' with '2' before processing
        sequence = sequence.replace('H', '2')
        
        time = self.last_play
        inn, half, ab, pn = time
        out_at = None
        
        for idx in range(len(sequence)-1):
            fielder_id = self.current_defense[self.inning_half][int(sequence[idx])]
            target_id = self.current_defense[self.inning_half][int(sequence[idx+1])]
            next_throw_id = str(uuid.uuid4())
            entry = (throw_id, self.game_id, inn, half, ab, pn,  fielder_id, target_id, out_at, None, next_throw_id)
            self.fielding_sequences.append(entry)
            throw_id = next_throw_id
        
        next_throw_id = str(uuid.uuid4())
        out_at = out_base
        fielder_id = self.current_defense[self.inning_half][int(sequence[-1])]
        target_id = None
        error_on = None
        entry = (throw_id, self.game_id, inn, half, ab, pn, fielder_id, target_id, out_at, error_on, next_throw_id)
        self.fielding_sequences.append(entry)
        return next_throw_id
    

    def emit_throw_with_error(self, pre_error, post_error, throw_id):
        # Replace 'H' with '2' before processing
        if pre_error:
            pre_error = pre_error.replace('H', '2')
        if post_error:
            post_error = post_error.replace('H', '2')
        
        time = self.last_play
        inn, half, ab, pn = time
        out_at = None
        
        if pre_error:
            for idx in range(len(pre_error)-1):
                fielder_id = self.current_defense[self.inning_half][int(pre_error[idx])]
                target_id = self.current_defense[self.inning_half][int(pre_error[idx+1])]
                next_throw_id = str(uuid.uuid4())
                entry = (throw_id, self.game_id, inn, half, ab, pn, fielder_id, target_id, out_at, None, next_throw_id)
                self.fielding_sequences.append(entry)
                throw_id = next_throw_id
            
            next_throw_id = str(uuid.uuid4())
            fielder_id = self.current_defense[self.inning_half][int(pre_error[-1])]
            target_id = self.current_defense[self.inning_half][int(post_error)]
            entry = (throw_id, self.game_id, inn, half, ab, pn, fielder_id, target_id, out_at, None, next_throw_id)
            self.fielding_sequences.append(entry)
            throw_id = next_throw_id
        
        next_throw_id = str(uuid.uuid4())
        fielder_id = self.current_defense[self.inning_half][int(post_error)]
        target_id = None
        error_on = fielder_id
        entry = (throw_id, self.game_id, inn, half, ab, pn, fielder_id, target_id, out_at, error_on, next_throw_id)
        self.fielding_sequences.append(entry)
        return next_throw_id
    
    def emit_throw(self, sequence, throw_id):
        # Replace 'H' with '2' before processing
        sequence = sequence.replace('H', '2')
        
        time = self.last_play
        inn, half, ab, pn = time
        out_at = None
        
        for idx in range(len(sequence)-1):
            fielder_id = self.current_defense[self.inning_half][int(sequence[idx])]
            target_id = self.current_defense[self.inning_half][int(sequence[idx+1])]
            next_throw_id = str(uuid.uuid4())
            entry = (throw_id, self.game_id, inn, half, ab, pn, fielder_id, target_id, out_at, None, next_throw_id)
            self.fielding_sequences.append(entry)
            throw_id = next_throw_id
        
        next_throw_id = str(uuid.uuid4())
        fielder_id = self.current_defense[self.inning_half][int(sequence[-1])]
        target_id = None
        error_on = None
        entry = (throw_id, self.game_id, inn, half, ab, pn, fielder_id, target_id, out_at, error_on, next_throw_id)
        self.fielding_sequences.append(entry)
        return next_throw_id
    
    def handle_advancements(self, advancements, stealing=False):
        banned = {'UR', 'NR', 'TH', 'WP', 'PB', 'RBI', 'TUR', 'THH', 'TH1', 'TH2', 'TH3', 'TH4', 'TH5', 'TH6', 'TH7', 'TH8', 'TH9', 'THC'}
        batter_at = '0'
        time = self.last_play
        inn, half, ab, pn = time
        cur_throw_id = str(uuid.uuid4())
        for advancement in advancements:
            scored = False
            was_out = False
            core, params = split_advancement(advancement)
            params = [param for param in params if param not in banned]
            if '-' in core:
                from_base, to_base = core.split('-')
                if to_base == 'H':
                    to_base = '4'
                if from_base == 'B':
                    from_base = batter_at
                if from_base == '0':
                    batter_at = to_base
                runner_id = self.bases[from_base]
                if runner_id is None:
                    raise ValueError(f"Runner ID is None for advancement: {advancement}")
                if to_base == '4':
                    self.bases[from_base] = None
                    if self.inning_half == 'T':
                        self.away_score += 1
                    else:
                        self.home_score += 1
                    scored = True
                    
                else:
                    self.bases[from_base] = None
                    self.bases[to_base] = runner_id
                    

                base_running_entry = (self.game_id, inn, half, ab, pn, runner_id, int(from_base), int(to_base), was_out, scored, stealing)
                self.base_running_events.append(base_running_entry)

                for param in params:
                    if param in banned:
                        continue  # ✅ Skip these completely
                    if 'E' in param:
                        # handle throw with error
                        if '/' in param:
                            sequence, mod = param.split('/',1)
                        else:
                            sequence = param
                        pre_error, post_error = sequence.split('E')
                        cur_throw_id = self.emit_throw_with_error(pre_error, post_error, cur_throw_id)
                    else:
                        # normal throw
                        if '/' in param:
                            sequence, mod = param.split('/',1)
                        else:
                            sequence = param
                        cur_throw_id = self.emit_throw(sequence, cur_throw_id)
                    
                        

                
            else:
                from_base, to_base = core.split('X')
                
                was_out = True
                for param in params:
                    if 'E' in param:
                        was_out = False
                        if '/' in param:
                            sequence, mod = param.split('/',1)
                        else:
                            sequence = param
                        pre_error, post_error = sequence.split('E')
                        out_at = None
                        if to_base == 'H':
                            to_base = '4'
                        if from_base == 'B':
                            from_base = batter_at
                        if from_base == '0':
                            batter_at = to_base
                        runner_id = self.bases[from_base]
                        if runner_id is None:
                            raise ValueError(f"Runner ID is None for advancement: {advancement}")
                        if to_base == '4':
                            self.bases[from_base] = None
                            if self.inning_half == 'T':
                                self.away_score += 1
                            else:
                                self.home_score += 1
                            scored = True
                            
                        else:
                            self.bases[from_base] = None
                            self.bases[to_base] = runner_id
                            
                        
                        base_running_entry = (self.game_id, inn, half, ab, pn, runner_id, int(from_base), int(to_base), was_out, scored, stealing)
                        self.base_running_events.append(base_running_entry)

                        cur_throw_id = self.emit_throw_with_error(pre_error, post_error, cur_throw_id)

                        

                if was_out:
                    if from_base == 'B':
                        from_base = batter_at
                    if to_base == 'H':
                        to_base = '4'
                    runner_id = self.bases[from_base]
                    if runner_id is None:
                        raise ValueError(f"Runner ID is None for advancement: {advancement}")
                    self.bases[from_base] = None
                    self.cur_outs += 1
                    base_running_entry = (self.game_id, inn, half, ab, pn, runner_id, int(from_base), int(to_base), was_out, scored, stealing)
                    self.base_running_events.append(base_running_entry)
                    
                    for param in params:
                        if param in banned:
                            continue  # ✅ Skip these completely
                        if '/' in param:
                            sequence, mod = param.split('/',1)
                        else:
                            sequence = param
                        cur_throw_id = self.emit_throw_and_out(sequence, to_base, cur_throw_id)
                        



    

    def insert_batter_advancement(self, advancements, batter_adv):
        if batter_adv:
    # find the first BX‐out (batter‐runner out at a base) in the list
            bx_idx = next(
                (i for i, a in enumerate(advancements)
                if a.upper().startswith("BX") or a.upper().startswith("B-")),
                None
            )
            if bx_idx is not None:
                # insert your batter advance just before the BX out
                advancements.insert(bx_idx, batter_adv)
            else:
                # no BX out, just append at the end
                advancements.append(batter_adv)
        
        return advancements
    
    
    def _handle_steal_like(self, core, params, advancements, idx):
        # core = "CS2" or "POCS3" etc.
        # extract from/to just the way you already do
        
        to_base = core[idx]
        if to_base == 'H':
            to_base = '4'
        from_base = str(int(to_base) - 1)
        
    
        
        steal_adv  = f"{from_base}X{to_base}({params[0]})"
        extra, failed, rest = None, None, advancements[:]   # copy

        for adv in advancements:
            if adv.startswith(f"{from_base}-"):
                extra = adv;     rest.remove(adv)
            elif adv.startswith(f"{from_base}X"):
                failed = adv;    rest.remove(adv)

        new_advs = [steal_adv]
        if to_base not in {'4'}:
            if extra:
                core_e, params_e = split_advancement(extra)
                _, to2 = core_e.split('-', 1)
                if params_e:
                    raw = params_e[0].split('/', 1)[0]
                    new_advs.append(f"{to_base}-{to2}({raw})")
                else:
                    new_advs.append(f"{to_base}-{to2}")

            # handle the “failed” steal advance (e.g. X to third on caught stealing)
            if failed:
                core_f, params_f = split_advancement(failed)
                _, to3 = core_f.split('X', 1)
                if params_f:
                    raw = params_f[0].split('/', 1)[0]
                    new_advs.append(f"{to_base}X{to3}({raw})")
                else:
                    new_advs.append(f"{to_base}X{to3}")
        else:
            if extra:

                core_e, params_e = split_advancement(extra)
                core_1, params_1 = split_advancement(steal_adv)
                from1, to1 = core_1.split('X', 1)
                from2, to2 = core_e.split('-', 1)
                if from1 == from2:
                    new_advs.remove(steal_adv)
                    new_advs.append(f"{from2}-{to2}({params_1[0]})")


                
        rest = sort_by_from_base(rest)
        from_base_val = int(from_base)
        
        # Find the insertion point - we want higher base numbers first
        insert_idx = len(rest)  # Default to end if no smaller base number found
        for i, adv in enumerate(rest):
            m = re.match(r'(\d+)', adv)
            if m and int(m.group(1)) < from_base_val:
                insert_idx = i
                break
        
        # Insert in the correct position
        result = rest[:insert_idx] + new_advs + rest[insert_idx:]
        return result
                
# Fielding Sequence Columns, (fielding_sequence_id, game_id, inning_num, inning_half, at_bat_num, pitch_num, out_at, error_on, fielder_id, target_id, next_fielding_sequence_id)
        
    def process_play(self, basic_play, modifiers, advancements):
        unnasisted_outs = {'1','2','3','4','5','6','7','8','9'}
        banned = {'UR', 'NR', 'TH', 'WP', 'PB', 'RBI', 'TUR', 'THH', 'TH1', 'TH2', 'TH3', 'TH4', 'TH5', 'TH6', 'TH7', 'TH8', 'TH9', 'THC'}
        time = self.last_play
        inn, half, ab, pn = time
       

        if basic_play in unnasisted_outs:
            batter_adv = f'0X1({basic_play})'
            advancements_copy = list(advancements)
            
            for advancement in advancements_copy:
                a, t = split_advancement(advancement)
                t = [x for x in t if x not in banned]
                if len(t) > 1:
                    for param in t:
                        if 'E' in param:
                            error = param
                            t.remove(param)
                            a = a + ''.join(f'({p})' for p in t)
                            if advancement in advancements:  # Check if still in list
                                advancements.remove(advancement)
                            advancements.append(a)
                            break
            advancements = self.insert_batter_advancement(advancements, batter_adv)
            advancements = sort_by_from_base(advancements)
            self.handle_advancements(advancements)
        elif basic_play.startswith('SB'):
            # parse from/to
            play2 = None
            if ';' in basic_play:
                play1, play2 = basic_play.split(';', 1)
                basic_play = play1

            raw_to = basic_play[2]
            to_base = '4' if raw_to == 'H' else raw_to
            from_base = str(int(to_base) - 1)
            initial_thrower = '2'
            
            # initialize
            steal_adv = f"{from_base}-{to_base}"
            new_advs  = [steal_adv]
            rest      = []
            extra = failed = None
            # split out extra/failure from the rest
            for adv in advancements:
                if adv.startswith(f"{from_base}-"):
                    extra = adv
                elif adv.startswith(f"{from_base}X"):
                    failed = adv
                else:
                    rest.append(adv)
            def annotate_param(core, params):
                # core: e.g. "2E4" or "46"; params: list of "(…)" contents
                if params:
                    if params[0] in banned:
                        params = None
                tag = params[0].split('/',1)[0] if params else ''
                tag = tag if tag.startswith(initial_thrower) else initial_thrower + tag
                return tag

            # handle the “advance on error” case (e.g. runner makes it to 3rd)
            if extra:
                core_e, params_e = split_advancement(extra)
                _, to2 = core_e.split('-',1)
                if to2 == 'H':
                    to2 = '4'
                if to_base == to2:
                    pass
                else:
                    tag = annotate_param(core_e, params_e)
                    new_advs.append(f"{to_base}-{to2}({tag})")

            # handle the “caught out” case
            if failed:
                core_f, params_f = split_advancement(failed)
                _, to3 = core_f.split('X',1)
                if to3 == 'H':
                    to3 = '4'
                if to_base == to3:
                    if len(params_f) == 1:
                        new_advs.remove(steal_adv)
                        new_advs.append(f"{from_base}X{to3}({params_f[0]})")
                    pass
                else:
                    tag = annotate_param(core_f, params_f)
                    new_advs.append(f"{to_base}X{to3}({tag})")

            # rebuild and dispatch
            base_start = int(from_base)
            bases_before_start = [str(b) for b in range(base_start)]
            bases_before_start.append('B')
            inserted = False
            for idx in range(len(rest)):
                
                if any(rest[idx].startswith(b) for b in bases_before_start):
                    advancements = rest[:idx] + new_advs + rest[idx:]
                    inserted = True
                    break

            if not inserted:
                advancements = rest + new_advs
            if play2:
                self.process_play(play2, modifiers, advancements)
            else:
                self.handle_advancements(advancements, stealing=True)

        elif basic_play in {'DI', 'BK', 'OA', 'PB', 'WP'}:
            advancements = sort_by_from_base(advancements)
            self.handle_advancements(advancements)
        elif basic_play.startswith('K'):
            if '+' in basic_play:
                batter_on = False
                strikeout, new_play = basic_play.split('+', 1)
                if strikeout == 'K':
                    throw_sequence = '12'
                else:
                    throw_sequence = strikeout[1:]
                for advancement in advancements:
                    if advancement.startswith('B'):
                        batter_on = True
                if not batter_on:
                    batter_adv = '0X1(12)'
                    advancements = self.insert_batter_advancement(advancements, batter_adv)
                self.process_play(new_play, modifiers, advancements)
            else:
                batter_on = False
                for adv in advancements:
                    if adv.startswith('B'):
                        batter_on = True
                if not batter_on:
                    batter_adv = '0X1(12)'
                    advancements = self.insert_batter_advancement(advancements, batter_adv)
                advancements = sort_by_from_base(advancements)
                self.handle_advancements(advancements)
        elif basic_play in {'C'}:
            batter_adv = f'0-1({modifiers[0]})'
            advancements = self.insert_batter_advancement(advancements, batter_adv)
            advancements = sort_by_from_base(advancements)
            self.handle_advancements(advancements)
        elif basic_play in {"W","I","IW","HP"}:
            batter_adv = '0-1(12)'
            advancements = self.insert_batter_advancement(advancements, batter_adv)
            advancements = sort_by_from_base(advancements)
            self.handle_advancements(advancements)
        elif basic_play.startswith('W'):
            if '+' in basic_play:
                batter_adv = '0-1(12)'
                advancements = self.insert_batter_advancement(advancements, batter_adv)
                advancements = sort_by_from_base(advancements)
                self.process_play(basic_play[2:], modifiers, advancements)
            else:
                batter_adv = '0-1(12)'
                advancements = self.insert_batter_advancement(advancements, batter_adv)
                advancements = sort_by_from_base(advancements)
                self.handle_advancements(advancements)
        elif basic_play.startswith('IW'):
            if '+' in basic_play:
                batter_adv = '0-1(12)'
                advancements = self.insert_batter_advancement(advancements, batter_adv)
                advancements = sort_by_from_base(advancements)
                self.process_play(basic_play[3:], modifiers, advancements)
            else:
                batter_adv = '0-1(12)'
                advancements = self.insert_batter_advancement(advancements, batter_adv)
                advancements = sort_by_from_base(advancements)
                self.handle_advancements(advancements)
        elif basic_play.startswith('SEGURA'):
            self.handle_advancements(advancements)
        elif basic_play.startswith('FC'):
            b_adv = None
            batter_adv = None
            error = None
            
            # Make a copy of advancements for safe iteration
            advancements_copy = list(advancements)
            
            for advancement in advancements_copy:
                a, t = split_advancement(advancement)
                t = [x for x in t if x not in banned]
                if len(t) > 1:
                    for param in t:
                        if 'E' in param:
                            error = param
                            t.remove(param)
                            a = a + ''.join(f'({p})' for p in t)
                            if advancement in advancements:  # Check if still in list
                                advancements.remove(advancement)
                            advancements.append(a)
                            break

                # Only try to process B advancement if it's still in the list
                if advancement.startswith('B') and advancement in advancements:
                    b_adv = advancement
                    advancements.remove(advancement)
                    break

            if b_adv is not None:  
                if 'X' in b_adv:
                    core, params = split_advancement(b_adv)
                    from_base, to_base = core.split('X', 1)
                    if to_base == 'H':
                        to_base = '4'
                    if from_base == 'B':
                        from_base = '0'
                    if params:
                        batter_adv = f'{from_base}X{to_base}({params[0]})'
                    else:
                        batter_adv = f'{from_base}X{to_base}'
                elif '-' in b_adv:
                    core, params = split_advancement(b_adv)
                    from_base, to_base = core.split('-', 1)
                    if error:
                        params.append(error)
                    if to_base == 'H':
                        to_base = '4'
                    if from_base == 'B':
                        from_base = '0'
                    if params:
                        batter_adv = f'{from_base}-{to_base}({params[0]})'
                    else:
                        batter_adv = f'{from_base}-{to_base}'
                else:
                    batter_adv = '0-1'
            else:
                batter_adv = '0-1'  

            advancements = self.insert_batter_advancement(advancements, batter_adv)
            advancements = sort_by_from_base(advancements)
            self.handle_advancements(advancements)
        elif basic_play.startswith('S'):
            if len(basic_play) > 1:
                throws = basic_play[1]
                batter_adv = f'0-1({throws})'
            else:
                batter_adv = '0-1'
            error = None
            
            # Make a copy for safe iteration
            advancements_copy = list(advancements)
            for adv in advancements_copy:
                a, t = split_advancement(adv)
                t = [x for x in t if x not in banned]
                if len(t) > 1:
                    for param in t:
                        if 'E' in param:
                            error = param
                            t.remove(param)
                            a = a + ''.join(f'({p})' for p in t)
                            if adv in advancements:  # Safety check
                                advancements.remove(adv)
                            advancements.append(a)
                            break
            if error:
                batter_adv += f'({error})'
            advancements = self.insert_batter_advancement(advancements, batter_adv)
            advancements = sort_by_from_base(advancements)
            self.handle_advancements(advancements)
        elif basic_play.startswith('DGR'):
            batter_adv = f'0-2'
            advancements = self.insert_batter_advancement(advancements, batter_adv)
            advancements = sort_by_from_base(advancements)
            self.handle_advancements(advancements)
        elif basic_play.startswith('D'):
            batter_movement = False
            for adv in advancements:
                if 'B' in adv:
                    a, t = split_advancement(adv)
                    advancements.remove(adv)
                    batter_movement = True
                    break
            if batter_movement:
                non_banned = [p for p in t if p not in banned]
                count_throws = len(non_banned)
                if count_throws > 1:
                    idx = next((i for i, x in enumerate(non_banned) if 'E' in x), -1)
                    if idx != -1:
                        err_throw = non_banned[idx]
                        other_throws = [p for i, p in enumerate(non_banned) if i != idx]
                        err_param = f'({err_throw})'
                        other_params = ''.join(f'({p})' for p in other_throws)
                        batter_adv = f'0-2{err_param}'
                
                        # Re-add batter out attempt
                        a = a + other_params
                        advancements.append(a)
                    else:
                        # Fallback: treat all throws as errorless
                        params = ''.join(f'({p})' for p in non_banned)
                        batter_adv = '0-2'
                        a = a + params
                        advancements.append(a)
                else:
                    # Only one throw: treat as error or not, move all to 0-2
                    params = ''.join(f'({p})' for p in non_banned)
                    batter_adv = '0-2'
                    a = a + params
                    advancements.append(a)

            elif len(basic_play) > 1:
                throws = basic_play[1]
                batter_adv = f'0-2({throws})'
            else:
                batter_adv = f'0-2'
            
            advancements = self.insert_batter_advancement(advancements, batter_adv)
            advancements = sort_by_from_base(advancements)
            self.handle_advancements(advancements)
        elif basic_play.startswith('T'):
            if len(basic_play) > 1:
                throws = basic_play[1]
                batter_adv = f'0-3({throws})'
            else:
                batter_adv = f'0-3'
            advancements = self.insert_batter_advancement(advancements, batter_adv)
            advancements = sort_by_from_base(advancements)
            self.handle_advancements(advancements)
        elif basic_play in {'HR', 'H'}:
            base_runner_entry = False
            for adv in advancements:
                if 'B' in adv:
                    base_runner_entry = True
                    break
            if not base_runner_entry:
                batter_adv = '0-4'
                advancements = self.insert_batter_advancement(advancements, batter_adv)
            advancements = sort_by_from_base(advancements)
            self.handle_advancements(advancements)              
        elif basic_play.startswith('HR'):
            base_runner_entry = False
            for adv in advancements:
                if 'B' in adv:
                    base_runner_entry = True
                    break
            if not base_runner_entry:
                batter_adv = f'0-4({basic_play[2:]})'
                advancements = self.insert_batter_advancement(advancements, batter_adv)
            advancements = sort_by_from_base(advancements)
            self.handle_advancements(advancements)
        elif basic_play.startswith('H'):
            base_runner_entry = False
            for adv in advancements:
                if 'B' in adv:
                    base_runner_entry = True
                    break
            if not base_runner_entry:
                batter_adv = f'0-4({basic_play[1:]})'
                advancements = self.insert_batter_advancement(advancements, batter_adv)
            advancements = sort_by_from_base(advancements)
            self.handle_advancements(advancements)
        elif basic_play.startswith('E'):
            if 'B-1' in advancements:
                advancements.remove('B-1')
                    

            batter_adv = f'0-1({basic_play})'

            advancements = self.insert_batter_advancement(advancements, batter_adv)
            advancements = sort_by_from_base(advancements)
            self.handle_advancements(advancements)
        elif basic_play.startswith(('CS','POCS')):
            core, params = split_advancement(basic_play)
            if basic_play.startswith('POCS'):
                idx = 4
            else:
                idx = 2
            new_advs = self._handle_steal_like(core, params, advancements, idx)
            self.handle_advancements(new_advs, stealing=True)
        elif basic_play.startswith('PO'): 
            core, params = split_advancement(basic_play)
            from_base = core[2]
            to_base = from_base
            steal_adv  = f"{from_base}X{to_base}({params[0]})"
            extra, failed, rest = None, None, advancements[:]   # copy

            for adv in advancements:
                if adv.startswith(f"{from_base}-"):
                    extra = adv;     rest.remove(adv)
                elif adv.startswith(f"{from_base}X"):
                    failed = adv;    rest.remove(adv)

            new_advs = [steal_adv]
            if extra:
                core_e, params_e = split_advancement(extra)
                _, to2 = core_e.split('-', 1)
                if params_e:
                    raw = params_e[0].split('/', 1)[0]
                    new_advs.append(f"{to_base}-{to2}({raw})")
                else:
                    new_advs.append(f"{to_base}-{to2}")

            # handle the “failed” steal advance (e.g. X to third on caught stealing)
            if failed:
                core_f, params_f = split_advancement(failed)
                _, to3 = core_f.split('X', 1)
                if params_f:
                    raw = params_f[0].split('/', 1)[0]
                    new_advs.append(f"{to_base}X{to3}({raw})")
                else:
                    new_advs.append(f"{to_base}X{to3}")

            advancement =  new_advs + rest
            advancement = sort_by_from_base(advancement)
            self.handle_advancements(advancement, stealing=False)
        elif basic_play.startswith('FLE'):
            pass
        else:
            # pure-digit plays → batter out:
            tokens = tokenize_play(basic_play)
           # print(f"Tokens: {tokens}")
            batter_out = False
            throw_sequence = ''
            last_token = False
            for token in tokens:
                if token.startswith('('):
                    from_base = token[1]
                    if from_base == 'B':
                        from_base = '0'
                        batter_out = True
                    to_base = str(int(from_base) + 1)
                    adv = f'{from_base}X{to_base}({throw_sequence})'
                    advancements.append(adv)
                    last_token = True
                else: 
                    if not last_token:
                        throw_sequence += token
                    else:
                        throw_sequence = token
                    last_token = False
            
            advancements_copy = list(advancements)
            error = None
            for adv in advancements_copy:
                a, t = split_advancement(adv)
                t = [x for x in t if x not in banned]
                if len(t) > 1:
                    for param in t:
                        if 'E' in param:
                            error = param
                            t.remove(param)
                            a = a + ''.join(f'({p})' for p in t)
                            if adv in advancements:  # Safety check
                                advancements.remove(adv)
                            advancements.append(a)
                            break
            
            if last_token and not batter_out:
                if error:
                    batter_adv = f'0-1({error})'
                else:
                    batter_adv = f'0-1'
                advancements.append(batter_adv)
            elif not last_token and not batter_out:
                batter_adv = f'0X1({throw_sequence})'
                advancements.append(batter_adv)
            
            
            advancements = sort_by_from_base(advancements)
            self.handle_advancements(advancements)

        # print(f'Advancements: {advancements}')
        # print(f'Current Bases: {self.bases}')



                    








    def handle_play(self, tokens):

        if self.cur_outs == 3:
            self.bases = {'0': None, '1': None, '2': None, '3': None}
            self.cur_outs = 0
            inn = self.current_inning
            if self.inning_half == 'T':
                half = 'B'
            else:
                inn = inn + 1
                half = 'T'
            ab = 1
            pn = 1
            start_inn_state = (self.game_id, inn, half, ab, pn, None, None, None)
            self.base_states.append(start_inn_state)
            update_pitch_data = (self.home_score, self.away_score, self.cur_outs, self.game_id, inn, half, ab, pn)
            self.update_pitch_metadata.append(update_pitch_data)
            self.inning_half = half
            self.at_bat_num = 0
        self.pitch_num = self.get_pitch_num(tokens[5])
        self.current_inning = int(tokens[1])
        # cur_half = int(tokens[2])
        # if cur_half:
        #     cur_half = 'B'
        # else:
        #     cur_half = 'T'
        # #If we are in a new inning half reset at bat number to 0 and reset the bases
        # if self.inning_half != cur_half:
        #     self.inning_half = cur_half
        #     self.at_bat_num = 0
        
        if tokens[6] == 'NP':
            if self.at_bat_num == 0:
                self.last_no_play = (self.current_inning, self.inning_half, 1, self.pitch_num+1)
            else:
                self.last_no_play = (self.current_inning, self.inning_half, self.at_bat_num, self.pitch_num+1)
            return
        # If we are in a new batter, update the at bat number and batter
        if self.batter_id != self.player_dict_retro[tokens[3]]:
            self.at_bat_num += 1
            self.batter_id = self.player_dict_retro[tokens[3]]
        
        #Put the batter at "home" cause they are batting
        self.bases['0'] = self.batter_id
        
        
        self.last_play = (self.current_inning, self.inning_half, self.at_bat_num, self.pitch_num)
        
        # Update the pitch metadata before the base state/outs/score changes due to the play
        if self.pitch_num == 0:
            self.pitch_num = 1
        update_pitch_data = (self.home_score, self.away_score, self.cur_outs, self.game_id, self.current_inning, self.inning_half, self.at_bat_num, self.pitch_num)
        self.update_pitch_metadata.append(update_pitch_data)
        # Push the base state before the play changes it.
        
        #pitch_char = tokens[5][-1]
        play_details = tokens[6]
        play_details = play_details.replace('!', '')
        play_details = play_details.replace('#', '')
        basic_play, modifiers, advancements = self.parse_play(play_details)
        adv = advancements.copy()
        # print(f"Play: {play_details}, Basic Play: {basic_play}, Modifiers: {modifiers}, Advancements: {advancements}")
        # print(f'Current Bases: {self.bases}, Current Outs: {self.cur_outs}, Current Score: {self.home_score}-{self.away_score}, current_inning: {self.current_inning}, current_half: {self.inning_half}, batter: {tokens[3]}')
        first, second, third = self.get_base_state()
        push_base_state = (self.game_id, self.current_inning, self.inning_half, self.at_bat_num, self.pitch_num, first, second, third)
        self.base_states.append(push_base_state)
        try:
            self.process_play(basic_play, modifiers, adv)
        except Exception as e:
            print(f"Error processing play: {play_details}, basic_play: {basic_play}, advancements: {advancements} Error: {e}")
            raise e
        
        # If there is 3 outs reset the base states


            
        
        

    def get_base_state(self):
        return self.bases['1'], self.bases['2'], self.bases['3']

    def get_defense_state_at(self, team_flag, game_time):
        return self.defense[team_flag].get(game_time)

    def get_lineup_state_at(self, team_flag, game_time):
        return self.lineups[team_flag].get(game_time)
    
    def get_game_subs(self):
        return self.game_subs
    
    def get_lineups(self):
        return self.lineups

    def get_base_states(self):
        return self.base_states
    def get_pitch_metadata(self):
        return self.update_pitch_metadata
    def get_base_running_events(self):
        return self.base_running_events
    def get_fielding_sequences(self):
        return self.fielding_sequences
    def get_game_comments(self):
        return self.game_comments
    def get_adjustments(self):
        return self.adjustments

    
    def get_home_score(self):
        return self.home_score
    def get_away_score(self):
        return self.away_score
    
    def get_defenses(self):
        defenses = {'0': [], '1': []}
        home = False
        game_id = self.game_id
        for team_flag in self.defense:
            if team_flag == '1':
                home = True
            else:
                home = False
            for game_time in self.defense[team_flag]:
                inn, half, ab, pn = game_time
                defense = (game_id, inn, half, ab, pn, self.defense[team_flag][game_time][1], self.defense[team_flag][game_time][2], self.defense[team_flag][game_time][3], self.defense[team_flag][game_time][4], self.defense[team_flag][game_time][5], self.defense[team_flag][game_time][6], self.defense[team_flag][game_time][7], self.defense[team_flag][game_time][8], self.defense[team_flag][game_time][9], home)
                defenses[team_flag].append(defense)
        return defenses
    
    def get_lineups(self):
        lineups = {'0': [], '1': []}
        home = False
        game_id = self.game_id
        for team_flag in self.lineups:
            if team_flag == '1':
                home = True
            else:
                home = False
            for game_time in self.lineups[team_flag]:
                inn, half, ab, pn = game_time
                lineup = (game_id, inn, half, ab, pn, self.lineups[team_flag][game_time][1], self.lineups[team_flag][game_time][2], self.lineups[team_flag][game_time][3], self.lineups[team_flag][game_time][4], self.lineups[team_flag][game_time][5], self.lineups[team_flag][game_time][6], self.lineups[team_flag][game_time][7], self.lineups[team_flag][game_time][8], self.lineups[team_flag][game_time][9], home)
                lineups[team_flag].append(lineup)
        return lineups

        