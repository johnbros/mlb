--
-- PostgreSQL database dump
--

-- Dumped from database version 17.4
-- Dumped by pg_dump version 17.4

-- Started on 2025-03-20 15:32:16

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 247 (class 1255 OID 522287)
-- Name: update_was_used(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.update_was_used() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE roster_games
    SET was_used = TRUE
    WHERE roster_games.player_id = NEW.player_id
      AND roster_games.game_id = NEW.game_id
      AND roster_games.team_id = NEW.team_id;
    RETURN NEW;
END;
$$;


ALTER FUNCTION public.update_was_used() OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 230 (class 1259 OID 514031)
-- Name: ab_outcomes; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ab_outcomes (
    ab_outcome_id integer NOT NULL,
    ab_outcome_label text NOT NULL
);


ALTER TABLE public.ab_outcomes OWNER TO postgres;

--
-- TOC entry 229 (class 1259 OID 514030)
-- Name: ab_outcomes_ab_outcome_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.ab_outcomes_ab_outcome_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ab_outcomes_ab_outcome_id_seq OWNER TO postgres;

--
-- TOC entry 5059 (class 0 OID 0)
-- Dependencies: 229
-- Name: ab_outcomes_ab_outcome_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.ab_outcomes_ab_outcome_id_seq OWNED BY public.ab_outcomes.ab_outcome_id;


--
-- TOC entry 226 (class 1259 OID 513994)
-- Name: at_bats; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.at_bats (
    inning_num integer NOT NULL,
    inning_half character(1) NOT NULL,
    batter_id integer NOT NULL,
    at_bat_num integer NOT NULL,
    pitcher_id integer NOT NULL,
    ab_outcome_id integer NOT NULL,
    game_id integer NOT NULL,
    at_bat_des text NOT NULL,
    outs integer NOT NULL,
    CONSTRAINT at_bats_inning_half_check CHECK ((inning_half = ANY (ARRAY['T'::bpchar, 'B'::bpchar])))
);


ALTER TABLE public.at_bats OWNER TO postgres;

--
-- TOC entry 246 (class 1259 OID 522265)
-- Name: bip_data; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bip_data (
    bip_data_id integer NOT NULL,
    pitch_data_id integer NOT NULL,
    hit_angle numeric,
    hit_speed numeric,
    hc_y_ft numeric,
    hc_x_ft numeric,
    isbarrel boolean DEFAULT false,
    distance numeric,
    bat_speed numeric
);


ALTER TABLE public.bip_data OWNER TO postgres;

--
-- TOC entry 245 (class 1259 OID 522264)
-- Name: bip_data_bip_data_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.bip_data_bip_data_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.bip_data_bip_data_id_seq OWNER TO postgres;

--
-- TOC entry 5060 (class 0 OID 0)
-- Dependencies: 245
-- Name: bip_data_bip_data_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.bip_data_bip_data_id_seq OWNED BY public.bip_data.bip_data_id;


--
-- TOC entry 220 (class 1259 OID 513783)
-- Name: divisions; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.divisions (
    division_id integer NOT NULL,
    division_name text NOT NULL
);


ALTER TABLE public.divisions OWNER TO postgres;

--
-- TOC entry 217 (class 1259 OID 116388)
-- Name: game_info; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.game_info (
    game_id text NOT NULL,
    game_date date NOT NULL,
    league text NOT NULL,
    game_type text NOT NULL,
    game_data jsonb
);


ALTER TABLE public.game_info OWNER TO postgres;

--
-- TOC entry 223 (class 1259 OID 513916)
-- Name: game_types; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.game_types (
    type_id integer NOT NULL,
    type_name text NOT NULL
);


ALTER TABLE public.game_types OWNER TO postgres;

--
-- TOC entry 222 (class 1259 OID 513915)
-- Name: game_types_type_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.game_types_type_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.game_types_type_id_seq OWNER TO postgres;

--
-- TOC entry 5061 (class 0 OID 0)
-- Dependencies: 222
-- Name: game_types_type_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.game_types_type_id_seq OWNED BY public.game_types.type_id;


--
-- TOC entry 224 (class 1259 OID 513924)
-- Name: games; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.games (
    h_team_id integer NOT NULL,
    a_team_id integer NOT NULL,
    venue_id integer NOT NULL,
    level_id integer NOT NULL,
    type_id integer NOT NULL,
    date date NOT NULL,
    game_id integer NOT NULL,
    season_year integer GENERATED ALWAYS AS (EXTRACT(year FROM date)) STORED
);


ALTER TABLE public.games OWNER TO postgres;

--
-- TOC entry 225 (class 1259 OID 513956)
-- Name: innings; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.innings (
    inning_num integer NOT NULL,
    inning_half character(1) NOT NULL,
    runs_scored integer NOT NULL,
    hits integer NOT NULL,
    game_id integer NOT NULL,
    CONSTRAINT innings_inning_half_check CHECK ((inning_half = ANY (ARRAY['T'::bpchar, 'B'::bpchar])))
);


ALTER TABLE public.innings OWNER TO postgres;

--
-- TOC entry 219 (class 1259 OID 513776)
-- Name: leagues; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.leagues (
    league_id integer NOT NULL,
    league_name text NOT NULL
);


ALTER TABLE public.leagues OWNER TO postgres;

--
-- TOC entry 221 (class 1259 OID 513903)
-- Name: levels; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.levels (
    level_id integer NOT NULL,
    level_name text NOT NULL,
    level_abbreviation text NOT NULL
);


ALTER TABLE public.levels OWNER TO postgres;

--
-- TOC entry 244 (class 1259 OID 522251)
-- Name: pitch_data; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.pitch_data (
    pitch_data_id integer NOT NULL,
    pitch_type_id integer NOT NULL,
    initial_speed numeric,
    final_speed numeric,
    x0 numeric,
    y0 numeric,
    z0 numeric,
    vx0 numeric,
    vy0 numeric,
    vz0 numeric,
    ax numeric,
    ay numeric,
    az numeric,
    px numeric,
    pz numeric,
    spin_rate numeric,
    sz_top numeric,
    sz_bot numeric,
    pfxx numeric,
    pfxz numeric,
    pfxzwithgravity numeric,
    pfxxwithgravity numeric,
    extension numeric
);


ALTER TABLE public.pitch_data OWNER TO postgres;

--
-- TOC entry 243 (class 1259 OID 522250)
-- Name: pitch_data_pitch_data_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.pitch_data_pitch_data_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.pitch_data_pitch_data_id_seq OWNER TO postgres;

--
-- TOC entry 5062 (class 0 OID 0)
-- Dependencies: 243
-- Name: pitch_data_pitch_data_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.pitch_data_pitch_data_id_seq OWNED BY public.pitch_data.pitch_data_id;


--
-- TOC entry 228 (class 1259 OID 514022)
-- Name: pitch_outcomes; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.pitch_outcomes (
    p_outcome_id integer NOT NULL,
    p_description text NOT NULL,
    p_result_code text NOT NULL,
    p_call_name text NOT NULL,
    p_call text NOT NULL
);


ALTER TABLE public.pitch_outcomes OWNER TO postgres;

--
-- TOC entry 227 (class 1259 OID 514021)
-- Name: pitch_outcomes_p_outcome_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.pitch_outcomes_p_outcome_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.pitch_outcomes_p_outcome_id_seq OWNER TO postgres;

--
-- TOC entry 5063 (class 0 OID 0)
-- Dependencies: 227
-- Name: pitch_outcomes_p_outcome_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.pitch_outcomes_p_outcome_id_seq OWNED BY public.pitch_outcomes.p_outcome_id;


--
-- TOC entry 242 (class 1259 OID 522242)
-- Name: pitch_type; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.pitch_type (
    pitch_type_id integer NOT NULL,
    pitch_type_code text NOT NULL,
    pitch_type_name text NOT NULL
);


ALTER TABLE public.pitch_type OWNER TO postgres;

--
-- TOC entry 241 (class 1259 OID 522241)
-- Name: pitch_type_pitch_type_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.pitch_type_pitch_type_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.pitch_type_pitch_type_id_seq OWNER TO postgres;

--
-- TOC entry 5064 (class 0 OID 0)
-- Dependencies: 241
-- Name: pitch_type_pitch_type_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.pitch_type_pitch_type_id_seq OWNED BY public.pitch_type.pitch_type_id;


--
-- TOC entry 240 (class 1259 OID 514436)
-- Name: pitches; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.pitches (
    inning_num integer NOT NULL,
    at_bat_num integer NOT NULL,
    game_id integer NOT NULL,
    inning_half character(1) NOT NULL,
    pitch_num integer NOT NULL,
    pitch_outcome_id integer NOT NULL,
    pitch_data_id integer,
    balls integer NOT NULL,
    strikes integer NOT NULL,
    CONSTRAINT pitches_inning_half_check CHECK ((inning_half = ANY (ARRAY['T'::bpchar, 'B'::bpchar])))
);


ALTER TABLE public.pitches OWNER TO postgres;

--
-- TOC entry 239 (class 1259 OID 514401)
-- Name: player_games_fielding; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.player_games_fielding (
    player_id integer NOT NULL,
    game_id integer NOT NULL,
    team_id integer NOT NULL,
    putouts integer DEFAULT 0,
    assists integer DEFAULT 0,
    errors integer DEFAULT 0,
    chances integer GENERATED ALWAYS AS (((putouts + assists) + errors)) STORED,
    caught_stealing integer DEFAULT 0,
    stolen_bases_allowed integer DEFAULT 0,
    passed_balls integer DEFAULT 0,
    pickoffs integer DEFAULT 0,
    position_id integer NOT NULL
);


ALTER TABLE public.player_games_fielding OWNER TO postgres;

--
-- TOC entry 235 (class 1259 OID 514204)
-- Name: player_games_hitting; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.player_games_hitting (
    player_id integer NOT NULL,
    game_id integer NOT NULL,
    team_id integer NOT NULL,
    at_bats integer DEFAULT 0,
    hits integer DEFAULT 0,
    walks integer DEFAULT 0,
    total_bases integer DEFAULT 0,
    rbis integer DEFAULT 0,
    hbp integer DEFAULT 0,
    hrs integer DEFAULT 0,
    doubles integer DEFAULT 0,
    triples integer DEFAULT 0,
    strikeouts integer DEFAULT 0,
    plate_appearances integer DEFAULT 0,
    runs integer DEFAULT 0,
    stolen_bases integer DEFAULT 0,
    caught_stealing integer DEFAULT 0,
    ground_into_double_play integer DEFAULT 0,
    sac_bunts integer DEFAULT 0,
    sac_flies integer DEFAULT 0,
    left_on_base integer DEFAULT 0,
    position_id integer NOT NULL,
    ground_into_triple_play integer DEFAULT 0,
    line_outs integer DEFAULT 0,
    pop_outs integer DEFAULT 0,
    fly_outs integer DEFAULT 0,
    air_outs integer DEFAULT 0,
    ground_outs integer DEFAULT 0,
    catchers_interference integer DEFAULT 0
);


ALTER TABLE public.player_games_hitting OWNER TO postgres;

--
-- TOC entry 238 (class 1259 OID 514356)
-- Name: player_games_pitching; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.player_games_pitching (
    player_id integer NOT NULL,
    game_id integer NOT NULL,
    team_id integer NOT NULL,
    innings_pitched numeric(3,1) DEFAULT 0.0,
    batters_faced integer DEFAULT 0,
    earned_runs integer DEFAULT 0,
    runs_allowed integer DEFAULT 0,
    hits_allowed integer DEFAULT 0,
    strikeouts integer DEFAULT 0,
    walks integer DEFAULT 0,
    home_runs_allowed integer DEFAULT 0,
    hit_by_pitch integer DEFAULT 0,
    wild_pitches integer DEFAULT 0,
    balks integer DEFAULT 0,
    pickoffs integer DEFAULT 0,
    complete_game boolean DEFAULT false,
    shutout boolean DEFAULT false,
    save_opportunity boolean DEFAULT false,
    inherited_runners integer DEFAULT 0,
    inherited_runners_scored integer DEFAULT 0,
    pitches_thrown integer DEFAULT 0,
    strikes_thrown integer DEFAULT 0,
    balls_thrown integer DEFAULT 0,
    save boolean DEFAULT false,
    doubles_allowed integer DEFAULT 0,
    triples_allowed integer DEFAULT 0,
    air_outs integer DEFAULT 0,
    line_outs integer DEFAULT 0,
    fly_outs integer DEFAULT 0,
    pop_outs integer DEFAULT 0,
    ground_outs integer DEFAULT 0,
    win boolean DEFAULT false,
    loss boolean DEFAULT false
);


ALTER TABLE public.player_games_pitching OWNER TO postgres;

--
-- TOC entry 233 (class 1259 OID 514176)
-- Name: players; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.players (
    player_id integer NOT NULL,
    player_name text NOT NULL,
    player_birthday date NOT NULL,
    position_id integer NOT NULL,
    bat_side character varying(1) NOT NULL,
    throw_side character varying(1) NOT NULL,
    sz_top numeric NOT NULL,
    sz_bot numeric NOT NULL
);


ALTER TABLE public.players OWNER TO postgres;

--
-- TOC entry 236 (class 1259 OID 514267)
-- Name: positions; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.positions (
    position_id integer NOT NULL,
    position_name text NOT NULL
);


ALTER TABLE public.positions OWNER TO postgres;

--
-- TOC entry 234 (class 1259 OID 514183)
-- Name: roster_games; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.roster_games (
    player_id integer NOT NULL,
    game_id integer NOT NULL,
    team_id integer NOT NULL,
    was_used boolean DEFAULT false
);


ALTER TABLE public.roster_games OWNER TO postgres;

--
-- TOC entry 237 (class 1259 OID 514279)
-- Name: team_games; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.team_games (
    game_id integer NOT NULL,
    team_id integer NOT NULL,
    runs_scored integer DEFAULT 0,
    errors integer DEFAULT 0,
    hits integer DEFAULT 0,
    earned_runs_allowed integer DEFAULT 0,
    total_pitches_thrown integer DEFAULT 0,
    innings_played numeric DEFAULT 9,
    runs_allowed integer
);


ALTER TABLE public.team_games OWNER TO postgres;

--
-- TOC entry 231 (class 1259 OID 514114)
-- Name: team_seasons; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.team_seasons (
    team_id integer NOT NULL,
    season_year integer NOT NULL,
    venue_id integer NOT NULL,
    league_id integer NOT NULL,
    spring_venue_id integer NOT NULL,
    spring_league_id integer NOT NULL,
    division_id integer NOT NULL,
    level_id integer NOT NULL
);


ALTER TABLE public.team_seasons OWNER TO postgres;

--
-- TOC entry 232 (class 1259 OID 514149)
-- Name: teams; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.teams (
    team_id integer NOT NULL,
    team_abr text,
    team_code text,
    club_name text,
    team_name text,
    team_location text,
    first_year integer
);


ALTER TABLE public.teams OWNER TO postgres;

--
-- TOC entry 218 (class 1259 OID 513769)
-- Name: venues; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.venues (
    venue_id integer NOT NULL,
    venue_name text
);


ALTER TABLE public.venues OWNER TO postgres;

--
-- TOC entry 4742 (class 2604 OID 514034)
-- Name: ab_outcomes ab_outcome_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ab_outcomes ALTER COLUMN ab_outcome_id SET DEFAULT nextval('public.ab_outcomes_ab_outcome_id_seq'::regclass);


--
-- TOC entry 4815 (class 2604 OID 522268)
-- Name: bip_data bip_data_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bip_data ALTER COLUMN bip_data_id SET DEFAULT nextval('public.bip_data_bip_data_id_seq'::regclass);


--
-- TOC entry 4739 (class 2604 OID 513919)
-- Name: game_types type_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.game_types ALTER COLUMN type_id SET DEFAULT nextval('public.game_types_type_id_seq'::regclass);


--
-- TOC entry 4814 (class 2604 OID 522254)
-- Name: pitch_data pitch_data_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pitch_data ALTER COLUMN pitch_data_id SET DEFAULT nextval('public.pitch_data_pitch_data_id_seq'::regclass);


--
-- TOC entry 4741 (class 2604 OID 514025)
-- Name: pitch_outcomes p_outcome_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pitch_outcomes ALTER COLUMN p_outcome_id SET DEFAULT nextval('public.pitch_outcomes_p_outcome_id_seq'::regclass);


--
-- TOC entry 4813 (class 2604 OID 522245)
-- Name: pitch_type pitch_type_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pitch_type ALTER COLUMN pitch_type_id SET DEFAULT nextval('public.pitch_type_pitch_type_id_seq'::regclass);


--
-- TOC entry 4843 (class 2606 OID 514038)
-- Name: ab_outcomes ab_outcomes_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ab_outcomes
    ADD CONSTRAINT ab_outcomes_pkey PRIMARY KEY (ab_outcome_id);


--
-- TOC entry 4839 (class 2606 OID 514108)
-- Name: at_bats at_bats_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.at_bats
    ADD CONSTRAINT at_bats_pkey PRIMARY KEY (game_id, at_bat_num, inning_half, inning_num);


--
-- TOC entry 4869 (class 2606 OID 522273)
-- Name: bip_data bip_data_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bip_data
    ADD CONSTRAINT bip_data_pkey PRIMARY KEY (bip_data_id);


--
-- TOC entry 4829 (class 2606 OID 513789)
-- Name: divisions divisions_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.divisions
    ADD CONSTRAINT divisions_pkey PRIMARY KEY (division_id);


--
-- TOC entry 4821 (class 2606 OID 116394)
-- Name: game_info game_info_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.game_info
    ADD CONSTRAINT game_info_pkey PRIMARY KEY (game_id);


--
-- TOC entry 4833 (class 2606 OID 513923)
-- Name: game_types game_types_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.game_types
    ADD CONSTRAINT game_types_pkey PRIMARY KEY (type_id);


--
-- TOC entry 4835 (class 2606 OID 514092)
-- Name: games games_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.games
    ADD CONSTRAINT games_pkey PRIMARY KEY (game_id);


--
-- TOC entry 4837 (class 2606 OID 514099)
-- Name: innings innings_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.innings
    ADD CONSTRAINT innings_pkey PRIMARY KEY (game_id, inning_half, inning_num);


--
-- TOC entry 4827 (class 2606 OID 513782)
-- Name: leagues leagues_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.leagues
    ADD CONSTRAINT leagues_pkey PRIMARY KEY (league_id);


--
-- TOC entry 4831 (class 2606 OID 513909)
-- Name: levels levels_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.levels
    ADD CONSTRAINT levels_pkey PRIMARY KEY (level_id);


--
-- TOC entry 4867 (class 2606 OID 522258)
-- Name: pitch_data pitch_data_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pitch_data
    ADD CONSTRAINT pitch_data_pkey PRIMARY KEY (pitch_data_id);


--
-- TOC entry 4841 (class 2606 OID 514029)
-- Name: pitch_outcomes pitch_outcomes_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pitch_outcomes
    ADD CONSTRAINT pitch_outcomes_pkey PRIMARY KEY (p_outcome_id);


--
-- TOC entry 4865 (class 2606 OID 522249)
-- Name: pitch_type pitch_type_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pitch_type
    ADD CONSTRAINT pitch_type_pkey PRIMARY KEY (pitch_type_id);


--
-- TOC entry 4863 (class 2606 OID 514441)
-- Name: pitches pitches_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pitches
    ADD CONSTRAINT pitches_pkey PRIMARY KEY (game_id, inning_num, inning_half, at_bat_num, pitch_num);


--
-- TOC entry 4861 (class 2606 OID 522284)
-- Name: player_games_fielding player_games_fielding_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.player_games_fielding
    ADD CONSTRAINT player_games_fielding_pkey PRIMARY KEY (player_id, game_id, team_id);


--
-- TOC entry 4853 (class 2606 OID 522286)
-- Name: player_games_hitting player_games_hitting_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.player_games_hitting
    ADD CONSTRAINT player_games_hitting_pkey PRIMARY KEY (player_id, game_id, team_id);


--
-- TOC entry 4859 (class 2606 OID 522282)
-- Name: player_games_pitching player_games_pitching_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.player_games_pitching
    ADD CONSTRAINT player_games_pitching_pkey PRIMARY KEY (player_id, game_id, team_id);


--
-- TOC entry 4849 (class 2606 OID 514182)
-- Name: players players_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.players
    ADD CONSTRAINT players_pkey PRIMARY KEY (player_id);


--
-- TOC entry 4855 (class 2606 OID 514273)
-- Name: positions positions_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.positions
    ADD CONSTRAINT positions_pkey PRIMARY KEY (position_id);


--
-- TOC entry 4851 (class 2606 OID 522280)
-- Name: roster_games roster_games_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.roster_games
    ADD CONSTRAINT roster_games_pkey PRIMARY KEY (player_id, game_id, team_id);


--
-- TOC entry 4857 (class 2606 OID 514294)
-- Name: team_games team_games_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.team_games
    ADD CONSTRAINT team_games_pkey PRIMARY KEY (team_id, game_id);


--
-- TOC entry 4845 (class 2606 OID 514118)
-- Name: team_seasons team_seasons_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.team_seasons
    ADD CONSTRAINT team_seasons_pkey PRIMARY KEY (team_id, season_year);


--
-- TOC entry 4847 (class 2606 OID 514155)
-- Name: teams teams_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.teams
    ADD CONSTRAINT teams_pkey PRIMARY KEY (team_id);


--
-- TOC entry 4825 (class 2606 OID 513775)
-- Name: venues venues_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.venues
    ADD CONSTRAINT venues_pkey PRIMARY KEY (venue_id);


--
-- TOC entry 4822 (class 1259 OID 513766)
-- Name: idx_game_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_game_date ON public.game_info USING btree (game_date);


--
-- TOC entry 4823 (class 1259 OID 513767)
-- Name: idx_league; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_league ON public.game_info USING btree (league);


--
-- TOC entry 4906 (class 2620 OID 522288)
-- Name: player_games_hitting trigger_update_roster_batting; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trigger_update_roster_batting AFTER INSERT ON public.player_games_hitting FOR EACH ROW EXECUTE FUNCTION public.update_was_used();


--
-- TOC entry 4908 (class 2620 OID 522290)
-- Name: player_games_fielding trigger_update_roster_fielding; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trigger_update_roster_fielding AFTER INSERT ON public.player_games_fielding FOR EACH ROW EXECUTE FUNCTION public.update_was_used();


--
-- TOC entry 4907 (class 2620 OID 522289)
-- Name: player_games_pitching trigger_update_roster_pitching; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trigger_update_roster_pitching AFTER INSERT ON public.player_games_pitching FOR EACH ROW EXECUTE FUNCTION public.update_was_used();


--
-- TOC entry 4877 (class 2606 OID 514109)
-- Name: at_bats at_bats_game_id_inning_num_inning_half_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.at_bats
    ADD CONSTRAINT at_bats_game_id_inning_num_inning_half_fkey FOREIGN KEY (game_id, inning_num, inning_half) REFERENCES public.innings(game_id, inning_num, inning_half);


--
-- TOC entry 4905 (class 2606 OID 522274)
-- Name: bip_data bip_data_pitch_data_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bip_data
    ADD CONSTRAINT bip_data_pitch_data_id_fkey FOREIGN KEY (pitch_data_id) REFERENCES public.pitch_data(pitch_data_id);


--
-- TOC entry 4878 (class 2606 OID 514044)
-- Name: at_bats fk_ab_outcome_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.at_bats
    ADD CONSTRAINT fk_ab_outcome_id FOREIGN KEY (ab_outcome_id) REFERENCES public.ab_outcomes(ab_outcome_id);


--
-- TOC entry 4870 (class 2606 OID 514166)
-- Name: games fk_games_a_team_season; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.games
    ADD CONSTRAINT fk_games_a_team_season FOREIGN KEY (a_team_id, season_year) REFERENCES public.team_seasons(team_id, season_year) ON DELETE CASCADE;


--
-- TOC entry 4871 (class 2606 OID 514161)
-- Name: games fk_games_h_team_season; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.games
    ADD CONSTRAINT fk_games_h_team_season FOREIGN KEY (h_team_id, season_year) REFERENCES public.team_seasons(team_id, season_year) ON DELETE CASCADE;


--
-- TOC entry 4879 (class 2606 OID 514171)
-- Name: team_seasons fk_team_seasons_teams; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.team_seasons
    ADD CONSTRAINT fk_team_seasons_teams FOREIGN KEY (team_id) REFERENCES public.teams(team_id);


--
-- TOC entry 4872 (class 2606 OID 513946)
-- Name: games games_level_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.games
    ADD CONSTRAINT games_level_id_fkey FOREIGN KEY (level_id) REFERENCES public.levels(level_id);


--
-- TOC entry 4873 (class 2606 OID 513951)
-- Name: games games_type_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.games
    ADD CONSTRAINT games_type_id_fkey FOREIGN KEY (type_id) REFERENCES public.game_types(type_id);


--
-- TOC entry 4874 (class 2606 OID 513941)
-- Name: games games_venue_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.games
    ADD CONSTRAINT games_venue_id_fkey FOREIGN KEY (venue_id) REFERENCES public.venues(venue_id);


--
-- TOC entry 4875 (class 2606 OID 514100)
-- Name: innings innings_game_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.innings
    ADD CONSTRAINT innings_game_id_fkey FOREIGN KEY (game_id) REFERENCES public.games(game_id);


--
-- TOC entry 4876 (class 2606 OID 514093)
-- Name: innings innings_game_id_int_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.innings
    ADD CONSTRAINT innings_game_id_int_fkey FOREIGN KEY (game_id) REFERENCES public.games(game_id);


--
-- TOC entry 4904 (class 2606 OID 522259)
-- Name: pitch_data pitch_data_pitch_type_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pitch_data
    ADD CONSTRAINT pitch_data_pitch_type_id_fkey FOREIGN KEY (pitch_type_id) REFERENCES public.pitch_type(pitch_type_id);


--
-- TOC entry 4903 (class 2606 OID 514442)
-- Name: pitches pitches_pitch_outcome_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pitches
    ADD CONSTRAINT pitches_pitch_outcome_id_fkey FOREIGN KEY (pitch_outcome_id) REFERENCES public.pitch_outcomes(p_outcome_id) ON DELETE CASCADE;


--
-- TOC entry 4899 (class 2606 OID 514419)
-- Name: player_games_fielding player_games_fielding_game_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.player_games_fielding
    ADD CONSTRAINT player_games_fielding_game_id_fkey FOREIGN KEY (game_id) REFERENCES public.games(game_id) ON DELETE CASCADE;


--
-- TOC entry 4900 (class 2606 OID 514414)
-- Name: player_games_fielding player_games_fielding_player_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.player_games_fielding
    ADD CONSTRAINT player_games_fielding_player_id_fkey FOREIGN KEY (player_id) REFERENCES public.players(player_id) ON DELETE CASCADE;


--
-- TOC entry 4901 (class 2606 OID 522291)
-- Name: player_games_fielding player_games_fielding_position_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.player_games_fielding
    ADD CONSTRAINT player_games_fielding_position_id_fkey FOREIGN KEY (position_id) REFERENCES public.positions(position_id);


--
-- TOC entry 4902 (class 2606 OID 514424)
-- Name: player_games_fielding player_games_fielding_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.player_games_fielding
    ADD CONSTRAINT player_games_fielding_team_id_fkey FOREIGN KEY (team_id) REFERENCES public.teams(team_id) ON DELETE CASCADE;


--
-- TOC entry 4890 (class 2606 OID 514341)
-- Name: player_games_hitting player_games_hitting_game_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.player_games_hitting
    ADD CONSTRAINT player_games_hitting_game_id_fkey FOREIGN KEY (game_id) REFERENCES public.games(game_id);


--
-- TOC entry 4891 (class 2606 OID 514393)
-- Name: player_games_hitting player_games_hitting_player_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.player_games_hitting
    ADD CONSTRAINT player_games_hitting_player_id_fkey FOREIGN KEY (player_id) REFERENCES public.players(player_id) ON DELETE CASCADE;


--
-- TOC entry 4892 (class 2606 OID 522296)
-- Name: player_games_hitting player_games_hitting_position_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.player_games_hitting
    ADD CONSTRAINT player_games_hitting_position_id_fkey FOREIGN KEY (position_id) REFERENCES public.positions(position_id);


--
-- TOC entry 4893 (class 2606 OID 514336)
-- Name: player_games_hitting player_games_hitting_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.player_games_hitting
    ADD CONSTRAINT player_games_hitting_team_id_fkey FOREIGN KEY (team_id) REFERENCES public.teams(team_id);


--
-- TOC entry 4896 (class 2606 OID 514383)
-- Name: player_games_pitching player_games_pitching_game_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.player_games_pitching
    ADD CONSTRAINT player_games_pitching_game_id_fkey FOREIGN KEY (game_id) REFERENCES public.games(game_id) ON DELETE CASCADE;


--
-- TOC entry 4897 (class 2606 OID 514378)
-- Name: player_games_pitching player_games_pitching_player_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.player_games_pitching
    ADD CONSTRAINT player_games_pitching_player_id_fkey FOREIGN KEY (player_id) REFERENCES public.players(player_id) ON DELETE CASCADE;


--
-- TOC entry 4898 (class 2606 OID 514388)
-- Name: player_games_pitching player_games_pitching_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.player_games_pitching
    ADD CONSTRAINT player_games_pitching_team_id_fkey FOREIGN KEY (team_id) REFERENCES public.teams(team_id) ON DELETE CASCADE;


--
-- TOC entry 4886 (class 2606 OID 514274)
-- Name: players players_position_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.players
    ADD CONSTRAINT players_position_id_fkey FOREIGN KEY (position_id) REFERENCES public.positions(position_id);


--
-- TOC entry 4887 (class 2606 OID 514194)
-- Name: roster_games roster_games_game_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.roster_games
    ADD CONSTRAINT roster_games_game_id_fkey FOREIGN KEY (game_id) REFERENCES public.games(game_id);


--
-- TOC entry 4888 (class 2606 OID 514189)
-- Name: roster_games roster_games_player_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.roster_games
    ADD CONSTRAINT roster_games_player_id_fkey FOREIGN KEY (player_id) REFERENCES public.players(player_id);


--
-- TOC entry 4889 (class 2606 OID 514199)
-- Name: roster_games roster_games_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.roster_games
    ADD CONSTRAINT roster_games_team_id_fkey FOREIGN KEY (team_id) REFERENCES public.teams(team_id);


--
-- TOC entry 4894 (class 2606 OID 514295)
-- Name: team_games team_games_game_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.team_games
    ADD CONSTRAINT team_games_game_id_fkey FOREIGN KEY (game_id) REFERENCES public.games(game_id);


--
-- TOC entry 4895 (class 2606 OID 514300)
-- Name: team_games team_games_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.team_games
    ADD CONSTRAINT team_games_team_id_fkey FOREIGN KEY (team_id) REFERENCES public.teams(team_id);


--
-- TOC entry 4880 (class 2606 OID 514119)
-- Name: team_seasons team_seasons_division_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.team_seasons
    ADD CONSTRAINT team_seasons_division_id_fkey FOREIGN KEY (division_id) REFERENCES public.divisions(division_id);


--
-- TOC entry 4881 (class 2606 OID 514124)
-- Name: team_seasons team_seasons_league_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.team_seasons
    ADD CONSTRAINT team_seasons_league_id_fkey FOREIGN KEY (league_id) REFERENCES public.leagues(league_id);


--
-- TOC entry 4882 (class 2606 OID 514139)
-- Name: team_seasons team_seasons_level_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.team_seasons
    ADD CONSTRAINT team_seasons_level_id_fkey FOREIGN KEY (level_id) REFERENCES public.levels(level_id);


--
-- TOC entry 4883 (class 2606 OID 514134)
-- Name: team_seasons team_seasons_spring_league_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.team_seasons
    ADD CONSTRAINT team_seasons_spring_league_id_fkey FOREIGN KEY (spring_league_id) REFERENCES public.leagues(league_id);


--
-- TOC entry 4884 (class 2606 OID 514144)
-- Name: team_seasons team_seasons_spring_venue_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.team_seasons
    ADD CONSTRAINT team_seasons_spring_venue_id_fkey FOREIGN KEY (spring_venue_id) REFERENCES public.venues(venue_id);


--
-- TOC entry 4885 (class 2606 OID 514129)
-- Name: team_seasons team_seasons_venue_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.team_seasons
    ADD CONSTRAINT team_seasons_venue_id_fkey FOREIGN KEY (venue_id) REFERENCES public.venues(venue_id);


-- Completed on 2025-03-20 15:32:17

--
-- PostgreSQL database dump complete
--

