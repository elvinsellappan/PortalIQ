-- Stores team information including metadata and affiliations.
CREATE TABLE teams (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    short_name TEXT,
    conference TEXT,
    school_id INTEGER,
    logo_url TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Stores player profiles and their current team link.
CREATE TABLE players (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    full_name TEXT NOT NULL,
    position TEXT,
    height TEXT,
    weight INTEGER,
    class_year TEXT,
    hometown TEXT,
    prev_school TEXT,
    cfbd_player_id INTEGER,
    current_team_id UUID REFERENCES teams(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Stores unique season years.
CREATE TABLE seasons (
    id SERIAL PRIMARY KEY,
    year INTEGER UNIQUE NOT NULL
);

-- Stores per-season player statistics with raw source data.
CREATE TABLE player_season_stats (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    player_id UUID REFERENCES players(id) ON DELETE CASCADE,
    team_id UUID REFERENCES teams(id) ON DELETE SET NULL,
    season_id INTEGER REFERENCES seasons(id) ON DELETE CASCADE,
    games_played INTEGER,
    snaps INTEGER,
    targets INTEGER,
    receptions INTEGER,
    yards INTEGER,
    tds INTEGER,
    tackles INTEGER,
    pass_breakups INTEGER,
    ints INTEGER,
    raw_source JSONB,
    UNIQUE (player_id, season_id)
);

-- Stores computed TVI scores for players by season and model version.
CREATE TABLE tvi_scores (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    player_id UUID REFERENCES players(id) ON DELETE CASCADE,
    team_id UUID REFERENCES teams(id) ON DELETE SET NULL,
    season_id INTEGER REFERENCES seasons(id) ON DELETE CASCADE,
    tvi NUMERIC(6,2) NOT NULL,
    components JSONB,
    model_version TEXT NOT NULL DEFAULT 'v1',
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (player_id, season_id, model_version)
);

CREATE INDEX ON tvi_scores (season_id, tvi DESC);
CREATE INDEX ON player_season_stats (player_id, season_id);
