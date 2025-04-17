

# MLB API Base URLs, IDs, and Endpoints

# Base URLs
MLB_API_BASE = "https://statsapi.mlb.com"
MLB_API_VERSION = "/api/v1"

# Sport IDs
# These are the sport IDs for the different levels of baseball
MLB_SPORT_ID = "1"
AAA_SPORT_ID = "11"
AA_SPORT_ID = "12"
A_PLUS_SPORT_ID = "13"
A_SPORT_ID = "14"

# Game/Schedule Endpoints
SCHEDULE_ENDPOINT = f"{MLB_API_BASE}{MLB_API_VERSION}/schedule"

# Team Endpoints
TEAMS_ENDPOINT = f"{MLB_API_BASE}{MLB_API_VERSION}/teams"

# Player Endpoints
PEOPLE_ENDPOINT = f"{MLB_API_BASE}{MLB_API_VERSION}/people"

# league Endpoints
LEAGUES_ENDPOINT = f"{MLB_API_BASE}{MLB_API_VERSION}/league"

# Division Endpoints
DIVISIONS_ENDPOINT = f"{MLB_API_BASE}{MLB_API_VERSION}/divisions"

# Venue Endpoints
VENUES_ENDPOINT = f"{MLB_API_BASE}{MLB_API_VERSION}/venues"