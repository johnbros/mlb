from datetime import datetime
import pytz

# You can store this dict somewhere global or in a helper module
TEAM_TIMEZONES = {
    'ANA': 'America/Los_Angeles',
    'ARI': 'America/Phoenix',
    'ATL': 'America/New_York',
    'BAL': 'America/New_York',
    'BOS': 'America/New_York',
    'CHA': 'America/Chicago',
    'CHN': 'America/Chicago',
    'CIN': 'America/New_York',
    'CLE': 'America/New_York',
    'COL': 'America/Denver',
    'DET': 'America/New_York',
    'HOU': 'America/Chicago',
    'KCA': 'America/Chicago',
    'LAN': 'America/Los_Angeles',
    'MIA': 'America/New_York',
    'MIL': 'America/Chicago',
    'MIN': 'America/Chicago',
    'NYA': 'America/New_York',
    'NYN': 'America/New_York',
    'OAK': 'America/Los_Angeles',
    'PHI': 'America/New_York',
    'PIT': 'America/New_York',
    'SDN': 'America/Los_Angeles',
    'SEA': 'America/Los_Angeles',
    'SFN': 'America/Los_Angeles',
    'SLN': 'America/Chicago',
    'TBA': 'America/New_York',
    'TEX': 'America/Chicago',
    'TOR': 'America/Toronto',
    'WAS': 'America/New_York',
    'SAN02': 'America/Los_Angeles',
    'ANA01': 'America/Los_Angeles',
    'PHO01': 'America/Phoenix',
    'CHI12': 'America/Chicago',
    'BOS07': 'America/New_York',
    'ATL02': 'America/New_York',
    'BAL12': 'America/New_York',
    'CHI11': 'America/Chicago',
    'WAS10': 'America/New_York',
    'CLE08': 'America/New_York',
    'CIN09': 'America/New_York',
    'DEN02': 'America/Denver',
    'DET05': 'America/New_York',
    'MIA01': 'America/New_York',
    'HOU03': 'America/Chicago',
    'KAN06': 'America/Chicago',
    'LOS03': 'America/Los_Angeles',
    'MIL06': 'America/Chicago',
    'MIN03': 'America/Chicago',
    'NYC16': 'America/New_York',
    'NYC17': 'America/New_York',
    'NYC21': 'America/New_York',
    'OAK01': 'America/Los_Angeles',
    'PHI13': 'America/New_York',
    'PIT08': 'America/New_York',
    'SEA03': 'America/Los_Angeles',
    'SFO03': 'America/Los_Angeles',
    'STL09': 'America/Chicago',
    'STP01': 'America/New_York',
    'ARL02': 'America/Chicago',
    'TOR02': 'America/Toronto',
    'WAS11': 'America/New_York',

}

def convert_to_utc(date_str, time_str, home_team_abbr):
    # e.g., "2015/04/10", "7:12PM", "ANA"
    local_tz_str = TEAM_TIMEZONES.get(home_team_abbr, 'America/New_York')
    local_tz = pytz.timezone(local_tz_str)

    naive_local = datetime.strptime(f"{date_str} {time_str}", "%Y/%m/%d %I:%M%p")
    localized = local_tz.localize(naive_local)
    utc = localized.astimezone(pytz.utc)

    return utc.replace(tzinfo=None)
