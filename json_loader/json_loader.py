import json
import psycopg
import os

# Before we read files we need the league and season names that we want.
league_names_array = ['La Liga', 'Premier League']
season_name_array = ['2020/2021', '2019/2020', '2018/2019', '2003/2004']

# Learn what files we want to read in matches.
competition_id_array = []

# Learn the match id's in matches.
match_id_array = []

def load_competitions(cursor):

    current_directory = os.getcwd()

    file_path = os.path.join(current_directory, "data", "competitions.json")

    with open(file_path, 'r') as file:
        json_file = json.load(file)

        for json_entry in json_file:

            # Only add things to the table that have the same league name
            for league_name in league_names_array:

                if (json_entry['competition_name'] == league_name and json_entry['season_name'] in season_name_array):
                    competition_id = json_entry['competition_id']
                    season_id = json_entry['season_id']
                    country_name = json_entry['country_name']
                    competition_name = json_entry['competition_name']
                    competition_gender = json_entry['competition_gender']
                    competition_youth = json_entry['competition_youth']
                    competition_international = json_entry['competition_international']
                    season_name = json_entry['season_name']

                    # INSERT INTO SEASON TABLE
                    sql_insert_into_season_table = f"INSERT INTO season (season_id, season_name) VALUES ({season_id}, '{season_name}') ON CONFLICT (season_id) DO NOTHING;"
                    cursor.execute(sql_insert_into_season_table)

                    # INSERT INTO COMPETITION TABLE
                    sql_insert_into_competition_table = f"INSERT INTO competition (competition_id, season_id, competition_name, competition_gender, country_name, youth, international) VALUES ({competition_id}, {season_id}, '{competition_name}', '{competition_gender}', '{country_name}', {competition_youth}, {competition_international});"
                    cursor.execute(sql_insert_into_competition_table)
                    
                    if (competition_id not in competition_id_array):
                        competition_id_array.append(competition_id)

    competition_id_array.sort()
    print(competition_id_array)

def load_matches(cursor):

    # Every file is read under matches/{competitonID}

    print("Reading matches folder please wait...")

    for competition_index in competition_id_array:

        current_directory = os.getcwd()

        file_path = os.path.join(current_directory, "data", "matches", str(competition_index))

        for file_name in os.listdir(file_path):

            full_path = os.path.join(file_path, file_name)
            
            # utf-8 is needed as some jsons crash if its not provided
            with open(full_path, 'r', encoding='utf-8') as file:
                
                json_file = json.load(file)

                for json_entry in json_file:

                    if (json_entry['competition']['competition_id'] in competition_id_array and json_entry['season']['season_name'] in season_name_array):

                        match_id = json_entry['match_id']
                        match_date = json_entry['match_date']
                        kick_off = json_entry['kick_off']
                        match_week = json_entry['match_week']
                        
                        competition_id = json_entry['competition']['competition_id']
                        competition_country_name = json_entry['competition']['country_name']            # not used because already read in load_competition
                        competition_competition_name = json_entry['competition']['competition_name']    # not used because already read in load_competition

                        season_id = json_entry['season']['season_id']
                        season_name = json_entry['season']['season_name']

                        # INSERT INTO SEASON TABLE
                        sql_insert_season_table = f"INSERT INTO season (season_id, season_name) VALUES ({season_id}, {season_name}) ON CONFLICT (season_id) DO NOTHING;"
                        cursor.execute(sql_insert_season_table)

                        home_team_id = json_entry['home_team']['home_team_id']
                        home_team_name = json_entry['home_team']['home_team_name']
                        home_team_gender = json_entry['home_team']['home_team_gender']
                        home_team_group = json_entry['home_team']['home_team_group']
                        home_team_country_id = json_entry['home_team']['country']['id']
                        home_team_country_name = json_entry['home_team']['country']['name']

                        # Note: There is only one manager per match.

                        home_team_manager_id = "NULL"
                        home_team_manager_name = "NULL"
                        home_team_manager_nickname = "NULL"
                        home_team_manager_dob = "NULL"
                        home_team_manager_country_id = "NULL"
                        home_team_manager_country_name = "NULL"

                        if 'managers' in json_entry['home_team']:
                            home_team_manager_id = json_entry['home_team']['managers'][0]['id']
                            home_team_manager_name = json_entry['home_team']['managers'][0]['name']
                            home_team_manager_nickname = json_entry['home_team']['managers'][0]['nickname']
                            home_team_manager_dob = json_entry['home_team']['managers'][0]['dob']
                            home_team_manager_country_id = json_entry['home_team']['managers'][0]['country']['id']
                            home_team_manager_country_name = json_entry['home_team']['managers'][0]['country']['name']
                        home_score = json_entry['home_score']

                        away_team_id = json_entry['away_team']['away_team_id']
                        away_team_name = json_entry['away_team']['away_team_name']
                        away_team_gender = json_entry['away_team']['away_team_gender']
                        away_team_group = json_entry['away_team']['away_team_group']
                        away_team_country_id = json_entry['away_team']['country']['id']
                        away_team_country_name = json_entry['away_team']['country']['name']

                        away_team_manager_id = "NULL"
                        away_team_manager_name = "NULL"
                        away_team_manager_nickname = "NULL"
                        away_team_manager_dob = "NULL"
                        away_team_manager_country_id = "NULL"
                        away_team_manager_country_name = "NULL"

                        if 'managers' in json_entry['away_team']:
                            away_team_manager_id = json_entry['away_team']['managers'][0]['id']
                            away_team_manager_name = json_entry['away_team']['managers'][0]['name']
                            away_team_manager_nickname = json_entry['away_team']['managers'][0]['nickname']
                            away_team_manager_dob = json_entry['away_team']['managers'][0]['dob']
                            away_team_manager_country_id = json_entry['away_team']['managers'][0]['country']['id']
                            away_team_manager_country_name = json_entry['away_team']['managers'][0]['country']['name']
                        away_score = json_entry['away_score']

                        competition_stage_id = json_entry['competition_stage']['id']
                        competition_stage_name = json_entry['competition_stage']['name']

                        stadium_id = "NULL"
                        stadium_name = "NULL"
                        stadium_country_id = "NULL"
                        stadium_country_name = "NULL"

                        if 'stadium' in json_entry:
                            stadium_id = json_entry['stadium']['id']
                            stadium_name = json_entry['stadium']['name']
                            stadium_country_id = json_entry['stadium']['country']['id']
                            stadium_country_name = json_entry['stadium']['country']['name']

                        referee_id = "NULL"
                        referee_name= "NULL"
                        referee_country_id = "NULL"
                        referee_country_name = "NULL"
                
                        if 'referee' in json_entry:
                            referee_id = json_entry['referee']['id']
                            referee_name= json_entry['referee']['name']
                            referee_country_id = json_entry['referee']['country']['id']
                            referee_country_name = json_entry['referee']['country']['name']
                            
                        # INSERT INTO COUNTRY TABLE
                            
                        # -- HOME_TEAM
                        sql_insert_into_country_table = f"INSERT INTO country (country_id, country_name) VALUES ({home_team_country_id}, '{home_team_country_name}') ON CONFLICT (country_id) DO NOTHING;"
                        cursor.execute(sql_insert_into_country_table)

                        # -- AWAY_TEAM
                        sql_insert_into_country_table = f"INSERT INTO country (country_id, country_name) VALUES ({away_team_country_id}, '{away_team_country_name}') ON CONFLICT (country_id) DO NOTHING;"
                        cursor.execute(sql_insert_into_country_table)

                        if (home_team_manager_country_id != "NULL" and home_team_manager_country_name != "NULL"):
                            # -- HOME_TEAM_MANAGER
                            sql_insert_into_country_table = f"INSERT INTO country (country_id, country_name) VALUES ({home_team_manager_country_id}, '{home_team_manager_country_name}') ON CONFLICT (country_id) DO NOTHING;"
                            cursor.execute(sql_insert_into_country_table)

                        if (away_team_manager_country_id != "NULL" and away_team_manager_country_name != "NULL"):
                            # -- AWAY_TEAM_MANAGER
                            sql_insert_into_country_table = f"INSERT INTO country (country_id, country_name) VALUES ({away_team_manager_country_id}, '{away_team_manager_country_name}') ON CONFLICT (country_id) DO NOTHING;"
                            cursor.execute(sql_insert_into_country_table)

                        # -- STADIUM
                        if (stadium_country_id != "NULL"):
                            sql_insert_into_country_table = f"INSERT INTO country (country_id, country_name) VALUES ({stadium_country_id}, '{stadium_country_name}') ON CONFLICT (country_id) DO NOTHING;"
                            cursor.execute(sql_insert_into_country_table)

                        # -- REFEREE
                        if (referee_country_id != "NULL" and referee_country_name != "NULL"):
                            sql_insert_into_country_table = f"INSERT INTO country (country_id, country_name) VALUES ({referee_country_id}, '{referee_country_name}') ON CONFLICT (country_id) DO NOTHING;"
                            cursor.execute(sql_insert_into_country_table)

                        # INSERT INTO STADIUM TABLE
                        if (stadium_country_id != "NULL"):
                            sql_insert_into_stadium_table = f"INSERT INTO stadium (stadium_id, stadium_name, stadium_country_id) VALUES ({stadium_id}, '{stadium_name}', {stadium_country_id}) ON CONFLICT (stadium_id) DO NOTHING;"
                            cursor.execute(sql_insert_into_stadium_table)
                        
                        # INSERT INTO REFEREE TABLE
                        if (referee_id != "NULL" and referee_name != "NULL" and referee_country_id != "NULL"):
                            sql_insert_referee_table = f"INSERT INTO referee (referee_id, referee_name, referee_country_id) VALUES ({referee_id}, '{referee_name}', {referee_country_id}) ON CONFLICT (referee_id) DO NOTHING;"
                            cursor.execute(sql_insert_referee_table)

                        # INSERT INTO MANAGER TABLE
                        if (home_team_manager_id != "NULL"):
                            # -- INSERT HOME_TEAM_MANAGER
                            sql_insert_into_manager_table = f"INSERT INTO manager (manager_id, manager_name, manager_nickname, manager_dob, manager_country_id) VALUES ({home_team_manager_id}, '{home_team_manager_name}', '{home_team_manager_nickname}', '{home_team_manager_dob}', {home_team_manager_country_id}) ON CONFLICT (manager_id) DO NOTHING;"
                            cursor.execute(sql_insert_into_manager_table)
                
                        # -- INSERT AWAY_TEAM_MANAGER
                        if (away_team_manager_id != "NULL"):
                            sql_insert_into_manager_table = f"INSERT INTO manager (manager_id, manager_name, manager_nickname, manager_dob, manager_country_id) VALUES ({away_team_manager_id}, '{away_team_manager_name}', '{away_team_manager_nickname}', '{away_team_manager_dob}', {away_team_manager_country_id}) ON CONFLICT (manager_id) DO NOTHING;"
                            cursor.execute(sql_insert_into_manager_table)

                        # INSERT INTO MATCH TABLE
                        sql_insert_into_match_table = f"INSERT INTO match (match_id, match_date, kick_off, match_week, competition_id, season_id, home_team_id, home_team_name, home_team_gender, home_team_group, home_team_country_id, home_team_manager_id, away_team_id, away_team_name, away_team_gender, away_team_group, away_team_country_id, away_team_manager_id, home_score, away_score, competition_stage_id, competition_stage_name, referee_id, stadium_id) VALUES ({match_id}, '{match_date}', '{kick_off}', {match_week}, {competition_id}, {season_id}, {home_team_id}, '{home_team_name}', '{home_team_gender}', '{home_team_group}', {home_team_country_id}, {home_team_manager_id}, {away_team_id}, '{away_team_name}', '{away_team_gender}', '{away_team_group}', {away_team_country_id}, {away_team_manager_id}, {home_score}, {away_score}, {competition_stage_id}, '{competition_stage_name}', {referee_id}, {stadium_id});"
                        cursor.execute(sql_insert_into_match_table)

                        # INSERT INTO TEAM TABLE

                        # -- TEAM_HOME_TEAM
                        sql_insert_into_lineup_table = f"INSERT INTO team (team_id, team_name, match_id) VALUES ({home_team_id}, '{home_team_name}', '{match_id}') ON CONFLICT (team_id) DO NOTHING;"
                        cursor.execute(sql_insert_into_lineup_table)

                        # -- TEAM_AWAY_TEAM
                        sql_insert_into_lineup_table = f"INSERT INTO team (team_id, team_name, match_id) VALUES ({away_team_id}, '{away_team_name}', '{match_id}') ON CONFLICT (team_id) DO NOTHING;"
                        cursor.execute(sql_insert_into_lineup_table)

                        if (match_id not in match_id_array):
                            match_id_array.append(match_id)

    match_id_array.sort() 
    print("Matches folder read finished!")
    print(match_id_array)
    
def load_lineups(cursor):

    print("Reading lineups folder please wait...")

    cursor.execute("BEGIN TRANSACTION;")

    for match_index in match_id_array:

        current_directory = os.getcwd()

        file_name = str(match_index) + ".json"

        file_path = os.path.join(current_directory, "data", "lineups", file_name)

        print(file_path)

        # utf-8 is needed as some jsons crash if its not provided
        with open(file_path, 'r', encoding='utf-8') as file:

            json_file = json.load(file)

            for json_entry in json_file:

                team_id = json_entry['team_id'] 
                team_name = json_entry['team_name']     # Not needed as team table is done in load_matches

                for player in json_entry['lineup']:

                    player_id = player['player_id']
                    player_name = player['player_name']
                    player_nickname = player['player_nickname']
                    player_jersey_number = player['jersey_number']

                    player_country_id = None
                    player_country_name = None

                    if 'country' in player:
                        player_country_id = player['country']['id']
                        player_country_name = player['country']['name']

                        # We need to escape the ' character so postgres accepts it.
                        if player_country_name is not None:
                            if "'" in player_country_name:
                                player_country_name = player_country_name.replace("'", "''")

                        # INSERT INTO COUNTRY TABLE
                        sql_insert_into_country_table = f"INSERT INTO country (country_id, country_name) VALUES ({player_country_id}, '{player_country_name}') ON CONFLICT (country_id) DO NOTHING;"
                        cursor.execute(sql_insert_into_country_table)

                    for position in player['positions']:
                        position_id = position['position_id']
                        position_name = position['position']
                        position_from = position['from']
                        position_to = position['to']
                        position_from_period = position['from_period']
                        position_to_period = position['to_period']
                        position_start_reason = position['start_reason']
                        position_end_reason = position['end_reason']

                        # INSERT INTO POSITION TABLE
                        if (position_to == None):
                            position_to = "NULL"
                        if (position_to_period == None):
                            position_to_period = "NULL"
                        sql_insert_into_position_table = f"INSERT INTO position (position_id, position, position_from, position_to, from_period, to_period, start_reason, end_reason, match_id) VALUES ({position_id}, '{position_name}', '{position_from}', '{position_to}', {position_from_period}, {position_to_period}, '{position_start_reason}', '{position_end_reason}', {match_index}) ON CONFLICT (position_id, match_id) DO NOTHING;"
                        cursor.execute(sql_insert_into_position_table)

                        # We need to escape the ' character so postgres accepts it.
                        if player_name is not None:
                            if "'" in player_name:
                                player_name = player_name.replace("'", "''")
                        
                        # We need to escape the ' character so postgres accepts it.
                        if player_nickname is not None:
                            if "'" in player_nickname:
                                player_nickname = player_nickname.replace("'", "''")
                        
                        if player_country_id is None:
                            player_country_id = "NULL"

                        sql_insert_into_player_table = f"INSERT INTO player (player_id, team_id, match_id, player_name, player_nickname, jersey_number, country_id, position_id) VALUES ({player_id}, {team_id}, {match_index}, '{player_name}', '{player_nickname}', {player_jersey_number}, {player_country_id}, {position_id}) ON CONFLICT (player_id, match_id) DO NOTHING;"
                        cursor.execute(sql_insert_into_player_table)

                    for card in player['cards']:
                        card_time = card['time']
                        card_type = card['card_type']
                        reason = card['reason']
                        period = card['period']

                        if period is None:
                            period = "NULL"

                        # INSERT INTO CARD TABLE
                        sql_insert_into_card_table = f"INSERT INTO card (player_id, card_time, card_type, reason, period, match_id) VALUES ({player_id}, '{card_time}', '{card_type}', '{reason}', {period}, {match_index});"
                        cursor.execute(sql_insert_into_card_table)

    cursor.execute("END TRANSACTION;")
    print("Lineups folder read finished!")

def load_events(cursor):

    print("Reading events folder please wait...")

    cursor.execute("BEGIN TRANSACTION;")

    for match_index in match_id_array:

        currentDirectory = os.getcwd()

        file_name = str(match_index) + ".json"

        file_path = os.path.join(currentDirectory, "data", "events", file_name)

        print(file_path)

        # utf-8 is needed as some jsons crash if its not provided
        with open(file_path, 'r', encoding='utf-8') as file:

            jsonFile = json.load(file)

            for jsonEntry in jsonFile:

                event_id = jsonEntry['id']
                match_id = match_index
                event_index = jsonEntry['index']
                period = jsonEntry['period']
                timestamp = jsonEntry['timestamp']
                minute = jsonEntry['minute']
                second = jsonEntry['second']
                event_type_id = jsonEntry['type']['id']
                event_name = jsonEntry['type']['name']
                team_id = jsonEntry['team']['id']
                team_name = jsonEntry['team']['name']
                possession = jsonEntry['possession']
                possesion_team_id = jsonEntry['possession_team']['id']
                possession_team_name = jsonEntry['possession_team']['name']
                play_pattern_id = jsonEntry['play_pattern']['id']
                play_pattern_name = jsonEntry['play_pattern']['name']

                player_id = "NULL"
                player_name = "NULL"
                if "player" in jsonEntry:
                    player_id = jsonEntry['player']['id']
                    player_name = jsonEntry['player']['name']

                    # We need to escape the ' character so postgres accepts it.
                    if player_name is not None:
                        if "'" in player_name:
                            player_name = player_name.replace("'", "''")

                position_id = "NULL"
                position_name = "NULL"
                if "position" in jsonEntry:
                    position_id = jsonEntry['position']['id']
                    position_name = jsonEntry['position']['name']

                event_location_x = "NULL"
                event_location_y = "NULL"
                if "location" in jsonEntry:
                    event_location_x = jsonEntry['location'][0]
                    event_location_y = jsonEntry['location'][1]

                duration = "NULL"
                if "duration" in jsonEntry:
                    duration = jsonEntry['duration']

                under_pressure = "NULL"
                if "under_pressure" in jsonEntry:
                    duration = jsonEntry['under_pressure']

                off_camera = "NULL"
                if "off_camera" in jsonEntry:
                    off_camera = jsonEntry['off_camera']

                out = "NULL"
                if "out" in jsonEntry:
                    off_camera = jsonEntry['out']

                if duration == True:
                    duration = 0

                # INSERT INTO EVENT TABLE
                sql_insert_into_event_table = f"INSERT INTO event (event_id, match_id, index, period, timestamp, minute, second, event_type_id, event_name, team_id, team_name, possession, possesion_team_id,	possession_team_name, play_pattern_id, play_pattern_name, player_id, player_name, position_id, position_name, event_location_x, event_location_y, duration, under_pressure, off_camera, out) VALUES ('{event_id}', {match_id}, {event_index}, {period}, '{timestamp}', {minute}, {second}, {event_type_id}, '{event_name}', {team_id}, '{team_name}', '{possession}', {possesion_team_id}, '{possession_team_name}', {play_pattern_id}, '{play_pattern_name}', {player_id}, '{player_name}', {position_id}, '{position_name}', {event_location_x}, {event_location_y}, {duration}, {under_pressure}, {off_camera}, {out});"
                cursor.execute(sql_insert_into_event_table)

                if event_name == '50/50':
                    
                    event_5050_outcome_id = jsonEntry['50_50']['outcome']['id']
                    event_5050_outcome_name = jsonEntry['50_50']['outcome']['name']

                    counterpress = "NULL"
                    if 'counterpress' in jsonEntry:
                        counterpress = jsonEntry['counterpress']

                    # INSERT INTO EVENT50-50 TABLE
                    sql_insert_into_5050_table = f"INSERT INTO event_5050 (event_id, event_5050_outcome_id, event_5050_outcome_name, counterpress) VALUES ('{event_id}', {event_5050_outcome_id}, '{event_5050_outcome_name}', {counterpress});"
                    cursor.execute(sql_insert_into_5050_table)
                
                if event_name == 'Bad Behaviour':
                    
                    bad_behaviour_card_id = jsonEntry['bad_behaviour']['card']['id']
                    bad_behaviour_card_name = jsonEntry['bad_behaviour']['card']['name']

                    # INSERT INTO EVENTBADBEHAVIOUR TABLE
                    sql_insert_into_bad_behaviour_table = f"INSERT INTO event_bad_behaviour (event_id, bad_behaviour_card_id, bad_behaviour_card_name) VALUES ('{event_id}', {bad_behaviour_card_id}, '{bad_behaviour_card_name}');"
                    cursor.execute(sql_insert_into_bad_behaviour_table)
 
                if event_name == 'Ball Receipt':

                    ball_receipt_id = jsonEntry['ball_receipt']['outcome']['id']
                    ball_receipt_name = jsonEntry['ball_receipt']['outcome']['name']

                    # INSERT INTO EVENTBALLRECEIPT TABLE
                    sql_insert_into_ball_receipt_table = f"INSERT INTO event_ball_receipt (event_id, ball_receipt_id, ball_receipt_name) VALUES ({event_id}, {ball_receipt_id}, {ball_receipt_name});"
                    cursor.execute(sql_insert_into_ball_receipt_table)       
        
                if event_name == 'Ball Recovery':

                    ball_recovery_offensive = "NULL"
                    ball_recovery_failure = "NULL"

                    if 'ball_recovery' in jsonEntry:

                        if 'recovery_offensive' in jsonEntry['ball_recovery']:
                            ball_recovery_offensive = jsonEntry['ball_recovery']['recovery_offensive']

                        if 'recovery_failure' in jsonEntry['ball_recovery']:
                            ball_recovery_failure = jsonEntry['ball_recovery']['recovery_failure']

                    # INSERT INTO EVENTBALLRECOVERY TABLE
                    sql_insert_into_ball_recovery_table = f"INSERT INTO event_ball_recovery(event_id, ball_recovery_offensive, ball_recovery_failure) VALUES ('{event_id}', {ball_recovery_offensive}, {ball_recovery_failure});"
                    cursor.execute(sql_insert_into_ball_recovery_table)

                if event_name == 'Block':

                    deflection = "NULL"
                    offensive = "NULL"
                    save_block = "NULL"
                    counterpress = "NULL"

                    if "block" in jsonEntry:

                        if "deflection" in jsonEntry['block']:
                            deflection = jsonEntry['block']['deflection']
                        
                        if "offensive" in jsonEntry['block']:
                            offensive = jsonEntry['block']['offensive']

                        if "save_block" in jsonEntry['block']:
                            save_block = jsonEntry['block']['save_block']

                    if "counterpress" in jsonEntry:
                        counterpress = jsonEntry['counterpress']

                    # INSERT INTO EVENTBLOCK TABLE
                    sql_insert_into_block_table = f"INSERT INTO event_block(event_id, deflection, offensive, save_block, counterpress) VALUES ('{event_id}', {deflection}, {offensive}, {save_block}, {counterpress});"
                    cursor.execute(sql_insert_into_block_table)
                
                if event_name == 'Carry':

                    carry_end_location_x = jsonEntry['carry']['end_location'][0]
                    carry_end_location_y = jsonEntry['carry']['end_location'][1]

                    # INSERT INTO EVENTCARRY TABLE
                    sql_insert_into_carry_table = f"INSERT INTO event_carry(event_id, carry_end_location_x, carry_end_location_y) VALUES ('{event_id}', {carry_end_location_x}, {carry_end_location_y});"
                    cursor.execute(sql_insert_into_carry_table)
                    
                if event_name == 'Clearance':

                    aerial_won = "NULL"
                    body_part_id = "NULL"
                    body_part_name = "NULL"

                    if 'clearance' in jsonEntry:
                        
                        if 'aerial_won' in jsonEntry['clearance']:
                            aerial_won = jsonEntry['clearance']['aerial_won']

                        if 'body_part' in jsonEntry['clearance']:
                            body_part_id = jsonEntry['clearance']['body_part']['id']
                            body_part_name = jsonEntry['clearance']['body_part']['name']

                    # INSERT INTO EVENTCLEARANCE TABLE
                    sql_insert_into_clearance_table = f"INSERT INTO event_clearance(event_id, aerial_won, body_part_id, body_part_name) VALUES ('{event_id}', {aerial_won}, {body_part_id}, '{body_part_name}');"
                    cursor.execute(sql_insert_into_clearance_table)
                
                if event_name == 'Dribble':

                    event_dribble_outcome_id = jsonEntry['dribble']['outcome']['id']
                    event_dribble_outcome_name = jsonEntry['dribble']['outcome']['name']

                    overrun = "NULL"
                    nutmeg = "NULL"
                    no_touch = "NULL"
                    if 'overrun' in jsonEntry['dribble']:
                        overrun = jsonEntry['dribble']['overrun']
                    if 'nutmeg' in jsonEntry['dribble']:
                        nutmeg = jsonEntry['dribble']['nutmeg']
                    if 'no_touch' in jsonEntry['dribble']:
                        no_touch = jsonEntry['dribble']['no_touch']

                    # INSERT INTO EVENTDRIBBLE TABLE
                    sql_insert_into_dribble_table = f"INSERT INTO event_dribble(event_id, dribble_outcome_id, dribble_outcome_name, overrun, nutmeg, no_touch) VALUES ('{event_id}', {event_dribble_outcome_id}, '{event_dribble_outcome_name}', {overrun}, {nutmeg}, {no_touch});"
                    cursor.execute(sql_insert_into_dribble_table)
                    

                if event_name == 'Dribbled Past':
                    
                    counterpress = "NULL"
                    if counterpress in jsonEntry:
                        counterpress = jsonEntry['counterpress']

                    # INSERT INTO EVENTDRIBBLEDPAST TABLE
                    sql_insert_into_dribbled_past_table = f"INSERT INTO event_dribbled_past(event_id, counterpress) VALUES ('{event_id}', {counterpress});"
                    cursor.execute(sql_insert_into_dribbled_past_table)
                    
                
                if event_name == 'Duel':
                    
                    counterpress = "NULL"
                    if "counterpress" in jsonEntry:
                        counterpress = jsonEntry['counterpress']

                    duel_type_id = "NULL"
                    duel_type_name = "NULL"
                    duel_outcome_id = "NULL"
                    duel_outcome_name = "NULL"

                    if 'duel' in jsonEntry:

                        if 'type' in jsonEntry['duel']:
                            duel_type_id = jsonEntry['duel']['type']['id']
                            duel_type_name = jsonEntry['duel']['type']['name']

                        if 'outcome' in jsonEntry['duel']:
                            duel_outcome_id = jsonEntry['duel']['outcome']['id']
                            duel_outcome_name = jsonEntry['duel']['outcome']['name']

                    # INSERT INTO DUEL TABLE
                    sql_insert_into_duel_table = f"INSERT INTO event_duel(event_id, counterpress, duel_type_id, duel_type_name, duel_outcome_id, duel_outcome_name) VALUES ('{event_id}', {counterpress}, {duel_type_id}, '{duel_type_name}', {duel_outcome_id}, '{duel_outcome_name}');"
                    cursor.execute(sql_insert_into_duel_table)
                
                if event_name == 'Foul Committed':

                    counterpress = "NULL"
                    if "counterpress" in jsonEntry:
                        counterpress = jsonEntry['counterpress']
                    
                    offensive = "NULL"
                    advantage = "NULL"
                    penalty = "NULL"
                    card_id = "NULL"
                    card_name = "NULL"
                    if 'foul_commited' in jsonEntry:

                        if 'offensive' in jsonEntry['foul_commited']:
                            offensive = jsonEntry['foul_commited']['offensive']
                        
                        if 'advantage' in jsonEntry['foul_commited']:
                            advantage = jsonEntry['foul_commited']['advantage']
                        
                        if 'penalty' in jsonEntry['foul_commited']:
                            penalty = jsonEntry['foul_commited']['penalty']

                        if 'card' in jsonEntry['foul_commited']:
                            card_id = jsonEntry['foul_commited']['card']['id']
                            card_name = jsonEntry['foul_commited']['card']['name']

                    # INSERT INTO FOUL COMMITED TABLE
                    sql_insert_into_foul_commited_table = f"INSERT INTO event_foul_commited(event_id, counterpress, offensive, advantage, penalty, card_id, card_name) VALUES ('{event_id}', {counterpress}, {offensive}, {advantage}, {penalty}, {card_id}, {card_name});"
                    cursor.execute(sql_insert_into_foul_commited_table)
                
                if event_name == 'Foul Won':

                    defensive = "NULL"
                    advantage = "NULL"
                    penalty = "NULL"
                    if "foul_won" in jsonEntry:

                        if "defensive" in jsonEntry['foul_won']:
                            defensive = jsonEntry['foul_won']['defensive']
                        
                        if "advantage" in jsonEntry['foul_won']:
                            advantage = jsonEntry['foul_won']['advantage']

                        if "penalty" in jsonEntry['foul_won']:
                            penalty = jsonEntry['foul_won']['penalty']

                    # INSERT INTO FOUL WON TABLE    
                    sql_insert_into_foul_won_table = f"INSERT INTO event_foul_won(event_id, defensive, advantage, penalty) VALUES ('{event_id}', {defensive}, {advantage}, {penalty});"
                    cursor.execute(sql_insert_into_foul_won_table)
                
                if event_name == 'Goal Keeper':
                    goalkeeper_position_id = "NULL"
                    goalkeeper_position_name = "NULL"
                    goalkeeper_technique_id = "NULL"
                    goalkeeper_technique_name = "NULL"
                    goalkeeper_body_part_id = "NULL"
                    goalkeeper_body_part_name = "NULL"
                    goalkeeper_type_id = "NULL"
                    goalkeeper_type_name = "NULL"
                    goalkeeper_outcome_id = "NULL"
                    goalkeeper_outcome_name = "NULL"
                    if jsonEntry['goalkeeper']:

                        if 'position' in jsonEntry['goalkeeper']:
                            goalkeeper_position_id = jsonEntry['goalkeeper']['position']['id']
                            goalkeeper_position_name = jsonEntry['goalkeeper']['position']['name']

                        if 'technique' in jsonEntry['goalkeeper']:
                            goalkeeper_technique_id = jsonEntry['goalkeeper']['technique']['id']
                            goalkeeper_technique_name = jsonEntry['goalkeeper']['technique']['name']
                    
                        if 'body_part' in jsonEntry['goalkeeper']:
                            goalkeeper_body_part_id = jsonEntry['goalkeeper']['body_part']['id']
                            goalkeeper_body_part_name = jsonEntry['goalkeeper']['body_part']['name']

                        if 'type' in jsonEntry['goalkeeper']:
                            goalkeeper_type_id = jsonEntry['goalkeeper']['type']['id']
                            goalkeeper_type_name = jsonEntry['goalkeeper']['type']['name']

                        if 'outcome' in jsonEntry['goalkeeper']:  
                            goalkeeper_outcome_id = jsonEntry['goalkeeper']['outcome']['id']
                            goalkeeper_outcome_name = jsonEntry['goalkeeper']['outcome']['name']

                    # INSERT INTO GOALKEEPER TABLE
                    sql_insert_into_goalkeeper_table = f"INSERT INTO event_goalkeeper(event_id, goalkeeper_position_id, goalkeeper_position_name, goalkeeper_technique_id, goalkeeper_technique_name, goalkeeper_body_part_id, goalkeeper_body_part_name, goalkeeper_type_id, goalkeeper_type_name, goalkeeper_type_outcome_id, goalkeeper_outcome_name) VALUES ('{event_id}', {goalkeeper_position_id}, '{goalkeeper_position_name}', {goalkeeper_technique_id}, '{goalkeeper_technique_name}', {goalkeeper_body_part_id}, '{goalkeeper_body_part_name}', {goalkeeper_type_id}, '{goalkeeper_type_name}', {goalkeeper_outcome_id}, '{goalkeeper_outcome_name}');"
                    cursor.execute(sql_insert_into_goalkeeper_table)
                
                if event_name == 'Half End':
                    match_suspended = "NULL"
                    early_video_end = "NULL"

                    if "half_end" in jsonEntry:

                        if "suspended" in jsonEntry['half_end']:
                            match_suspended = jsonEntry['half_end']['suspended']

                        if "early_video_end" in jsonEntry['half_end']:
                            early_video_end = jsonEntry['half_end']['early_video_end']

                    # INSERT INTO HALF END TABLE
                    sql_insert_into_half_end_table = f"INSERT into event_half_end(event_id, early_video_end, match_suspended) VALUES ('{event_id}', {early_video_end}, {match_suspended});"
                    cursor.execute(sql_insert_into_half_end_table)

            
                if event_name == 'Half Start':
                
                    late_video_start = "NULL"

                    if "half_start" in jsonEntry:

                        if "late_video_start" in jsonEntry['half_start']:
                            late_video_start = jsonEntry['half_start']['late_video_start']

                    
                    # INSERT INTO HALF START TABLE
                    sql_insert_into_half_start_table = f"INSERT into event_half_start(event_id, late_video_start) VALUES ('{event_id}', {late_video_start});"
                    cursor.execute(sql_insert_into_half_start_table)

                if event_name == 'Injury Stoppage':

                    in_chain = "NULL"
                    if "injury_stoppage" in jsonEntry:
                        in_chain = jsonEntry['injury_stoppage']['in_chain']                    

                    # INSERT INTO INJURY STOPPAGE TABLE
                    sql_insert_into_injury_stoppage_table = f"INSERT into event_injury_stoppage(event_id, in_chain) VALUES ('{event_id}', {in_chain});"
                    cursor.execute(sql_insert_into_injury_stoppage_table)
                
                if event_name == 'Interception':
                    outcome_id = jsonEntry['interception']['outcome']['id']
                    outcome_name = jsonEntry['interception']['outcome']['name']
                    
                    # INSERT INTO INTERCEPTION TABLE
                    sql_insert_into_interception_table = f"INSERT into event_interception(event_id, outcome_id, outcome_name) VALUES ('{event_id}', {outcome_id}, '{outcome_name}');"
                    cursor.execute(sql_insert_into_interception_table)
                
                if event_name == 'Miscontrol':
                    aerial_won = "NULL"
                    if "miscontrol" in jsonEntry:
                        aerial_won = jsonEntry['miscontrol']['aerial_won']

                    # INSERT INTO MISCONTROL TABLE
                    sql_insert_into_miscontrol_table = f"INSERT into event_miscontrol(event_id, aerial_won) VALUES ('{event_id}', {aerial_won});"
                    cursor.execute(sql_insert_into_miscontrol_table)

                
                if event_name == 'Pass':
                    pass_length = jsonEntry['pass']['length']
                    pass_angle = jsonEntry['pass']['angle']
                    pass_height_id = jsonEntry['pass']['height']['id']
                    pass_height_name = jsonEntry['pass']['height']['name']
                    pass_end_location_x = jsonEntry['pass']['end_location'][0]
                    pass_end_location_y = jsonEntry['pass']['end_location'][1]

                    pass_outcome_id = "NULL"
                    pass_outcome_name = "NULL"
                    if "outcome" in jsonEntry['pass']:
                        pass_outcome_id = jsonEntry['pass']['outcome']['id']
                        pass_outcome_name = jsonEntry['pass']['outcome']['name']

                    pass_body_part_id = "NULL"
                    pass_body_part_name = "NULL"
                    if "body_part" in jsonEntry['pass']:
                        pass_body_part_id = jsonEntry['pass']['body_part']['id']
                        pass_body_part_name = jsonEntry['pass']['body_part']['name']

                    pass_recipient_id = "NULL"
                    pass_recipient_name = "NULL"
                    if "recipient" in jsonEntry['pass']:
                        pass_recipient_id = jsonEntry['pass']['recipient']['id']
                        pass_recipient_name = jsonEntry['pass']['recipient']['name']

                        # We need to escape the ' character so postgres accepts it.
                        if pass_recipient_name is not None:
                            if "'" in pass_recipient_name:
                                pass_recipient_name = pass_recipient_name.replace("'", "''")

                    pass_assisted_shot_id = "NULL"
                    if "assisted_shot_id" in jsonEntry['pass']:
                        pass_assisted_shot_id = jsonEntry['pass']['assisted_shot_id']

                    pass_backheel = "NULL"
                    if "backheel" in jsonEntry['pass']:
                        pass_backheel = jsonEntry['pass']['backheel']

                    pass_deflected = "NULL"
                    if "deflected" in jsonEntry['pass']:
                        pass_cross = jsonEntry['pass']['deflected']


                    pass_miscommunication = "NULL"
                    if "miscommunication" in jsonEntry['pass']:
                        pass_cross = jsonEntry['pass']['miscommunication']

                    pass_cross = "NULL"
                    if "cross" in jsonEntry['pass']:
                        pass_cross = jsonEntry['pass']['cross']

                    pass_cut_back = "NULL"
                    if "cut_back" in jsonEntry['pass']:
                        pass_cross = jsonEntry['pass']['cut_back']


                    pass_switch = "NULL"
                    if "switch" in jsonEntry['pass']:
                        pass_switch = jsonEntry['pass']['switch']

                    pass_shot_assist = "NULL"
                    if "shot_assist" in jsonEntry['pass']:
                        pass_shot_assist = jsonEntry['pass']['shot_assist']

                    pass_type_id = "NULL"
                    pass_type_name = "NULL"
                    if "type" in jsonEntry['pass']:
                        pass_type_id = jsonEntry['pass']['type']['id']
                        pass_type_name = jsonEntry['pass']['type']['name']

                    pass_technique_id = "NULL"
                    pass_technique_name = "NULL"
                    if "technique" in jsonEntry['pass']:
                        pass_technique_id = jsonEntry['pass']['technique']['id']
                        pass_technique_name = jsonEntry['pass']['technique']['name']

                    # INSERT INTO PASS TABLE
                    if pass_assisted_shot_id != "NULL":
                        sql_insert_into_pass_table = f"INSERT into event_pass(event_id,	pass_recipient_id, pass_recipient_name, pass_length, pass_angle, pass_height_id, pass_height_name, pass_end_location_x, pass_end_location_y, pass_assisted_shot_id, pass_backheel, pass_deflected, pass_miscommunication, pass_cross, pass_cut_back, pass_switch, pass_shot_assist, pass_body_part_id, pass_body_part_name, pass_type_id, pass_type_name, pass_outcome_id, pass_outcome_name, pass_technique_id, pass_technique_name) VALUES ('{event_id}', {pass_recipient_id}, '{pass_recipient_name}', {pass_length}, {pass_angle}, {pass_height_id}, '{pass_height_name}', {pass_end_location_x}, {pass_end_location_y}, '{pass_assisted_shot_id}', {pass_backheel}, {pass_deflected}, {pass_miscommunication}, {pass_cross}, {pass_cut_back}, {pass_switch}, {pass_shot_assist}, {pass_body_part_id}, '{pass_body_part_name}', {pass_type_id}, '{pass_type_name}', {pass_outcome_id}, '{pass_outcome_name}', {pass_technique_id}, '{pass_technique_name}');"
                        cursor.execute(sql_insert_into_pass_table)
                    else:
                        sql_insert_into_pass_table = f"INSERT into event_pass(event_id,	pass_recipient_id, pass_recipient_name, pass_length, pass_angle, pass_height_id, pass_height_name, pass_end_location_x, pass_end_location_y, pass_assisted_shot_id, pass_backheel, pass_deflected, pass_miscommunication, pass_cross, pass_cut_back, pass_switch, pass_shot_assist, pass_body_part_id, pass_body_part_name, pass_type_id, pass_type_name, pass_outcome_id, pass_outcome_name, pass_technique_id, pass_technique_name) VALUES ('{event_id}', {pass_recipient_id}, '{pass_recipient_name}', {pass_length}, {pass_angle}, {pass_height_id}, '{pass_height_name}', {pass_end_location_x}, {pass_end_location_y}, {pass_assisted_shot_id}, {pass_backheel}, {pass_deflected}, {pass_miscommunication}, {pass_cross}, {pass_cut_back}, {pass_switch}, {pass_shot_assist}, {pass_body_part_id}, '{pass_body_part_name}', {pass_type_id}, '{pass_type_name}', {pass_outcome_id}, '{pass_outcome_name}', {pass_technique_id}, '{pass_technique_name}');"
                        cursor.execute(sql_insert_into_pass_table)
                
                if event_name == 'Player Off':
                    permanent = "NULL"

                    if "player_off" in jsonEntry:
                        permanent = jsonEntry['player_off']['permanent']

                    sql_insert_into_player_off_table = f"INSERT into event_player_off(event_id, permanent) VALUES ('{event_id}', {permanent});"
                    cursor.execute(sql_insert_into_player_off_table)
                    
                
                if event_name == 'Shot':

                    key_pass_id = "NULL"
                    if 'key_pass_id' in jsonEntry['shot']:
                        key_pass_id = jsonEntry['shot']['key_pass_id']

                    shot_stats_bomb_xg = jsonEntry['shot']['statsbomb_xg']
                    shot_end_location_x = jsonEntry['shot']['end_location'][0]
                    shot_end_location_y = jsonEntry['shot']['end_location'][1]
                    shot_end_location_z = "NULL"

                    if len(jsonEntry['shot']) == 3:
                        shot_end_location_z = jsonEntry['shot']['end_location'][2]

                    shot_type_id = jsonEntry['shot']['type']['id']
                    shot_type_name = jsonEntry['shot']['type']['name']
                    shot_technique_id = jsonEntry['shot']['technique']['id']
                    shot_technique_name = jsonEntry['shot']['technique']['name']
                    shot_outcome_id = jsonEntry['shot']['outcome']['id']
                    shot_outcome_name = jsonEntry['shot']['outcome']['name']
                    shot_body_part_id = jsonEntry['shot']['body_part']['id']
                    shot_body_part_name = jsonEntry['shot']['body_part']['name']

                    shot_aerial_won = "NULL"
                    if "aerial_won" in jsonEntry['shot']:
                        shot_aerial_won = jsonEntry['shot']['aerial_won']

                    shot_follows_dribble = "NULL"
                    if "follows_dribble" in jsonEntry['shot']:
                        shot_follows_dribble = jsonEntry['shot']['follows_dribble']

                    shot_first_time = "NULL"
                    if "first_time" in jsonEntry['shot']:
                        shot_first_time = jsonEntry['shot']['first_time']

                    shot_open_goal = "NULL"
                    if "open_goal" in jsonEntry['shot']:
                        shot_open_goal = jsonEntry['shot']['open_goal']

                    shot_deflected = "NULL"
                    if "shot_deflected" in jsonEntry['shot']:
                        shot_deflected = jsonEntry['shot']['shot_deflected']

                    # INSERT INTO SHOT TABLE
                    if key_pass_id != "NULL":
                        sql_insert_into_shot_table = f"INSERT into event_shot(event_id, key_pass_id, shot_end_location_x, shot_end_location_y, shot_end_location_z, shot_aerial_won, shot_follows_dribble, shot_first_time, shot_open_goal, shot_stats_bomb_xg, shot_deflected, shot_technique_id, shot_technique_name, shot_body_part_id, shot_body_part_name, shot_type_id, shot_type_name, shot_outcome_id, shot_outcome_name) VALUES ('{event_id}', '{key_pass_id}', {shot_end_location_x}, {shot_end_location_y}, {shot_end_location_z}, {shot_aerial_won}, {shot_follows_dribble}, {shot_first_time}, {shot_open_goal}, {shot_stats_bomb_xg}, {shot_deflected}, {shot_technique_id}, '{shot_technique_name}', {shot_body_part_id}, '{shot_body_part_name}', {shot_type_id}, '{shot_type_name}', {shot_outcome_id}, '{shot_outcome_name}');"
                        cursor.execute(sql_insert_into_shot_table)
                    else:
                        sql_insert_into_shot_table = f"INSERT into event_shot(event_id, key_pass_id, shot_end_location_x, shot_end_location_y, shot_end_location_z, shot_aerial_won, shot_follows_dribble, shot_first_time, shot_open_goal, shot_stats_bomb_xg, shot_deflected, shot_technique_id, shot_technique_name, shot_body_part_id, shot_body_part_name, shot_type_id, shot_type_name, shot_outcome_id, shot_outcome_name) VALUES ('{event_id}', {key_pass_id}, {shot_end_location_x}, {shot_end_location_y}, {shot_end_location_z}, {shot_aerial_won}, {shot_follows_dribble}, {shot_first_time}, {shot_open_goal}, {shot_stats_bomb_xg}, {shot_deflected}, {shot_technique_id}, '{shot_technique_name}', {shot_body_part_id}, '{shot_body_part_name}', {shot_type_id}, '{shot_type_name}', {shot_outcome_id}, '{shot_outcome_name}');"
                        cursor.execute(sql_insert_into_shot_table)
                
                if event_name == 'Substitution':

                    substitution_outcome_id = jsonEntry['substitution']['outcome']['id']
                    substitution_outcome_name = jsonEntry['substitution']['outcome']['name']
                    substitution_replacement_id = jsonEntry['substitution']['replacement']['id']
                    
                    substitution_replacement_name = jsonEntry['substitution']['replacement']['name']
                    # We need to escape the ' character so postgres accepts it.
                    if substitution_replacement_name is not None:
                        if "'" in substitution_replacement_name:
                            substitution_replacement_name = substitution_replacement_name.replace("'", "''")                

                    # INSERT INTO SUBSTITUTION TABLE
                    sql_insert_into_substitution_table = f"INSERT into event_substitution(event_id, substitution_outcome_id, substitution_outcome_name, substitution_replacement_id, substitution_replacement_name) VALUES ('{event_id}', {substitution_outcome_id}, '{substitution_outcome_name}', {substitution_replacement_id}, '{substitution_replacement_name}');"
                    cursor.execute(sql_insert_into_substitution_table)

    cursor.execute("END TRANSACTION;")
    print("Events folder read finished!")

try:
    # TA: Before running the program you need to create a database using pgAdmin4 and put the database name here.

    # Attempt to connect to the database
    conn = psycopg.connect(dbname='project_soccer_database', user='postgres', password='1234', host='localhost', port=5432)

    # The cursor is used to make queries to the database/execute commands.
    cursor = conn.cursor()

    # Step 1: Load the competitions file
    load_competitions(cursor)

    # Step 2: Go into the matches files
    load_matches(cursor)

    # Step 3: Go into lineups file
    load_lineups(cursor)

    # Step 4: Go into events file
    load_events(cursor)

    print("Were done reading files!")

# Catch any errors psycopg throws
except psycopg.OperationalError as e:
    error_message = f"Error: {e}"
    print(error_message)
    exit(1)

# Close our database connections
finally:
    cursor.close()
    conn.commit()
    conn.close()