# fantasy_baseball/teams/team_data.py
from datetime import datetime
import logging
from ..core.database import DatabaseConnector

logger = logging.getLogger("fantasy_baseball")

class TeamDataManager(DatabaseConnector):
    """Handles team-related database operations"""

    def __init__(self, host=None, database=None, user=None, password=None, port=None):
        super().__init__(host, database, user, password, port)
    
    def get_all_teams(self):
        """Get all fantasy teams in the database"""
        query = """
            SELECT team_id, team_name, league_name, owner_name, league_size, 
                   league_format, season 
            FROM fantasy_teams
            ORDER BY season DESC, league_name, team_name
        """
        
        results = self.execute_query(query)
        
        if not results:
            logger.warning("No fantasy teams found")
            return []
            
        # Convert to list of dictionaries
        columns = ["team_id", "team_name", "league_name", "owner_name", 
                   "league_size", "league_format", "season"]
        return [dict(zip(columns, team)) for team in results]
    
    def get_team_by_id(self, team_id):
        """Get a team by ID"""
        try:
            query = """
                SELECT team_id, team_name, league_name, owner_name, league_size, 
                       league_format, season 
                FROM fantasy_teams
                WHERE team_id = %s
            """

            result = self.execute_query(query, (team_id,), fetch_all=False)

            if not result:
                logger.warning(f"Team ID {team_id} not found")
                return None

            columns = ["team_id", "team_name", "league_name", "owner_name", 
                      "league_size", "league_format", "season"]
            return dict(zip(columns, result))

        except psycopg2.DatabaseError as e:
            logger.error(f"Database error retrieving team {team_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error retrieving team {team_id}: {e}")
            return None
    
    def get_team_with_mlb_id(self, mlb_id):
        """
        Find the fantasy team that has a player with the given MLB ID

        Args:
            mlb_id (str): The MLB ID of the player

        Returns:
            dict: Team data or None if not found
        """
        try:
            # First, get the player_id for the given MLB ID
            query_player = """
                SELECT player_id
                FROM players
                WHERE mlb_id = %s
            """

            player_result = self.execute_query(query_player, (mlb_id,), fetch_all=False)

            if not player_result:
                logger.warning(f"No player found with MLB ID {mlb_id}")
                return None

            player_id = player_result[0]

            # Now find the team that has this player on its roster
            query_team = """
                SELECT t.team_id, t.team_name, t.league_name, t.owner_name, 
                       t.league_size, t.league_format, t.season
                FROM fantasy_teams t
                JOIN fantasy_rosters r ON t.team_id = r.team_id
                WHERE r.player_id = %s AND r.is_active = TRUE
                LIMIT 1
            """

            team_result = self.execute_query(query_team, (player_id,), fetch_all=False)

            if not team_result:
                logger.info(f"Player with MLB ID {mlb_id} is not on any active roster")
                return None

            # Convert to dictionary
            columns = ["team_id", "team_name", "league_name", "owner_name", 
                      "league_size", "league_format", "season"]
            team_data = dict(zip(columns, team_result))

            logger.info(f"Found team {team_data['team_name']} with player MLB ID {mlb_id}")
            return team_data

        except psycopg2.OperationalError as e:
            logger.error(f"Database connection error looking for team with player {mlb_id}: {e}")
            return None
        except psycopg2.DatabaseError as e:
            logger.error(f"Database error looking for team with player {mlb_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error looking for team with player {mlb_id}: {e}")
            return None
    
    def get_team_roster(self, team_id):
        """Get the complete roster for a team"""
        # First get team info
        team_data = self.get_team_by_id(team_id)
        if not team_data:
            return None
        
        team_data['players'] = []
        
        # Get roster players
        query = """
            SELECT 
                p.player_id, p.mlb_id, p.full_name, p.team, p.position, 
                r.position as fantasy_position, r.acquisition_type, r.acquisition_date,
                CASE
                    WHEN p.position = 'P' THEN 'pitcher'
                    ELSE 'batter'
                END as player_type
            FROM fantasy_rosters r
            JOIN players p ON r.player_id = p.player_id
            WHERE r.team_id = %s AND r.is_active = TRUE
            ORDER BY 
                CASE 
                    WHEN r.position IN ('C', '1B', '2B', '3B', 'SS', 'OF', 'DH', 'UTIL') THEN 1
                    WHEN r.position IN ('SP', 'RP') THEN 2
                    ELSE 3
                END,
                r.position
        """
        
        roster = self.execute_query(query, (team_id,))
        
        if not roster:
            logger.info(f"No players found for team {team_id}")
            return team_data
        
        columns = ["player_id", "mlb_id", "name", "team", "position", 
                  "fantasy_position", "acquisition_type", "acquisition_date", "player_type"]
        
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            
            # Process each player
            for player in roster:
                player_dict = dict(zip(columns, player))
                player_dict['stats'] = {}
                
                player_id = player_dict['player_id']
                player_type = player_dict['player_type']
                
                # Get stats based on player type
                if player_type == 'batter':
                    cursor.execute("""
                        SELECT 
                            games, plate_appearances, home_runs, rbi, stolen_bases,
                            batting_avg, on_base_pct, slugging_pct, ops
                        FROM batting_stats
                        WHERE player_id = %s AND season = %s
                    """, (player_id, team_data['season']))
                    
                    stat_row = cursor.fetchone()
                    
                    if stat_row:
                        stat_columns = ["games", "plate_appearances", "home_runs", "rbi", "stolen_bases",
                                      "batting_avg", "on_base_pct", "slugging_pct", "ops"]
                        player_dict['stats'] = dict(zip(stat_columns, stat_row))
                    
                else:  # pitcher
                    cursor.execute("""
                        SELECT 
                            games, innings_pitched, wins, saves, strikeouts,
                            era, whip, k_per_9
                        FROM pitching_stats
                        WHERE player_id = %s AND season = %s
                    """, (player_id, team_data['season']))
                    
                    stat_row = cursor.fetchone()
                    
                    if stat_row:
                        stat_columns = ["games", "innings_pitched", "wins", "saves", "strikeouts",
                                       "era", "whip", "k_per_9"]
                        player_dict['stats'] = dict(zip(stat_columns, stat_row))
                
                team_data['players'].append(player_dict)
                
            logger.info(f"Retrieved roster for team {team_id} with {len(roster)} players")
            return team_data
            
        except Exception as e:
            logger.error(f"Error retrieving team roster: {e}")
            return team_data
        finally:
            cursor.close()
            self.release_connection(conn)
    
    def add_player_to_team(self, team_id, player_id, position, acquisition_type="waiver"):
        """Add a player to a team's roster"""
        query = """
            INSERT INTO fantasy_rosters 
            (team_id, player_id, position, acquisition_type, acquisition_date, is_active) 
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (team_id, player_id) 
            DO UPDATE SET
                position = EXCLUDED.position,
                acquisition_type = EXCLUDED.acquisition_type,
                acquisition_date = EXCLUDED.acquisition_date,
                is_active = EXCLUDED.is_active
        """
        
        try:
            self.execute_query(
                query, 
                (team_id, player_id, position, acquisition_type, datetime.now(), True),
                commit=True,
                fetch=False
            )
            logger.info(f"Added player {player_id} to team {team_id}")
            return True
        except Exception as e:
            logger.error(f"Error adding player to team: {e}")
            return False
    
    def remove_player_from_team(self, team_id, player_id):
        """Remove a player from a team's roster"""
        query = """
            DELETE FROM fantasy_rosters 
            WHERE team_id = %s AND player_id = %s
        """
        
        try:
            result = self.execute_query(query, (team_id, player_id), commit=True, fetch=False)
            logger.info(f"Removed player {player_id} from team {team_id}")
            return True
        except Exception as e:
            logger.error(f"Error removing player from team: {e}")
            return False