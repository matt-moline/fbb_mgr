# fantasy_baseball/players/player_data.py
from datetime import datetime
import logging
from ..core.database import DatabaseConnector

logger = logging.getLogger("fantasy_baseball")

class PlayerDataManager(DatabaseConnector):
    """Handles player-related database operations"""
    
    def __init__(self, host=None, database=None, user=None, password=None, port=None):
        super().__init__(host, database, user, password, port)
    
    def get_player_by_mlb_id(self, mlb_id):
        """Get a player by MLB ID"""
        try:
            query = """
                SELECT player_id, mlb_id, full_name, team, position, active
                FROM players
                WHERE mlb_id = %s
            """
            return self.execute_query(query, (mlb_id,), fetch=True, fetch_all=False)
        except psycopg2.DatabaseError as e:
            logger.error(f"Database error retrieving player {mlb_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error retrieving player {mlb_id}: {e}")
            return None
    
    def get_players_by_name(self, name_fragment, limit=10):
        """Search for players by name fragment"""
        query = """
            SELECT player_id, mlb_id, full_name, team, position
            FROM players
            WHERE full_name ILIKE %s
            ORDER BY full_name
            LIMIT %s
        """
        return self.execute_query(query, (f"%{name_fragment}%", limit), fetch=True)
    
    def get_player_stats(self, player_id, season, stat_type='batting'):
        """Get player stats for a season"""
        if stat_type == 'batting':
            query = """
                SELECT 
                    games, plate_appearances, home_runs, rbi, stolen_bases,
                    batting_avg, on_base_pct, slugging_pct, ops
                FROM batting_stats
                WHERE player_id = %s AND season = %s
            """
        else:  # pitching
            query = """
                SELECT 
                    games, innings_pitched, wins, saves, strikeouts,
                    era, whip, k_per_9
                FROM pitching_stats
                WHERE player_id = %s AND season = %s
            """
        
        return self.execute_query(query, (player_id, season), fetch=True, fetch_all=False)
    
    def add_player(self, mlb_id, full_name, team, position, active=True):
        """Add a new player to the database"""
        query = """
            INSERT INTO players (mlb_id, full_name, team, position, active, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (mlb_id) 
            DO UPDATE SET
                full_name = EXCLUDED.full_name,
                team = EXCLUDED.team,
                position = EXCLUDED.position,
                active = EXCLUDED.active,
                last_updated = EXCLUDED.last_updated
            RETURNING player_id
        """
        
        result = self.execute_query(
            query, 
            (mlb_id, full_name, team, position, active, datetime.now()),
            fetch=True,
            commit=True,
            fetch_all=False
        )
        
        if result:
            logger.info(f"Added/updated player {full_name} (ID: {result[0]})")
            return result[0]
        return None
    
    def get_available_players(self, team_id, position=None, search_term=None, limit=20):
        """Get players not on a specific team"""
        query_parts = ["""
            SELECT p.player_id, p.mlb_id, p.full_name, p.team, p.position
            FROM players p
            WHERE p.active = TRUE
            AND p.player_id NOT IN (
                SELECT player_id FROM fantasy_rosters WHERE team_id = %s
            )
        """]
        
        params = [team_id]
        
        if position:
            query_parts.append("AND p.position = %s")
            params.append(position)
        
        if search_term:
            query_parts.append("AND p.full_name ILIKE %s")
            params.append(f"%{search_term}%")
        
        query_parts.append("ORDER BY p.full_name LIMIT %s")
        params.append(limit)
        
        query = " ".join(query_parts)
        
        results = self.execute_query(query, params, fetch=True)
        
        # Convert to list of dictionaries
        if results:
            columns = ["player_id", "mlb_id", "full_name", "team", "position"]
            return [dict(zip(columns, row)) for row in results]
        return []