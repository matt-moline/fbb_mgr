# nfbc_manual_import.py

import pandas as pd
import os
import logging
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("nfbc_manual_import.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("nfbc_manual_import")

# Load environment variables
load_dotenv()

class NFBCManualImporter:
    def __init__(self):
        """Initialize NFBC Manual Importer with database connection"""
        self.conn = None
        self.connect_to_db()
        
    def connect_to_db(self):
        """Establish database connection"""
        try:
            # Get database credentials from environment variables
            db_host = os.getenv("RDS_HOST")
            db_name = os.getenv("RDS_DATABASE")
            db_user = os.getenv("RDS_USER")
            db_password = os.getenv("RDS_PASSWORD")
            db_port = "5432"
            
            self.conn = psycopg2.connect(
                host=db_host,
                database=db_name,
                user=db_user,
                password=db_password,
                port=db_port
            )
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def import_team_file(self, file_path, league_name=None, season=2025):
        """Import team data from manually downloaded CSV/HTML file"""
        try:
            # Determine file type by extension
            _, ext = os.path.splitext(file_path)
            
            if ext.lower() == '.csv':
                # Process CSV file
                df = pd.read_csv(file_path)
                return self._process_team_csv(df, league_name, season)
            elif ext.lower() in ['.html', '.htm']:
                # Process HTML file
                with open(file_path, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                df = pd.read_html(html_content)
                # Usually team data is in the first table
                return self._process_team_html(df[0], league_name, season)
            else:
                logger.error(f"Unsupported file type: {ext}")
                return False
        except Exception as e:
            logger.error(f"Error importing team file: {e}")
            return False
    
    def import_roster_file(self, file_path, team_id=None):
        """Import roster data from manually downloaded CSV/HTML file"""
        try:
            # Determine file type by extension
            _, ext = os.path.splitext(file_path)
            
            if ext.lower() == '.csv':
                # Process CSV file
                df = pd.read_csv(file_path)
                return self._process_roster_csv(df, team_id)
            elif ext.lower() in ['.html', '.htm']:
                # Process HTML file
                with open(file_path, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                dfs = pd.read_html(html_content)
                # Find the roster table (usually it has Player, Team, Position columns)
                for df in dfs:
                    if 'Player' in df.columns and 'Team' in df.columns:
                        return self._process_roster_html(df, team_id)
                logger.error("Couldn't find roster table in HTML")
                return False
            else:
                logger.error(f"Unsupported file type: {ext}")
                return False
        except Exception as e:
            logger.error(f"Error importing roster file: {e}")
            return False
    
    def import_standings_file(self, file_path, league_name=None, season=2025):
        """Import standings data from manually downloaded CSV/HTML file"""
        try:
            # Similar implementation to the roster import
            _, ext = os.path.splitext(file_path)
            
            if ext.lower() == '.csv':
                df = pd.read_csv(file_path)
                return self._process_standings_csv(df, league_name, season)
            elif ext.lower() in ['.html', '.htm']:
                with open(file_path, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                dfs = pd.read_html(html_content)
                # Find the standings table
                for df in dfs:
                    if 'Rank' in df.columns or 'Team' in df.columns:
                        return self._process_standings_html(df, league_name, season)
                logger.error("Couldn't find standings table in HTML")
                return False
            else:
                logger.error(f"Unsupported file type: {ext}")
                return False
        except Exception as e:
            logger.error(f"Error importing standings file: {e}")
            return False
    
    def _process_team_csv(self, df, league_name, season):
        """Process team data from CSV format"""
        # Example implementation - adjust based on actual CSV structure
        cursor = self.conn.cursor()
        try:
            teams_added = 0
            
            # Assuming CSV has columns like Team, Owner, etc.
            for _, row in df.iterrows():
                team_name = row.get('Team', row.get('TeamName', 'Unknown Team'))
                owner_name = row.get('Owner', row.get('OwnerName', 'Unknown Owner'))
                league_size = row.get('LeagueSize', 12)  # Default if not present
                league_format = row.get('Format', 'NFBC')  # Default if not present
                
                # Insert into fantasy_teams table
                sql = """
                    INSERT INTO fantasy_teams 
                    (team_name, league_name, owner_name, league_size, league_format, season)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (team_name, league_name, season) 
                    DO UPDATE SET
                        owner_name = EXCLUDED.owner_name,
                        league_size = EXCLUDED.league_size,
                        league_format = EXCLUDED.league_format
                    RETURNING team_id
                """
                cursor.execute(sql, (
                    team_name, 
                    league_name, 
                    owner_name, 
                    league_size, 
                    league_format, 
                    season
                ))
                
                # Get the team_id
                team_id = cursor.fetchone()[0]
                teams_added += 1
            
            self.conn.commit()
            logger.info(f"Successfully imported {teams_added} teams")
            return True
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error processing team CSV: {e}")
            return False
            
        finally:
            cursor.close()
    
    def _process_team_html(self, df, league_name, season):
        """Process team data from HTML format"""
        # Similar to CSV processing but with HTML table structure
        # Implementation will depend on actual NFBC HTML structure
        # This is a placeholder - adjust based on actual HTML structure
        return self._process_team_csv(df, league_name, season)
    
    def _process_roster_csv(self, df, team_id):
        """Process roster data from CSV format"""
        cursor = self.conn.cursor()
        try:
            # First, update or insert players
            players_added = 0
            roster_entries = 0
            
            for _, row in df.iterrows():
                player_name = row.get('Player', row.get('Name'))
                team = row.get('Team', row.get('MLB', 'N/A'))
                position = row.get('Position', row.get('Pos', 'N/A'))
                
                # Insert player into players table
                sql = """
                    INSERT INTO players (full_name, team, position)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (full_name) 
                    DO UPDATE SET
                        team = EXCLUDED.team,
                        position = EXCLUDED.position
                    RETURNING player_id
                """
                cursor.execute(sql, (player_name, team, position))
                player_id = cursor.fetchone()[0]
                players_added += 1
                
                # Insert into fantasy_rosters table
                if team_id:
                    fantasy_position = row.get('RosterPos', position)
                    sql = """
                        INSERT INTO fantasy_rosters
                        (team_id, player_id, position, acquisition_type, acquisition_date, is_active)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (team_id, player_id) 
                        DO UPDATE SET
                            position = EXCLUDED.position,
                            is_active = EXCLUDED.is_active
                    """
                    cursor.execute(sql, (
                        team_id, 
                        player_id, 
                        fantasy_position, 
                        'draft',  # Default acquisition type
                        datetime.now(),  # Current date as acquisition date
                        True  # Is active
                    ))
                    roster_entries += 1
            
            self.conn.commit()
            logger.info(f"Successfully imported {players_added} players and {roster_entries} roster entries")
            return True
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error processing roster CSV: {e}")
            return False
            
        finally:
            cursor.close()
    
    def _process_roster_html(self, df, team_id):
        """Process roster data from HTML format"""
        # Similar implementation to CSV but for HTML structure
        return self._process_roster_csv(df, team_id)
    
    def _process_standings_csv(self, df, league_name, season):
        """Process standings data from CSV format"""
        # Implementation will depend on your database schema
        # This is a placeholder based on common standings structure
        logger.info(f"Processing standings for {league_name}, season {season}")
        return True
    
    def _process_standings_html(self, df, league_name, season):
        """Process standings data from HTML format"""
        # Similar implementation to CSV but for HTML structure
        return self._process_standings_csv(df, league_name, season)
    
    def close(self):
        """Close database connection"""
        if self.conn is not None and not self.conn.closed:
            self.conn.close()
            logger.info("Database connection closed")