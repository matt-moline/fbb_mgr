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

    def import_league_players_csv(self, file_path, league_id=None, team_id=662833):
        """
        Import a CSV file containing all players in a league, including their team assignments

        Args:
            file_path (str): Path to the CSV file
            league_id (int): League ID (optional)
            team_id (int): Your team ID to identify your players

        Returns:
            bool: Success status
        """
        try:
            # Read the CSV file
            df = pd.read_csv(file_path)

            logger.info(f"Processing league players CSV with {len(df)} players")

            # Connect to database
            cursor = self.conn.cursor()

            # Determine column names based on CSV structure
            # This step is important as NFBC column names might vary
            required_fields = ['Player', 'Team']

            # Map common variations of column names
            column_mapping = {
                'Player': ['Player', 'Name', 'PlayerName'],
                'Team': ['Team', 'MLB Team', 'MLBTeam'],
                'Position': ['Position', 'Pos', 'PrimaryPosition'],
                'FantasyTeam': ['Fantasy Team', 'Fantasy_Team', 'Owner', 'OwnerTeam'],
                'Salary': ['Salary', 'Cost', 'Value'],
                'Status': ['Status', 'PlayerStatus', 'Roster']
            }

            # Create a standardized dataframe with consistent column names
            standardized_df = pd.DataFrame()

            # Map columns to standardized names
            for std_name, variations in column_mapping.items():
                for var in variations:
                    if var in df.columns:
                        standardized_df[std_name] = df[var]
                        break

                # If we couldn't find a match for required fields, raise an error
                if std_name in required_fields and std_name not in standardized_df.columns:
                    logger.error(f"Required column '{std_name}' not found in CSV")
                    return False

            # Add default position if not found
            if 'Position' not in standardized_df.columns:
                standardized_df['Position'] = 'Unknown'

            # Process each player
            players_processed = 0
            roster_entries = 0

            for _, row in standardized_df.iterrows():
                player_name = row['Player']
                team = row['Team']
                position = row.get('Position', 'Unknown')
                fantasy_team = row.get('FantasyTeam', None)

                # Insert or update player in the players table
                sql = """
                    INSERT INTO players (full_name, team, position, active, last_updated)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (full_name) 
                    DO UPDATE SET
                        team = EXCLUDED.team,
                        position = EXCLUDED.position,
                        active = EXCLUDED.active,
                        last_updated = EXCLUDED.last_updated
                    RETURNING player_id
                """
                cursor.execute(sql, (
                    player_name, 
                    team, 
                    position, 
                    True,  # Active
                    datetime.now()
                ))
                player_id = cursor.fetchone()[0]
                players_processed += 1

                # If the player is on a fantasy team and we know which one
                if fantasy_team and fantasy_team.strip() and fantasy_team.lower() != 'free agent':
                    # Get or create fantasy team record
                    if league_id:
                        # If we know the league_id, use it to find the team
                        cursor.execute("""
                            SELECT team_id FROM fantasy_teams 
                            WHERE team_name = %s AND league_id = %s
                        """, (fantasy_team, league_id))
                    else:
                        # Otherwise just match by team name
                        cursor.execute("""
                            SELECT team_id FROM fantasy_teams 
                            WHERE team_name = %s
                        """, (fantasy_team,))

                    team_result = cursor.fetchone()

                    if team_result:
                        fantasy_team_id = team_result[0]

                        # Insert or update roster entry
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
                            fantasy_team_id, 
                            player_id, 
                            position, 
                            'unknown',  # Acquisition type
                            datetime.now(),  # Acquisition date
                            True  # Is active
                        ))
                        roster_entries += 1

                # If this player is on the specified team (your team)
                if team_id:
                    if fantasy_team and fantasy_team.strip() and team_id in str(fantasy_team):
                        # For your team, you might want to add more detailed roster information
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
                            position, 
                            'unknown',  # Acquisition type
                            datetime.now(),  # Acquisition date
                            True  # Is active
                        ))

            # Commit all changes
            self.conn.commit()

            logger.info(f"Successfully processed {players_processed} players and {roster_entries} roster entries")
            return True

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error importing league players CSV: {e}")
            return False

        finally:
            if 'cursor' in locals():
                cursor.close()

    def identify_free_agents(self, league_id, position=None):
        """
        Identify free agents in a specific league

        Args:
            league_id (int): The league ID
            position (str, optional): Filter by position

        Returns:
            list: List of free agent players
        """
        try:
            cursor = self.conn.cursor()

            # Get all teams in the league
            cursor.execute("""
                SELECT team_id FROM fantasy_teams 
                WHERE league_id = %s
            """, (league_id,))

            team_ids = [row[0] for row in cursor.fetchall()]

            if not team_ids:
                logger.error(f"No teams found for league ID {league_id}")
                return []

            # Find all players rostered in this league
            placeholders = ', '.join(['%s'] * len(team_ids))
            cursor.execute(f"""
                SELECT player_id FROM fantasy_rosters 
                WHERE team_id IN ({placeholders}) AND is_active = TRUE
            """, team_ids)

            rostered_player_ids = [row[0] for row in cursor.fetchall()]

            # Build query to find players not on any team
            query = """
                SELECT p.player_id, p.full_name, p.team, p.position
                FROM players p
                WHERE p.active = TRUE
            """

            params = []

            # Add position filter if specified
            if position:
                query += " AND p.position = %s"
                params.append(position)

            # Exclude rostered players
            if rostered_player_ids:
                placeholders = ', '.join(['%s'] * len(rostered_player_ids))
                query += f" AND p.player_id NOT IN ({placeholders})"
                params.extend(rostered_player_ids)

            query += " ORDER BY p.full_name"

            cursor.execute(query, params)

            # Format the results
            free_agents = [
                {
                    'player_id': row[0],
                    'name': row[1],
                    'team': row[2],
                    'position': row[3]
                }
                for row in cursor.fetchall()
            ]

            return free_agents

        except Exception as e:
            logger.error(f"Error identifying free agents: {e}")
            return []

        finally:
            if 'cursor' in locals():
                cursor.close()

    def project_team_standings(self, league_id, stats_source='current'):
        """
        Project team standings based on player stats

        Args:
            league_id (int): The league ID
            stats_source (str): 'current' or 'projected'

        Returns:
            dict: Projected standings by category
        """
        try:
            cursor = self.conn.cursor()

            # Get all teams in the league
            cursor.execute("""
                SELECT team_id, team_name 
                FROM fantasy_teams 
                WHERE league_id = %s
            """, (league_id,))

            teams = {row[0]: row[1] for row in cursor.fetchall()}

            if not teams:
                logger.error(f"No teams found for league ID {league_id}")
                return {}

            # Initialize results
            results = {team_name: {
                'batting': {
                    'avg': 0.0,
                    'hr': 0,
                    'r': 0,
                    'rbi': 0,
                    'sb': 0
                },
                'pitching': {
                    'era': 0.0,
                    'whip': 0.0,
                    'k': 0,
                    'w': 0,
                    'sv': 0
                }
            } for team_name in teams.values()}

            # Process batters
            for team_id, team_name in teams.items():
                # Get all batters for this team
                cursor.execute("""
                    SELECT p.player_id, p.full_name
                    FROM fantasy_rosters fr
                    JOIN players p ON fr.player_id = p.player_id
                    WHERE fr.team_id = %s AND fr.is_active = TRUE AND p.position != 'P'
                """, (team_id,))

                batters = cursor.fetchall()

                # For each batter, get their stats
                for player_id, _ in batters:
                    if stats_source == 'current':
                        cursor.execute("""
                            SELECT 
                                batting_avg, home_runs, runs, rbi, stolen_bases
                            FROM batting_stats
                            WHERE player_id = %s
                            ORDER BY season DESC
                            LIMIT 1
                        """, (player_id,))
                    else:
                        # For projected stats, you'd need a separate projected_stats table
                        # This is just a placeholder
                        cursor.execute("""
                            SELECT 
                                batting_avg, home_runs, runs, rbi, stolen_bases
                            FROM batting_stats
                            WHERE player_id = %s
                            ORDER BY season DESC
                            LIMIT 1
                        """, (player_id,))

                    stats = cursor.fetchone()

                    if stats:
                        results[team_name]['batting']['avg'] += stats[0] or 0
                        results[team_name]['batting']['hr'] += stats[1] or 0
                        results[team_name]['batting']['r'] += stats[2] or 0
                        results[team_name]['batting']['rbi'] += stats[3] or 0
                        results[team_name]['batting']['sb'] += stats[4] or 0

                # Normalize batting average
                if len(batters) > 0:
                    results[team_name]['batting']['avg'] /= len(batters)

                # Get all pitchers for this team
                cursor.execute("""
                    SELECT p.player_id, p.full_name
                    FROM fantasy_rosters fr
                    JOIN players p ON fr.player_id = p.player_id
                    WHERE fr.team_id = %s AND fr.is_active = TRUE AND p.position = 'P'
                """, (team_id,))

                pitchers = cursor.fetchall()

                # For each pitcher, get their stats
                for player_id, _ in pitchers:
                    if stats_source == 'current':
                        cursor.execute("""
                            SELECT 
                                era, whip, strikeouts, wins, saves
                            FROM pitching_stats
                            WHERE player_id = %s
                            ORDER BY season DESC
                            LIMIT 1
                        """, (player_id,))
                    else:
                        # For projected stats
                        cursor.execute("""
                            SELECT 
                                era, whip, strikeouts, wins, saves
                            FROM pitching_stats
                            WHERE player_id = %s
                            ORDER BY season DESC
                            LIMIT 1
                        """, (player_id,))

                    stats = cursor.fetchone()

                    if stats:
                        # For ERA and WHIP, we'll need a weighted average based on innings pitched
                        # This is simplified for now
                        results[team_name]['pitching']['era'] += stats[0] or 0
                        results[team_name]['pitching']['whip'] += stats[1] or 0
                        results[team_name]['pitching']['k'] += stats[2] or 0
                        results[team_name]['pitching']['w'] += stats[3] or 0
                        results[team_name]['pitching']['sv'] += stats[4] or 0

                # Normalize ERA and WHIP
                if len(pitchers) > 0:
                    results[team_name]['pitching']['era'] /= len(pitchers)
                    results[team_name]['pitching']['whip'] /= len(pitchers)

            # Calculate rankings for each category
            categories = {
                'batting': ['avg', 'hr', 'r', 'rbi', 'sb'],
                'pitching': ['era', 'whip', 'k', 'w', 'sv']
            }

            rankings = {team_name: {'total': 0} for team_name in teams.values()}

            for stat_type, cats in categories.items():
                for cat in cats:
                    # Sort teams by this category
                    if cat in ['era', 'whip']:  # Lower is better
                        sorted_teams = sorted(teams.values(), key=lambda t: results[t][stat_type][cat])
                    else:  # Higher is better
                        sorted_teams = sorted(teams.values(), key=lambda t: results[t][stat_type][cat], reverse=True)

                    # Assign points (1 for last place, N for first place)
                    for i, team_name in enumerate(sorted_teams, 1):
                        if cat not in rankings[team_name]:
                            rankings[team_name][cat] = 0
                        rankings[team_name][cat] = i
                        rankings[team_name]['total'] += i

            # Sort teams by total points
            final_standings = sorted(rankings.items(), key=lambda x: x[1]['total'], reverse=True)

            return {
                'raw_stats': results,
                'rankings': rankings,
                'standings': final_standings
            }

        except Exception as e:
            logger.error(f"Error projecting team standings: {e}")
            return {}

        finally:
            if 'cursor' in locals():
                cursor.close()                
                
                
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