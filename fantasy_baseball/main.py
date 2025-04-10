# fantasy_baseball/main.py
# In fantasy_baseball/main.py
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Debug environment variables
print("Environment variables:")
print(f"RDS_HOST: {os.getenv('RDS_HOST')}")
print(f"RDS_DATABASE: {os.getenv('RDS_DATABASE')}")
print(f"RDS_USER: {os.getenv('RDS_USER')}")
print(f"RDS_PASSWORD: {'*' * len(os.getenv('RDS_PASSWORD', '')) if os.getenv('RDS_PASSWORD') else 'Not set'}")

# Get database connection parameters
DB_HOST = os.getenv("RDS_HOST")
DB_NAME = os.getenv("RDS_DATABASE")
DB_USER = os.getenv("RDS_USER")
DB_PASSWORD = os.getenv("RDS_PASSWORD")
DB_PORT = "5432"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fantasy_baseball.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("fantasy_baseball")

# Import components
from fantasy_baseball.core.database import DatabaseConnector
from fantasy_baseball.players.player_data import PlayerDataManager
from fantasy_baseball.teams.team_data import TeamDataManager
from fantasy_baseball.analytics.team_analyzer import TeamAnalyzer
from fantasy_baseball.visualization.data_viz import VisualizationManager

class FantasyBaseballManager:
    """Main class that integrates all components"""
    
    # In fantasy_baseball/main.py
    # In fantasy_baseball/main.py
    def __init__(self):
        """Initialize components"""
        # Create team manager first
        self.team_manager = TeamDataManager(
            host=DB_HOST, 
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )

        # Create player manager
        self.player_manager = PlayerDataManager(
            host=DB_HOST, 
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )

        # Create analyzer and pass the team_manager
        self.analyzer = TeamAnalyzer(
            host=DB_HOST, 
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            team_manager=self.team_manager  # Pass the existing team_manager
        )

        # Visualization manager doesn't need database connection
        self.visualizer = VisualizationManager()

        logger.info("Fantasy Baseball Manager initialized")
        
    def _get_connection(self):
        return self.pool.getconn()
        
    def _release_connection(self, conn):
        self.pool.putconn(conn)
        
    def method_that_needs_db(self):
        conn = self._get_connection()
        try:
            # Use connection
            return result
        finally:
            self._release_connection(conn)
    
    def get_all_teams(self):
        """Get all fantasy teams"""
        return self.team_manager.get_all_teams()
    
    def get_team_roster(self, team_id):
        """Get roster for a specific team"""
        return self.team_manager.get_team_roster(team_id)
    
    def analyze_team(self, team_id):
        """Perform comprehensive team analysis"""
        return self.analyzer.analyze_team(team_id)
    
    def search_players(self, name_fragment, limit=10):
        """Search for players by name"""
        return self.player_manager.get_players_by_name(name_fragment, limit)
    
    def get_available_players(self, team_id, position=None, search_term=None, limit=20):
        """Get players not on a specific team"""
        return self.player_manager.get_available_players(team_id, position, search_term, limit)
    
    def add_player_to_team(self, team_id, player_id, position, acquisition_type="waiver"):
        """Add a player to a team"""
        return self.team_manager.add_player_to_team(team_id, player_id, position, acquisition_type)
    
    def remove_player_from_team(self, team_id, player_id):
        """Remove a player from a team"""
        return self.team_manager.remove_player_from_team(team_id, player_id)
    
    def get_baseball_savant_data(self, season=2025, player_type='batter', min_pa=50):
        """Get data from Baseball Savant"""
        try:
            # Build URL for CSV export
            if player_type == 'batter':
                metrics = 'xba,xslg,xwoba,exit_velocity_avg,launch_angle_avg,barrel_batted_rate,hard_hit_percent'
            else:  # pitcher
                metrics = 'xera,xba,xslg,xwoba,exit_velocity_avg,barrel_batted_rate,hard_hit_percent'

            url = f"https://baseballsavant.mlb.com/leaderboard/custom?year={season}&type={player_type}&filter=&min={min_pa}&selections={metrics}&chart=false&x=xba&y=xba&r=no&chartType=scatter&csv=true"

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            }

            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                # Processing code
                return df
            else:
                logger.error(f"HTTP error {response.status_code} from Baseball Savant API")
                return self._create_mock_savant_data(player_type, season)

        except requests.ConnectionError as e:
            logger.error(f"Connection error accessing Baseball Savant: {e}")
            return self._create_mock_savant_data(player_type, season)
        except requests.Timeout as e:
            logger.error(f"Timeout error accessing Baseball Savant: {e}")
            return self._create_mock_savant_data(player_type, season)
        except requests.RequestException as e:
            logger.error(f"Request error accessing Baseball Savant: {e}")
            return self._create_mock_savant_data(player_type, season)
        except ValueError as e:
            logger.error(f"Data parsing error: {e}")
            return self._create_mock_savant_data(player_type, season)
        except Exception as e:
            logger.error(f"Unexpected error retrieving Baseball Savant data: {e}")
            return self._create_mock_savant_data(player_type, season)
    
    def _standardize_savant_columns(self, df):
        """Standardize column names from Baseball Savant"""
        column_mapping = {}
        
        # Map player name
        name_candidates = ['player_name', 'name', 'player', 'last_name,first_name', 'last_name, first_name']
        for col in df.columns:
            if any(candidate.lower() in col.lower() for candidate in name_candidates):
                column_mapping[col] = 'player_name'
                break
        
        # Add basic mappings for common columns with varying names
        basic_mappings = {
            'last_name, first_name': 'player_name',
            'xavg': 'xba',
            'est_ba': 'xba',
            'est_slg': 'xslg',
            'est_woba': 'xwoba',
            'exit_velo': 'exit_velocity_avg',
            'exit_speed': 'exit_velocity_avg',
            'launch_ang': 'launch_angle_avg',
            'brls/pa': 'barrel_batted_rate',
            'hard_pct': 'hard_hit_percent'
        }
        
        for old, new in basic_mappings.items():
            for col in df.columns:
                if old.lower() in col.lower():
                    column_mapping[col] = new
        
        return column_mapping
    
    def _create_mock_savant_data(self, player_type, season=2025):
        """Create mock Savant data when API fails"""
        import pandas as pd
        import random
        
        if player_type == 'batter':
            # Top MLB hitters
            players = [
                {'player_name': 'Aaron Judge', 'team': 'NYY', 'barrel_batted_rate': 19.8, 'xwoba': 0.435, 'hard_hit_percent': 60.5, 'exit_velocity_avg': 95.2},
                {'player_name': 'Shohei Ohtani', 'team': 'LAD', 'barrel_batted_rate': 18.2, 'xwoba': 0.422, 'hard_hit_percent': 58.3, 'exit_velocity_avg': 94.0},
                {'player_name': 'Juan Soto', 'team': 'NYY', 'barrel_batted_rate': 14.5, 'xwoba': 0.410, 'hard_hit_percent': 52.1, 'exit_velocity_avg': 92.8},
                {'player_name': 'Ronald Acuña Jr.', 'team': 'ATL', 'barrel_batted_rate': 13.8, 'xwoba': 0.395, 'hard_hit_percent': 54.7, 'exit_velocity_avg': 93.5},
                # Add more players...
            ]
            
            # Add launch angle for batters
            for player in players:
                player['launch_angle_avg'] = random.uniform(10, 25)
                player['xba'] = random.uniform(0.250, 0.330)
                player['xslg'] = random.uniform(0.400, 0.650)
        else:  # pitchers
            # Top MLB pitchers
            players = [
                {'player_name': 'Spencer Strider', 'team': 'ATL', 'barrel_batted_rate': 5.2, 'xwoba': 0.275, 'hard_hit_percent': 35.8, 'exit_velocity_avg': 87.2, 'xera': 2.85},
                {'player_name': 'Gerrit Cole', 'team': 'NYY', 'barrel_batted_rate': 6.1, 'xwoba': 0.285, 'hard_hit_percent': 37.2, 'exit_velocity_avg': 88.5, 'xera': 3.15},
                {'player_name': 'Zack Wheeler', 'team': 'PHI', 'barrel_batted_rate': 5.8, 'xwoba': 0.280, 'hard_hit_percent': 36.5, 'exit_velocity_avg': 87.8, 'xera': 3.05},
                # Add more players...
            ]
        
        # Create DataFrame
        df = pd.DataFrame(players)
        
        # Add year column
        df['year'] = season
        
        # Save to file
        df.to_csv(f'savant_{player_type}_data_{season}.csv', index=False)
        
        logger.info(f"Created mock {player_type} data for {season} with {len(df)} players")
        return df
    
    def display_advanced_metrics_dashboard(self, team_id=None):
        """Display the advanced metrics dashboard"""
        # Get Savant data
        batter_data = self.get_baseball_savant_data(season=2025, player_type='batter')
        pitcher_data = self.get_baseball_savant_data(season=2025, player_type='pitcher')
        
        # Filter for team players if team_id provided
        team_players = None
        if team_id:
            team_roster = self.team_manager.get_team_roster(team_id)
            if team_roster and 'players' in team_roster:
                team_players = [player['name'] for player in team_roster['players']]
                logger.info(f"Filtering dashboard for {len(team_players)} players on team {team_id}")
        
        # Display the dashboard
        self.visualizer.display_advanced_metrics_dashboard(batter_data, pitcher_data, team_players)
    
    def close(self):
        """Close all database connections"""
        self.player_manager.close()
        self.team_manager.close()
        self.analyzer.close()
        logger.info("All connections closed")