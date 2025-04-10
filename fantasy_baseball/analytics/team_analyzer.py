# fantasy_baseball/analytics/team_analyzer.py
import pandas as pd
import logging
from ..core.database import DatabaseConnector
from ..teams.team_data import TeamDataManager

logger = logging.getLogger("fantasy_baseball")

class TeamAnalyzer(DatabaseConnector):
    """Handles team analysis and recommendations"""
    
    def __init__(self, host=None, database=None, user=None, password=None, port=None, team_manager=None):
        super().__init__(host, database, user, password, port)
        
        # Use provided team_manager or create a new one with the same connection parameters
        self.team_manager = team_manager or TeamDataManager(
            host=host, 
            database=database,
            user=user,
            password=password,
            port=port
        )
    
    def analyze_team(self, team_id):
        """Perform comprehensive analysis of a fantasy team"""
        # Get team roster data
        team_data = self.team_manager.get_team_roster(team_id)
        
        if not team_data:
            logger.error(f"No data found for team {team_id}")
            return None
        
        analysis = {
            'team_id': team_id,
            'team_name': team_data['team_name'],
            'league_name': team_data['league_name'],
            'roster_breakdown': self._get_roster_breakdown(team_data),
            'category_analysis': self._analyze_categories(team_data),
            'team_strengths': [],
            'team_weaknesses': [],
            'recommended_actions': []
        }
        
        # Determine strengths and weaknesses
        for category, rating in analysis['category_analysis'].items():
            if rating >= 8:
                analysis['team_strengths'].append(category)
            elif rating <= 4:
                analysis['team_weaknesses'].append(category)
        
        # Generate recommendations
        analysis['recommended_actions'] = self._generate_recommendations(analysis)
        
        logger.info(f"Completed analysis for team {team_id}")
        return analysis
    
    def _get_roster_breakdown(self, team_data):
        """Generate roster position breakdown"""
        positions = {}
        
        for player in team_data['players']:
            pos = player['fantasy_position']
            if pos not in positions:
                positions[pos] = 0
            positions[pos] += 1
        
        return positions
    
    def _analyze_categories(self, team_data):
        """Analyze team performance in standard fantasy categories"""
        # Extract batting stats
        batters = [p for p in team_data['players'] if p.get('stats') and 'batting_avg' in p.get('stats', {})]
        
        # Extract pitching stats
        pitchers = [p for p in team_data['players'] if p.get('stats') and 'era' in p.get('stats', {})]
        
        # Convert to DataFrames for more efficient processing
        batters_df = pd.DataFrame([p['stats'] for p in batters]) if batters else pd.DataFrame()
        pitchers_df = pd.DataFrame([p['stats'] for p in pitchers]) if pitchers else pd.DataFrame()
        
        # Get league averages for comparison
        league_avgs = self._get_league_averages(team_data['season'])
        
        # Calculate team totals and weighted averages
        ratings = {}
        
        # Calculate hitting stats
        if not batters_df.empty:
            # Totals
            batting_totals = {
                'home_runs': batters_df['home_runs'].sum() if 'home_runs' in batters_df else 0,
                'rbi': batters_df['rbi'].sum() if 'rbi' in batters_df else 0,
                'stolen_bases': batters_df['stolen_bases'].sum() if 'stolen_bases' in batters_df else 0
            }
            
            # Weighted batting average
            if 'plate_appearances' in batters_df and 'batting_avg' in batters_df:
                team_avg = (batters_df['batting_avg'] * batters_df['plate_appearances']).sum() / batters_df['plate_appearances'].sum()
            else:
                team_avg = 0
            
            # Rate home runs (higher is better)
            if league_avgs.get('home_runs'):
                hr_rating = min(10, int((batting_totals['home_runs'] / league_avgs['home_runs']) * 5) + 5)
                ratings['home_runs'] = max(1, hr_rating)
            
            # Rate RBI (higher is better)
            if league_avgs.get('rbi'):
                rbi_rating = min(10, int((batting_totals['rbi'] / league_avgs['rbi']) * 5) + 5)
                ratings['rbi'] = max(1, rbi_rating)
            
            # Rate stolen bases (higher is better)
            if league_avgs.get('stolen_bases'):
                sb_rating = min(10, int((batting_totals['stolen_bases'] / league_avgs['stolen_bases']) * 5) + 5)
                ratings['stolen_bases'] = max(1, sb_rating)
            
            # Rate batting average (higher is better)
            if league_avgs.get('batting_avg'):
                avg_diff = team_avg - league_avgs['batting_avg']
                avg_rating = min(10, int((avg_diff / 0.030) * 5) + 5)  # 0.030 full spread
                ratings['batting_avg'] = max(1, avg_rating)
        
        # Calculate pitching stats
        if not pitchers_df.empty:
            # Weighted ERA and WHIP
            if 'innings_pitched' in pitchers_df:
                if 'era' in pitchers_df:
                    team_era = (pitchers_df['era'] * pitchers_df['innings_pitched']).sum() / pitchers_df['innings_pitched'].sum()
                else:
                    team_era = 0
                    
                if 'whip' in pitchers_df:
                    team_whip = (pitchers_df['whip'] * pitchers_df['innings_pitched']).sum() / pitchers_df['innings_pitched'].sum()
                else:
                    team_whip = 0
            else:
                team_era = 0
                team_whip = 0
            
            # Pitching counting stats
            pitching_totals = {
                'wins': pitchers_df['wins'].sum() if 'wins' in pitchers_df else 0,
                'saves': pitchers_df['saves'].sum() if 'saves' in pitchers_df else 0,
                'strikeouts': pitchers_df['strikeouts'].sum() if 'strikeouts' in pitchers_df else 0
            }
            
            # Rate ERA (lower is better)
            if league_avgs.get('era'):
                era_diff = league_avgs['era'] - team_era
                era_rating = min(10, int((era_diff / 0.50) * 5) + 5)  # 0.50 full spread
                ratings['era'] = max(1, era_rating)
            
            # Rate WHIP (lower is better)
            if league_avgs.get('whip'):
                whip_diff = league_avgs['whip'] - team_whip
                whip_rating = min(10, int((whip_diff / 0.10) * 5) + 5)  # 0.10 full spread
                ratings['whip'] = max(1, whip_rating)
            
            # Rate wins (higher is better)
            if league_avgs.get('wins'):
                wins_rating = min(10, int((pitching_totals['wins'] / league_avgs['wins']) * 5) + 5)
                ratings['wins'] = max(1, wins_rating)
            
            # Rate saves (higher is better)
            if league_avgs.get('saves'):
                saves_rating = min(10, int((pitching_totals['saves'] / league_avgs['saves']) * 5) + 5)
                ratings['saves'] = max(1, saves_rating)
            
            # Rate strikeouts (higher is better)
            if league_avgs.get('strikeouts'):
                k_rating = min(10, int((pitching_totals['strikeouts'] / league_avgs['strikeouts']) * 5) + 5)
                ratings['strikeouts'] = max(1, k_rating)
        
        return ratings
    
    def _get_league_averages(self, season):
        """Get league average statistics for comparison"""
        # Batting averages query
        batting_query = """
            SELECT
                SUM(home_runs) as total_hr,
                SUM(rbi) as total_rbi,
                SUM(stolen_bases) as total_sb,
                AVG(batting_avg) as avg_avg
            FROM batting_stats
            WHERE season = %s
        """
        
        # Pitching averages query
        pitching_query = """
            SELECT
                SUM(wins) as total_wins,
                SUM(saves) as total_saves,
                SUM(strikeouts) as total_k,
                AVG(era) as avg_era,
                AVG(whip) as avg_whip
            FROM pitching_stats
            WHERE season = %s
        """
        
        try:
            batting_row = self.execute_query(batting_query, (season,), fetch_all=False)
            pitching_row = self.execute_query(pitching_query, (season,), fetch_all=False)
            
            # Teams per league (estimate)
            teams_per_league = 15
            
            # Combine into single dictionary
            league_avgs = {
                'home_runs': batting_row[0] / teams_per_league if batting_row[0] else 0,
                'rbi': batting_row[1] / teams_per_league if batting_row[1] else 0,
                'stolen_bases': batting_row[2] / teams_per_league if batting_row[2] else 0,
                'batting_avg': batting_row[3] if batting_row[3] else 0,
                'wins': pitching_row[0] / teams_per_league if pitching_row[0] else 0,
                'saves': pitching_row[1] / teams_per_league if pitching_row[1] else 0,
                'strikeouts': pitching_row[2] / teams_per_league if pitching_row[2] else 0,
                'era': pitching_row[3] if pitching_row[3] else 0,
                'whip': pitching_row[4] if pitching_row[4] else 0
            }
            
            return league_avgs
            
        except Exception as e:
            logger.error(f"Error retrieving league averages: {e}")
            return {}
    
    def _generate_recommendations(self, analysis):
        """Generate team improvement recommendations"""
        recommendations = []
        
        # Position recommendations
        if 'OF' in analysis['roster_breakdown'] and analysis['roster_breakdown']['OF'] < 3:
            recommendations.append("Need to add more outfielders")
        
        if 'RP' in analysis['roster_breakdown'] and analysis['roster_breakdown']['RP'] < 2:
            recommendations.append("Add more relievers to compete in saves")
        
        # Category recommendations
        if 'stolen_bases' in analysis['team_weaknesses']:
            recommendations.append("Target speed players to improve stolen bases category")
        
        if 'saves' in analysis['team_weaknesses']:
            recommendations.append("Add closers to improve in the saves category")
        
        if 'era' in analysis['team_weaknesses'] and 'whip' in analysis['team_weaknesses']:
            recommendations.append("Consider streaming only high-quality pitchers to improve ratios")
        
        if 'batting_avg' in analysis['team_weaknesses']:
            recommendations.append("Look for high-average hitters to improve batting average")
        
        # Advanced recommendations based on ratio of strengths to weaknesses
        if len(analysis['team_strengths']) > len(analysis['team_weaknesses']) + 2:
            recommendations.append("Consider trading from your strengths to address weaknesses")
        
        # If team is strong in pitching but weak in hitting or vice versa
        pitching_cats = ['era', 'whip', 'strikeouts', 'wins', 'saves']
        hitting_cats = ['batting_avg', 'home_runs', 'rbi', 'stolen_bases']
        
        pitching_strengths = [cat for cat in analysis['team_strengths'] if cat in pitching_cats]
        hitting_strengths = [cat for cat in analysis['team_strengths'] if cat in hitting_cats]
        
        pitching_weaknesses = [cat for cat in analysis['team_weaknesses'] if cat in pitching_cats]
        hitting_weaknesses = [cat for cat in analysis['team_weaknesses'] if cat in hitting_cats]
        
        if len(pitching_strengths) >= 3 and len(hitting_weaknesses) >= 2:
            recommendations.append("Consider trading pitching for hitting help")
        elif len(hitting_strengths) >= 3 and len(pitching_weaknesses) >= 2:
            recommendations.append("Consider trading hitting for pitching help")
            
        return recommendations