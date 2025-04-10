# example_usage.py
from fantasy_baseball.main import FantasyBaseballManager
import logging
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """Example usage of the Fantasy Baseball Manager"""
    # Create manager
    manager = FantasyBaseballManager()
    
    try:
        # Get all teams
        all_teams = manager.get_all_teams()
        if all_teams:
            print(f"Found {len(all_teams)} teams")
            
            # Use the first team for demonstration
            team_id = all_teams[0]['team_id']
            
            # Get team roster
            team_roster = manager.get_team_roster(team_id)
            
            if team_roster:
                print(f"\nTeam: {team_roster['team_name']} ({team_roster['league_name']})")
                print(f"Owner: {team_roster['owner_name']}")
                print(f"Season: {team_roster['season']}")
                print(f"Players: {len(team_roster['players'])}")
                
                # Print first few players
                print("\nRoster Sample:")
                for i, player in enumerate(team_roster['players'][:5], 1):
                    position = player['fantasy_position']
                    name = player['name']
                    team = player['team']
                    
                    stats = []
                    if 'batting_avg' in player.get('stats', {}):
                        stats = [
                            f"HR: {player['stats'].get('home_runs', 0)}",
                            f"AVG: {player['stats'].get('batting_avg', 0):.3f}"
                        ]
                    elif 'era' in player.get('stats', {}):
                        stats = [
                            f"ERA: {player['stats'].get('era', 0):.2f}",
                            f"WHIP: {player['stats'].get('whip', 0):.2f}"
                        ]
                    
                    print(f"{i}. {position}: {name} ({team}) - {', '.join(stats)}")
                
                # Analyze team
                print("\nAnalyzing team...")
                analysis = manager.analyze_team(team_id)
                
                if analysis:
                    print("\nTeam Analysis:")
                    print(f"Strengths: {', '.join(analysis['team_strengths'])}")
                    print(f"Weaknesses: {', '.join(analysis['team_weaknesses'])}")
                    print("\nRecommendations:")
                    
                    for i, rec in enumerate(analysis['recommended_actions'], 1):
                        print(f"{i}. {rec}")
                
                # Search for available players
                print("\nSearching for available players...")
                available = manager.get_available_players(team_id, limit=5)
                if available:
                    print("\nSample Available Players:")
                    for i, player in enumerate(available, 1):
                        print(f"{i}. {player['full_name']} ({player['team']}, {player['position']})")
                
                # Example of advanced metrics dashboard (uncomment to use)
                # print("\nLoading advanced metrics dashboard...")
                # manager.display_advanced_metrics_dashboard(team_id)
                
                # Example of adding a player to team (uncomment to use)
                # if available:
                #     player_to_add = available[0]
                #     success = manager.add_player_to_team(
                #         team_id, 
                #         player_to_add['player_id'], 
                #         player_to_add['position']
                #     )
                #     if success:
                #         print(f"Added {player_to_add['full_name']} to the team")
        else:
            print("No teams found in the database. Please check your database connection and data.")
            print("You may need to import team data first.")
    
    except Exception as e:
        print(f"\nERROR: {e}")
        print("\nDetailed traceback:")
        traceback.print_exc()
        print("\nTroubleshooting tips:")
        print("1. Check your database connection in .env file")
        print("2. Make sure all tables exist in your database")
        print("3. Verify that your Python environment has all required packages")
    
    finally:
        # Close connections
        print("\nClosing database connections...")
        manager.close()
        print("Done!")

if __name__ == "__main__":
    print("Starting Fantasy Baseball Manager example...")
    main()