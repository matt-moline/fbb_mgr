# run.py
from fantasy_baseball.main import FantasyBaseballManager
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fantasy_baseball_runner")

def main():
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
                print(f"Owner: {team_roster.get('owner_name', 'Unknown')}")
                print(f"Season: {team_roster.get('season', 'Unknown')}")
                
                # Show roster info
                if team_roster.get('players'):
                    print(f"\nRoster: {len(team_roster['players'])} players")
                    
                    # Example: Print first 5 players if any exist
                    for i, player in enumerate(team_roster['players'][:5], 1):
                        print(f"{i}. {player['name']} ({player.get('team', 'N/A')}) - {player.get('fantasy_position', 'N/A')}")
                else:
                    print("\nNo players on roster. Let's add some players...")
                    
                    # Search for some players
                    print("\nSearching for available players...")
                    available_players = manager.get_available_players(team_id, limit=5)
                    
                    if available_players:
                        print(f"Found {len(available_players)} available players")
                        for i, player in enumerate(available_players, 1):
                            print(f"{i}. {player['full_name']} ({player.get('team', 'N/A')}) - {player.get('position', 'N/A')}")
                        
                        # Option to add a player
                        print("\nWould you like to add a player to the team? (y/n)")
                        choice = input().lower()
                        if choice.startswith('y'):
                            print("Enter player number:")
                            player_num = int(input())
                            if 1 <= player_num <= len(available_players):
                                player = available_players[player_num-1]
                                position = player.get('position', 'UTIL')
                                success = manager.add_player_to_team(team_id, player['player_id'], position)
                                if success:
                                    print(f"Added {player['full_name']} to team as {position}")
                    else:
                        print("No available players found")
        else:
            print("No teams found in database")
    
    finally:
        # Close connections
        manager.close()

if __name__ == "__main__":
    main()