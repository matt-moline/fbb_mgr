# fantasy_baseball/visualization/data_viz.py
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import ipywidgets as widgets
from IPython.display import display, clear_output
import logging

logger = logging.getLogger("fantasy_baseball")

class VisualizationManager:
    """Handles data visualization for fantasy baseball data"""
    
    def __init__(self):
        """Initialize visualization manager"""
        pass
    
    def display_advanced_metrics_dashboard(self, batter_data, pitcher_data, team_players=None):
        """
        Display advanced metrics dashboard for players
        
        Args:
            batter_data: DataFrame with batter statistics
            pitcher_data: DataFrame with pitcher statistics
            team_players: Optional list of player names to filter by
        """
        try:
            # Make a copy of the data to avoid modifying the original
            batter_data = batter_data.copy()
            pitcher_data = pitcher_data.copy()

            # Determine the name column
            batter_name_col = self._identify_name_column(batter_data)
            pitcher_name_col = self._identify_name_column(pitcher_data)

            # Filter by team players if provided
            if team_players:
                batter_data = batter_data[batter_data[batter_name_col].isin(team_players)]
                pitcher_data = pitcher_data[pitcher_data[pitcher_name_col].isin(team_players)]
                print(f"Showing metrics for {len(batter_data)} batters and {len(pitcher_data)} pitchers on roster")

            # Define metrics based on player type
            batter_metrics = self._identify_batter_metrics(batter_data)
            pitcher_metrics = self._identify_pitcher_metrics(pitcher_data)

            # Create widgets
            player_type = widgets.RadioButtons(
                options=['Batters', 'Pitchers'],
                description='Player Type:',
                disabled=False
            )

            metric_dropdown = widgets.Dropdown(
                options=list(batter_metrics.keys()),
                description='Metric:',
                disabled=False,
            )

            update_button = widgets.Button(
                description='Update Chart',
                button_style='primary',
                tooltip='Click to update the chart'
            )

            output = widgets.Output()
            
        except ValueError as e:
            print(f"Error with metric values: {e}")
            logger.error(f"ValueError in dashboard: {e}")
        
        except KeyError as e:
            print(f"Required key missing from data: {e}")
            logger.error(f"KeyError in dashboard: {e}")
        except Exception as e:
            print(f"An error occurred displaying the dashboard: {e}")
            logger.error(f"Unexpected error in dashboard: {e}")
        
        # Define the update function
        def update_chart(b):
            with output:
                clear_output(wait=True)
                
                # Get current selections
                player_selection = player_type.value
                metric_label = metric_dropdown.value
                
                # Get the right dataset and metric
                if player_selection == 'Batters':
                    df = batter_data
                    metric = batter_metrics[metric_label]
                    name_col = batter_name_col
                else:
                    df = pitcher_data
                    metric = pitcher_metrics[metric_label]
                    name_col = pitcher_name_col
                
                # Check if metric exists
                if metric not in df.columns:
                    print(f"Metric {metric} not found in data columns.")
                    print(f"Available columns: {', '.join(df.columns[:10])}...")
                    return
                
                # Remove NaN values for the selected metric
                df = df.dropna(subset=[metric])
                
                if df.empty:
                    print(f"No data available for {metric_label} metric")
                    return
                
                # Sort and get all players
                ascending = True if player_selection == 'Pitchers' and any(m in metric.lower() for m in ['era', 'xera', 'woba']) else False
                sorted_df = df.sort_values(by=metric, ascending=ascending)
                
                # Create figure
                plt.figure(figsize=(12, 8))
                
                # Define colors based on values
                if not ascending:
                    # For metrics where higher is better
                    norm = plt.Normalize(sorted_df[metric].min(), sorted_df[metric].max())
                    colors = plt.cm.viridis(norm(sorted_df[metric].values))
                else:
                    # For metrics where lower is better
                    norm = plt.Normalize(sorted_df[metric].max(), sorted_df[metric].min())
                    colors = plt.cm.viridis(norm(sorted_df[metric].values))
                
                # If dataset is too large, just show top 20
                display_df = sorted_df.head(20) if len(sorted_df) > 20 else sorted_df
                
                # Create the plot
                plt.barh(range(len(display_df)), display_df[metric], color=colors[:len(display_df)])
                
                # Add labels
                plt.yticks(range(len(display_df)), display_df[name_col])
                plt.title(f'Players by {metric_label}')
                plt.xlabel(metric_label)
                plt.tight_layout()
                
                # Show the plot
                plt.show()
                
                # Identify team column
                team_col = self._identify_team_column(display_df)
                
                # Show the data table
                display_cols = [name_col]
                if team_col:
                    display_cols.append(team_col)
                display_cols.append(metric)
                
                display(display_df[display_cols].reset_index(drop=True))
        
        # Connect the button to the function
        update_button.on_click(update_chart)
        
        # Define what happens when player type changes
        def on_player_type_change(change):
            if change['new'] == 'Batters':
                metric_dropdown.options = list(batter_metrics.keys())
            else:
                metric_dropdown.options = list(pitcher_metrics.keys())
        
        player_type.observe(on_player_type_change, names='value')
        
        # Create layout
        controls = widgets.VBox([player_type, metric_dropdown, update_button])
        dashboard = widgets.HBox([controls, output])
        
        # Display the dashboard
        display(dashboard)
        
        # Trigger initial update
        update_button.click()
    
    def _identify_name_column(self, df):
        """Find the column containing player names"""
        name_candidates = ['player_name', 'name', 'full_name', 'last_name, first_name']
        
        # Check for exact matches first
        for col in name_candidates:
            if col in df.columns:
                return col
        
        # Then look for partial matches
        for col in df.columns:
            if any(candidate.lower() in col.lower() for candidate in name_candidates):
                return col
        
        # If all else fails, return the first column
        return df.columns[0]
    
    def _identify_team_column(self, df):
        """Find the column containing team information"""
        team_candidates = ['team', 'team_name', 'club', 'organization']
        
        # Check for exact matches first
        for col in team_candidates:
            if col in df.columns:
                return col
        
        # Then look for partial matches
        for col in df.columns:
            if any(candidate.lower() in col.lower() for candidate in team_candidates):
                return col
        
        # If no team column found, return None
        return None
    
    def _identify_batter_metrics(self, df):
        """Identify relevant batter metrics in the dataframe"""
        metrics = {}
        
        # Look for barrel rate
        for col in df.columns:
            if 'barrel' in col.lower():
                metrics['Barrel %'] = col
                break
        
        # Look for xwOBA
        for col in df.columns:
            if 'xwoba' in col.lower():
                metrics['xwOBA'] = col
                break
        
        # Look for hard hit %
        for col in df.columns:
            if 'hard_hit' in col.lower() or 'hardhit' in col.lower():
                metrics['Hard Hit %'] = col
                break
        
        # Look for exit velocity
        for col in df.columns:
            if 'exit' in col.lower() and 'velo' in col.lower():
                metrics['Exit Velocity'] = col
                break
        
        # Look for launch angle
        for col in df.columns:
            if 'launch' in col.lower() and 'angle' in col.lower():
                metrics['Launch Angle'] = col
                break
        
        # Look for xBA
        for col in df.columns:
            if 'xba' in col.lower():
                metrics['Expected BA'] = col
                break
        
        # Fall back to default metrics if none found
        if not metrics:
            metrics = {
                'Barrel %': 'barrel_batted_rate', 
                'xwOBA': 'xwoba',
                'Hard Hit %': 'hard_hit_percent',
                'Exit Velocity': 'exit_velocity_avg',
                'Launch Angle': 'launch_angle_avg',
                'Expected BA': 'xba'
            }
        
        return metrics
    
    def _identify_pitcher_metrics(self, df):
        """Identify relevant pitcher metrics in the dataframe"""
        metrics = {}
        
        # Look for xERA
        for col in df.columns:
            if 'xera' in col.lower():
                metrics['xERA'] = col
                break
        
        # Look for xwOBA
        for col in df.columns:
            if 'xwoba' in col.lower():
                metrics['xwOBA (against)'] = col
                break
        
        # Look for barrel rate
        for col in df.columns:
            if 'barrel' in col.lower():
                metrics['Barrel % (against)'] = col
                break
        
        # Look for hard hit %
        for col in df.columns:
            if 'hard_hit' in col.lower() or 'hardhit' in col.lower():
                metrics['Hard Hit % (against)'] = col
                break
        
        # Look for K%
        for col in df.columns:
            if ('k%' in col.lower() or 'k_pct' in col.lower() or 
                'strikeout' in col.lower() and 'pct' in col.lower()):
                metrics['Strikeout %'] = col
                break
        
        # Fall back to default metrics if none found
        if not metrics:
            metrics = {
                'xERA': 'xera',
                'xwOBA (against)': 'xwoba',
                'Barrel % (against)': 'barrel_batted_rate',
                'Hard Hit % (against)': 'hard_hit_percent',
                'Strikeout %': 'k_percent'
            }
        
        return metrics
    
    def plot_team_category_radar(self, team_analysis):
        """
        Create a radar chart showing team category strengths and weaknesses
        
        Args:
            team_analysis: Analysis output from TeamAnalyzer
        """
        categories = list(team_analysis['category_analysis'].keys())
        values = list(team_analysis['category_analysis'].values())
        
        if not categories:
            print("No category data available for radar chart")
            return
            
        # Create radar chart
        angles = np.linspace(0, 2*np.pi, len(categories), endpoint=False).tolist()
        
        # Close the polygon
        values += values[:1]
        angles += angles[:1]
        categories += categories[:1]
        
        # Create figure
        fig, ax = plt.subplots(figsize=(10, 8), subplot_kw=dict(polar=True))
        
        # Plot data
        ax.plot(angles, values, 'o-', linewidth=2)
        ax.fill(angles, values, alpha=0.25)
        
        # Set category labels
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(categories[:-1])
        
        # Set y-axis limits
        ax.set_ylim(0, 10)
        
        # Set chart title
        plt.title(f"Category Analysis: {team_analysis['team_name']}", size=16)
        
        # Add legend for reference
        plt.text(0.05, 0.05, "10 = Excellent\n1 = Poor", transform=plt.gcf().transFigure)
        
        plt.tight_layout()
        plt.show()
    
    def compare_teams(self, teams_data, categories=None):
        """
        Create comparison visualizations for multiple teams
        
        Args:
            teams_data: List of team analysis results from TeamAnalyzer
            categories: Optional list of categories to compare (defaults to all)
        """
        if not teams_data:
            print("No team data provided for comparison")
            return
        
        # Extract team names and category data
        team_names = [t['team_name'] for t in teams_data]
        
        # If no categories specified, use categories from first team
        if not categories:
            categories = list(teams_data[0]['category_analysis'].keys())
        
        # Create a DataFrame for comparison
        comparison_data = []
        for team in teams_data:
            team_data = {'Team': team['team_name']}
            for category in categories:
                if category in team['category_analysis']:
                    team_data[category] = team['category_analysis'][category]
                else:
                    team_data[category] = 0
            comparison_data.append(team_data)
        
        df = pd.DataFrame(comparison_data)
        df = df.set_index('Team')
        
        # Create heatmap
        plt.figure(figsize=(12, len(team_names) * 0.8 + 2))
        
        # Plot heatmap
        im = plt.imshow(df.values, cmap='viridis', aspect='auto')
        
        # Add colorbar
        cbar = plt.colorbar(im)
        cbar.set_label('Rating (1-10)')
        
        # Add labels
        plt.xticks(range(len(categories)), categories, rotation=45, ha='right')
        plt.yticks(range(len(team_names)), team_names)
        
        # Add title
        plt.title('Team Comparison by Category')
        
        # Add values to cells
        for i in range(len(team_names)):
            for j in range(len(categories)):
                text = plt.text(j, i, df.iloc[i, j],
                               ha="center", va="center", color="w" if df.iloc[i, j] < 5 else "black")
        
        plt.tight_layout()
        plt.show()
        
        # Return the DataFrame for further analysis
        return df
    
    def visualize_recommendations(self, team_analysis):
        """
        Create a visual summary of team recommendations
        
        Args:
            team_analysis: Analysis output from TeamAnalyzer
        """
        # Extract strengths, weaknesses and recommendations
        strengths = team_analysis['team_strengths']
        weaknesses = team_analysis['team_weaknesses']
        recommendations = team_analysis['recommended_actions']
        
        # Create figure with 2 columns (strengths/weaknesses and recommendations)
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 8))
        
        # Plot strengths and weaknesses
        ax1.barh(range(len(strengths)), [8] * len(strengths), color='green', alpha=0.7, height=0.4)
        ax1.barh([i + 0.5 for i in range(len(weaknesses))], [8] * len(weaknesses), color='red', alpha=0.7, height=0.4)
        
        # Add labels
        for i, strength in enumerate(strengths):
            ax1.text(0.5, i, f" {strength}", va='center', ha='left', fontsize=10)
            
        for i, weakness in enumerate(weaknesses):
            ax1.text(0.5, i + 0.5, f" {weakness}", va='center', ha='left', fontsize=10)
        
        # Configure first axis
        ax1.set_yticks([])
        ax1.set_xticks([])
        ax1.set_xlim(0, 10)
        ax1.set_ylim(-0.5, max(len(strengths), len(weaknesses)) + 0.5)
        ax1.set_title('Strengths & Weaknesses', fontsize=14)
        
        # Add legend
        ax1.plot([], [], color='green', alpha=0.7, linewidth=10, label='Strengths')
        ax1.plot([], [], color='red', alpha=0.7, linewidth=10, label='Weaknesses')
        ax1.legend(loc='upper right')
        
        # Plot recommendations
        y_pos = range(len(recommendations))
        ax2.barh(y_pos, [1] * len(recommendations), color='blue', alpha=0.3, height=0.6)
        
        # Add recommendation text
        for i, rec in enumerate(recommendations):
            wrapped_text = self._wrap_text(rec, 40)
            ax2.text(0.1, i, wrapped_text, va='center', ha='left', fontsize=10)
        
        # Configure second axis
        ax2.set_yticks([])
        ax2.set_xticks([])
        ax2.set_xlim(0, 2)
        ax2.set_title('Recommended Actions', fontsize=14)
        
        # Add overall title
        plt.suptitle(f"Team Analysis: {team_analysis['team_name']}", fontsize=16)
        
        plt.tight_layout()
        plt.subplots_adjust(top=0.9)
        plt.show()
    
    def _wrap_text(self, text, width):
        """Wrap text to specified width"""
        words = text.split()
        lines = []
        current_line = []
        
        for word in words:
            if len(' '.join(current_line + [word])) <= width:
                current_line.append(word)
            else:
                lines.append(' '.join(current_line))
                current_line = [word]
                
        if current_line:
            lines.append(' '.join(current_line))
            
        return '\n'.join(lines)