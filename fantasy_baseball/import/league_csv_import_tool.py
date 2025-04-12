# league_csv_import_tool.py

import os
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from nfbc_manual_import import NFBCManualImporter

class LeagueCsvImportTool:
    def __init__(self, root):
        self.root = root
        self.root.title("NFBC League CSV Import Tool")
        self.root.geometry("600x400")
        
        self.importer = NFBCManualImporter()
        
        # Create widgets
        self.create_widgets()
        
    def create_widgets(self):
        # CSV file selection
        ttk.Label(self.root, text="League Players CSV File:").grid(row=0, column=0, sticky='w', padx=10, pady=10)
        self.csv_file_path = tk.StringVar()
        ttk.Entry(self.root, textvariable=self.csv_file_path, width=40).grid(row=0, column=1, padx=5, pady=10)
        ttk.Button(self.root, text="Browse", command=self.browse_csv_file).grid(row=0, column=2, padx=5, pady=10)
        
        # League ID or Name
        ttk.Label(self.root, text="League ID:").grid(row=1, column=0, sticky='w', padx=10, pady=10)
        self.league_id = tk.StringVar()
        ttk.Entry(self.root, textvariable=self.league_id, width=40).grid(row=1, column=1, padx=5, pady=10)
        
        # Your Team ID
        ttk.Label(self.root, text="Your Team ID:").grid(row=2, column=0, sticky='w', padx=10, pady=10)
        self.team_id = tk.StringVar(value="662833")  # Default from your example
        ttk.Entry(self.root, textvariable=self.team_id, width=40).grid(row=2, column=1, padx=5, pady=10)
        
        # Import button
        ttk.Button(self.root, text="Import League CSV", command=self.import_csv).grid(row=3, column=1, pady=20)
        
        # Status label
        self.status_var = tk.StringVar(value="Ready to import")
        ttk.Label(self.root, textvariable=self.status_var).grid(row=4, column=0, columnspan=3, pady=10)
        
        # Free Agents section
        ttk.Label(self.root, text="After importing, you can:").grid(row=5, column=0, columnspan=3, sticky='w', padx=10, pady=5)
        ttk.Button(self.root, text="View Free Agents", command=self.view_free_agents).grid(row=6, column=0, padx=10, pady=5)
        ttk.Button(self.root, text="Project Standings", command=self.project_standings).grid(row=6, column=1, padx=10, pady=5)
        
    def browse_csv_file(self):
        filename = filedialog.askopenfilename(
            initialdir=os.getcwd(),
            title="Select League CSV File",
            filetypes=(("CSV files", "*.csv"), ("All files", "*.*"))
        )
        if filename:
            self.csv_file_path.set(filename)
            
    def import_csv(self):
        file_path = self.csv_file_path.get()
        if not file_path:
            messagebox.showwarning("Warning", "Please select a CSV file")
            return
            
        league_id = self.league_id.get()
        if not league_id:
            league_id = None  # Make optional
            
        team_id = self.team_id.get()
        if not team_id:
            messagebox.showwarning("Warning", "Please enter your team ID")
            return
            
        try:
            # Update status
            self.status_var.set("Importing... Please wait")
            self.root.update()
            
            # Import the CSV
            success = self.importer.import_league_players_csv(file_path, league_id, team_id)
            
            if success:
                self.status_var.set("Import successful!")
                messagebox.showinfo("Success", "League CSV data imported successfully")
            else:
                self.status_var.set("Import failed")
                messagebox.showerror("Error", "Failed to import league CSV data")
        except Exception as e:
            self.status_var.set(f"Error: {str(e)}")
            messagebox.showerror("Error", f"An error occurred: {str(e)}")
            
    def view_free_agents(self):
        league_id = self.league_id.get()
        if not league_id:
            messagebox.showwarning("Warning", "Please enter a league ID")
            return
            
        try:
            # Get free agents
            free_agents = self.importer.identify_free_agents(league_id)
            
            if free_agents:
                # Show in a new window
                free_agent_window = tk.Toplevel(self.root)
                free_agent_window.title("Free Agents")
                free_agent_window.geometry("800x600")
                
                # Create a treeview (table) to display the free agents
                columns = ('Name', 'Team', 'Position')
                tree = ttk.Treeview(free_agent_window, columns=columns, show='headings')
                
                # Set column headings
                for col in columns:
                    tree.heading(col, text=col)
                    tree.column(col, width=100)
                
                # Add data to the treeview
                for agent in free_agents:
                    tree.insert('', tk.END, values=(agent['name'], agent['team'], agent['position']))
                
                # Add a scrollbar
                scrollbar = ttk.Scrollbar(free_agent_window, orient=tk.VERTICAL, command=tree.yview)
                tree.configure(yscroll=scrollbar.set)
                
                # Position the treeview and scrollbar
                tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
                scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            else:
                messagebox.showinfo("Info", "No free agents found")
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred: {str(e)}")
            
    def project_standings(self):
        league_id = self.league_id.get()
        if not league_id:
            messagebox.showwarning("Warning", "Please enter a league ID")
            return
            
        try:
            # Get projected standings
            projections = self.importer.project_team_standings(league_id)
            
            if projections and 'standings' in projections:
                # Show in a new window
                standings_window = tk.Toplevel(self.root)
                standings_window.title("Projected Standings")
                standings_window.geometry("800x600")
                
                # Create a treeview (table) to display the standings
                columns = ('Team', 'Total', 'AVG', 'HR', 'R', 'RBI', 'SB', 'ERA', 'WHIP', 'K', 'W', 'SV')
                tree = ttk.Treeview(standings_window, columns=columns, show='headings')
                
                # Set column headings
                for col in columns:
                    tree.heading(col, text=col)
                    tree.column(col, width=70)
                
                # Add data to the treeview
                for team_name, stats in projections['standings']:
                    values = [team_name, stats['total']]
                    
                    # Add category points
                    for cat in ['avg', 'hr', 'r', 'rbi', 'sb', 'era', 'whip', 'k', 'w', 'sv']:
                        values.append(stats.get(cat, 0))
                    
                    tree.insert('', tk.END, values=values)
                
                # Add a scrollbar
                scrollbar = ttk.Scrollbar(standings_window, orient=tk.VERTICAL, command=tree.yview)
                tree.configure(yscroll=scrollbar.set)
                
                # Position the treeview and scrollbar
                tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
                scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            else:
                messagebox.showinfo("Info", "Could not generate projected standings")
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred: {str(e)}")
            
    def on_closing(self):
        """Cleanup when closing the application"""
        self.importer.close()
        self.root.destroy()

if __name__ == "__main__":
    root = tk.Tk()
    app = LeagueCsvImportTool(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()