# fantasy_baseball/core/database.py
import logging
import os
import psycopg2
from psycopg2.extras import execute_batch, execute_values
from psycopg2.pool import ThreadedConnectionPool
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection parameters
DB_HOST = os.getenv("RDS_HOST")
DB_NAME = os.getenv("RDS_DATABASE")
DB_USER = os.getenv("RDS_USER")
DB_PASSWORD = os.getenv("RDS_PASSWORD")
DB_PORT = "5432"

logger = logging.getLogger("fantasy_baseball")

class DatabaseConnector:
    """Base class for database operations"""
    
    def __init__(self, host=None, database=None, user=None, password=None, port=None):
        """Initialize the database connection pool"""
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.pool = None
        self._init_connection_pool()
    
    def _init_connection_pool(self):
        """Initialize the connection pool"""
        try:
            # Check if we have all required connection parameters
            if not all([self.host, self.database, self.user, self.password]):
                logger.error("Missing database connection parameters")
                print("Database connection error: Missing required connection parameters")
                print("Please check your .env file and ensure it has RDS_HOST, RDS_DATABASE, RDS_USER, and RDS_PASSWORD")
                self.pool = None
                return

            self.pool = ThreadedConnectionPool(
                1, 10,  # min connections, max connections
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port or "5432"
            )
            logger.info("Database connection pool established")
        except Exception as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
    
    def get_connection(self):
        """Get a connection from the pool"""
        if not self.pool:
            self._init_connection_pool()
        return self.pool.getconn()
    
    def release_connection(self, conn):
        """Return a connection to the pool"""
        if self.pool and conn:
            self.pool.putconn(conn)
    
    # In fantasy_baseball/core/database.py

    def execute_query(self, query, params=None, fetch=True, commit=False, fetch_all=True):
        """Execute a query and optionally return results"""
        conn = None
        cursor = None
        results = None

        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query, params)

            if fetch:
                if fetch_all:
                    results = cursor.fetchall()
                else:
                    results = cursor.fetchone()

            if commit:
                conn.commit()

            return results
        except psycopg2.OperationalError as e:
            if conn:
                conn.rollback()
            logger.error(f"Database connection error: {e}")
            raise
        except psycopg2.IntegrityError as e:
            if conn:
                conn.rollback()
            logger.error(f"Database integrity error: {e}")
            raise
        except psycopg2.DatabaseError as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Unexpected error executing query: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.release_connection(conn)
    
    def execute_batch_query(self, query, param_sets, commit=True):
        """Execute a query in batch mode"""
        conn = None
        cursor = None
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            execute_batch(cursor, query, param_sets)
            
            if commit:
                conn.commit()
                
            return cursor.rowcount
        except psycopg2.DatabaseError as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error in batch query: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.release_connection(conn)
    
    def close(self):
        """Close the connection pool"""
        if self.pool:
            try:
                self.pool.closeall()
                logger.info("Connection pool closed")
            except Exception as e:
                logger.debug(f"Error closing pool: {e}")
            finally:
                # Set pool to None to prevent double-closing
                self.pool = None
    
    def __del__(self):
        """Ensure connections are closed when object is deleted"""
        self.close()