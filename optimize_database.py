import sqlite3
import argparse
import logging
import os
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def add_indexes(db_path):
    """Add indexes to improve query performance"""
    try:
        logger.info(f"Adding indexes to database: {db_path}")
        
        # Connect to database
        conn = sqlite3.connect(db_path, timeout=30.0)
        cursor = conn.cursor()
        
        # Add indexes for common query patterns
        indexes = [
            # For expiration date queries
            "CREATE INDEX IF NOT EXISTS idx_options_greeks_expiration_date ON Options_Greeks(expiration_date)",
            
            # For strike price queries
            "CREATE INDEX IF NOT EXISTS idx_options_greeks_strike ON Options_Greeks(K)",
            
            # For option type queries
            "CREATE INDEX IF NOT EXISTS idx_options_greeks_option_type ON Options_Greeks(option_type)",
            
            # Composite index for common filters
            "CREATE INDEX IF NOT EXISTS idx_options_greeks_expiration_strike ON Options_Greeks(expiration_date, K)",
            
            # For sorting by expiration and strike
            "CREATE INDEX IF NOT EXISTS idx_options_greeks_sort ON Options_Greeks(expiration_date, K, option_type)",
            
            # For time-based queries (if time column exists)
            "CREATE INDEX IF NOT EXISTS idx_options_greeks_time ON Options_Greeks(time) WHERE time IS NOT NULL"
        ]
        
        for index_sql in indexes:
            try:
                logger.info(f"Creating index: {index_sql}")
                cursor.execute(index_sql)
                logger.info("✓ Index created successfully")
            except sqlite3.Error as e:
                logger.warning(f"Could not create index: {e}")
        
        # Commit changes
        conn.commit()
        conn.close()
        
        logger.info("Index creation completed!")
        return True
        
    except Exception as e:
        logger.error(f"Error adding indexes: {e}")
        return False

def optimize_settings(db_path):
    """Optimize database settings for better performance"""
    try:
        logger.info(f"Optimizing database settings: {db_path}")
        
        # Connect to database
        conn = sqlite3.connect(db_path, timeout=30.0)
        cursor = conn.cursor()
        
        # Optimize settings
        optimizations = [
            "PRAGMA journal_mode=WAL",
            "PRAGMA synchronous=NORMAL",
            "PRAGMA cache_size=-10000",  # 10MB cache
            "PRAGMA temp_store=MEMORY",
            "PRAGMA mmap_size=268435456"  # 256MB memory mapping
        ]
        
        for opt in optimizations:
            try:
                logger.info(f"Applying setting: {opt}")
                cursor.execute(opt)
                result = cursor.fetchone()
                if result:
                    logger.info(f"Setting result: {result}")
            except sqlite3.Error as e:
                logger.warning(f"Could not apply setting: {e}")
        
        # Commit changes
        conn.commit()
        conn.close()
        
        logger.info("Database settings optimized!")
        return True
        
    except Exception as e:
        logger.error(f"Error optimizing settings: {e}")
        return False

def vacuum_database(db_path):
    """Run VACUUM to defragment the database"""
    try:
        logger.info(f"Vacuuming database: {db_path}")
        
        # Connect to database
        conn = sqlite3.connect(db_path, timeout=60.0)
        cursor = conn.cursor()
        
        # Run VACUUM
        cursor.execute("VACUUM")
        
        # Commit changes
        conn.commit()
        conn.close()
        
        logger.info("Database vacuum completed!")
        return True
        
    except Exception as e:
        logger.error(f"Error vacuuming database: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Optimize SQLite database for better performance')
    parser.add_argument('--db-path', required=True, help='Path to database file')
    parser.add_argument('--add-indexes', action='store_true', help='Add indexes to improve query performance')
    parser.add_argument('--optimize-settings', action='store_true', help='Optimize database settings')
    parser.add_argument('--vacuum', action='store_true', help='Vacuum the database to defragment it')
    
    args = parser.parse_args()
    
    # Validate database path
    if not os.path.exists(args.db_path):
        logger.error(f"Database file not found: {args.db_path}")
        return
    
    logger.info(f"Starting database optimization for: {args.db_path}")
    
    # Perform requested optimizations
    if args.add_indexes:
        add_indexes(args.db_path)
    
    if args.optimize_settings:
        optimize_settings(args.db_path)
    
    if args.vacuum:
        vacuum_database(args.db_path)
    
    logger.info("Database optimization completed!")

if __name__ == "__main__":
    main()