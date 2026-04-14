"""
utils/db_connection.py — Shared database connection utilities.

Usage:
    from utils.db_connection import get_connection, run_query

    df = run_query("SELECT * FROM team_info")
"""

import sqlite3
import pandas as pd
from pathlib import Path
import sys

# Allow imports from project root
sys.path.append(str(Path(__file__).parent.parent))
from config import DB_PATH


def get_connection(timeout: int = 30) -> sqlite3.Connection:
    """
    Returns a sqlite3 connection to the NHL database.
    Raises FileNotFoundError if the database does not exist yet.

    Args:
        timeout: Seconds to wait if the database is locked before raising an error.
    """
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found at {DB_PATH}.\n"
            "Run the database setup notebook first: notebooks/00_database_setup.ipynb"
        )
    return sqlite3.connect(DB_PATH, timeout=timeout)


def run_query(sql: str, params: tuple = ()) -> pd.DataFrame:
    """
    Executes a SQL query and returns the result as a pandas DataFrame.

    Args:
        sql:    A SQL query string.
        params: Optional tuple of parameters for parameterized queries.

    Returns:
        pd.DataFrame of query results.

    Example:
        df = run_query(
            "SELECT * FROM game WHERE season = ? AND type = ?",
            (20152016, 'P')
        )
    """
    with get_connection() as conn:
        return pd.read_sql_query(sql, conn, params=params)


def list_tables() -> list[str]:
    """Returns a list of all table names in the database."""
    df = run_query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    return df["name"].tolist()


def table_info(table_name: str) -> pd.DataFrame:
    """Returns column names and types for a given table."""
    return run_query(f"PRAGMA table_info({table_name})")


def create_clean_views() -> None:
    """
    Creates deduplicated SQL views for the tables known to contain duplicate rows.

    Root cause: The Kaggle source dataset included the 2018-2019 and 2019-2020
    seasons twice (from two separate data collection passes). This affects:
    game, game_skater_stats, game_teams_stats, game_goals, game_penalties,
    and game_plays.

    Strategy: For each affected table, create a view named clean_<table> that
    keeps only the first occurrence of each row, partitioned by the table's
    natural primary key. Uses rowid (SQLite's internal row identifier) to pick
    the earliest-inserted copy deterministically.

    These views are what all downstream queries and models should use instead
    of the raw tables.
    """
    VIEWS = {
        "clean_game": """
            CREATE VIEW IF NOT EXISTS clean_game AS
            SELECT * FROM game
            WHERE rowid IN (
                SELECT MIN(rowid) FROM game GROUP BY game_id
            )
            AND type != 'A'
        """,
        "clean_game_skater_stats": """
            CREATE VIEW IF NOT EXISTS clean_game_skater_stats AS
            SELECT s.* FROM game_skater_stats s
            WHERE s.rowid IN (
                SELECT MIN(rowid) FROM game_skater_stats
                GROUP BY game_id, player_id
            )
        """,
        "clean_game_teams_stats": """
            CREATE VIEW IF NOT EXISTS clean_game_teams_stats AS
            SELECT * FROM game_teams_stats
            WHERE rowid IN (
                SELECT MIN(rowid) FROM game_teams_stats
                GROUP BY game_id, team_id
            )
        """,
        "clean_game_goals": """
            CREATE VIEW IF NOT EXISTS clean_game_goals AS
            SELECT * FROM game_goals
            WHERE rowid IN (
                SELECT MIN(rowid) FROM game_goals GROUP BY play_id
            )
        """,
        "clean_game_penalties": """
            CREATE VIEW IF NOT EXISTS clean_game_penalties AS
            SELECT * FROM game_penalties
            WHERE rowid IN (
                SELECT MIN(rowid) FROM game_penalties GROUP BY play_id
            )
        """,
        "clean_game_plays": """
            CREATE VIEW IF NOT EXISTS clean_game_plays AS
            SELECT * FROM game_plays
            WHERE rowid IN (
                SELECT MIN(rowid) FROM game_plays GROUP BY play_id
            )
        """,
    }

    conn = get_connection()
    cursor = conn.cursor()

    print("Creating clean views...\n")
    for view_name, ddl in VIEWS.items():
        try:
            cursor.execute(ddl.strip())
            conn.commit()
            # Verify row count
            count = cursor.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
            print(f"  OK  {view_name:<35} {count:>10,} rows")
        except Exception as e:
            print(f"  FAILED  {view_name:<35} {e}")

    conn.close()
    print("\nDone. All downstream queries should use clean_* views.")


def list_views() -> list[str]:
    """Returns a list of all view names in the database."""
    df = run_query("SELECT name FROM sqlite_master WHERE type='view' ORDER BY name")
    return df["name"].tolist()


def create_database_from_csvs(csv_files: list, db_path: Path = DB_PATH) -> None:
    """
    Loads a list of CSV files into a SQLite database, one table per file.
    Table names are derived from the CSV filename (without extension).
    Prints progress and row counts as each table is loaded.

    Args:
        csv_files:  List of Path objects pointing to CSV files.
        db_path:    Destination path for the SQLite database file.
                    Defaults to DB_PATH from config.py.

    Example:
        from pathlib import Path
        csvs = list(Path("data/raw").glob("*.csv"))
        create_database_from_csvs(csvs)
    """
    missing = [f for f in csv_files if not Path(f).exists()]
    if missing:
        raise FileNotFoundError(
            f"The following CSV files were not found:\n" +
            "\n".join(f"  - {f}" for f in missing)
        )

    print(f"Building database at: {db_path}\n")
    conn = sqlite3.connect(db_path, timeout=30)

    for csv_path in csv_files:
        csv_path = Path(csv_path)
        table_name = csv_path.stem
        try:
            df = pd.read_csv(csv_path, low_memory=False)
            df.to_sql(name=table_name, con=conn, if_exists="replace", index=False,
                      method="multi", chunksize=1000)
            conn.commit()
            print(f"  OK  {table_name:<30} {len(df):>10,} rows")
        except Exception as e:
            print(f"  FAILED  {table_name:<30} {e}")

    conn.close()
    print(f"\nDone. Database saved to {db_path}")