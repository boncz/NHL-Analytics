"""
utils/profiler.py — Reusable data profiling utility.

Usage:
    from utils.profiler import DataProfiler

    df = run_query("SELECT * FROM game")
    p = DataProfiler(df, name="game")
    p.summary()           # full profile in one call
    p.null_report()       # detailed null breakdown
    p.categorical(col)    # value counts for a column
    p.date_range(col)     # min / max for a datetime column
"""

import pandas as pd
import numpy as np


class DataProfiler:
    """
    Profiles a pandas DataFrame and prints structured reports.
    Designed for reuse across all tables in the NHL analytics project.

    Args:
        df:     The DataFrame to profile.
        name:   Optional label used in report headers (e.g. table name).
    """

    def __init__(self, df: pd.DataFrame, name: str = ""):
        self.df = df
        self.name = name
        self._header = f"[ {name} ]" if name else "[DataFrame]"

    # ── Public API ────────────────────────────────────────────────────────────

    def summary(self) -> None:
        """Runs all standard checks and prints a full profile."""
        self._section("PROFILE SUMMARY")
        self._shape()
        self._dtypes()
        self.null_report()
        self.duplicate_report()
        self.numeric_ranges()

    def null_report(self) -> None:
        """Reports null counts and percentages for every column."""
        self._section("NULL REPORT")
        null_counts = self.df.isnull().sum()
        null_pct = (null_counts / len(self.df) * 100).round(2)
        report = pd.DataFrame({
            "null_count": null_counts,
            "null_pct": null_pct
        })
        has_nulls = report[report["null_count"] > 0]

        if has_nulls.empty:
            print("  No nulls found.")
        else:
            print(f"  {len(has_nulls)} column(s) with nulls:\n")
            print(has_nulls.to_string())

        total_null = null_counts.sum()
        total_cells = self.df.size
        print(f"\n  Total null cells: {total_null:,} / {total_cells:,} "
              f"({total_null / total_cells * 100:.2f}%)")

    def duplicate_report(self, subset: list = None) -> None:
        """
        Reports duplicate rows. Optionally pass a subset of columns
        to check for key-level duplicates (e.g. ['game_id', 'player_id']).
        """
        self._section("DUPLICATE REPORT")
        n_dupes = self.df.duplicated(subset=subset).sum()
        scope = f"columns {subset}" if subset else "all columns"
        if n_dupes == 0:
            print(f"  No duplicate rows found ({scope}).")
        else:
            print(f"  {n_dupes:,} duplicate row(s) found ({scope}).")
            print("  Sample duplicates:")
            print(self.df[self.df.duplicated(subset=subset, keep=False)]
                  .head(4).to_string())

    def numeric_ranges(self) -> None:
        """Prints min, max, mean, and std for all numeric columns."""
        self._section("NUMERIC RANGES")
        num_cols = self.df.select_dtypes(include=[np.number])
        if num_cols.empty:
            print("  No numeric columns.")
            return
        stats = num_cols.agg(["min", "max", "mean", "std"]).T.round(3)
        print(stats.to_string())

    def categorical(self, col: str, top_n: int = 10) -> None:
        """
        Prints value counts for a categorical column.

        Args:
            col:    Column name to inspect.
            top_n:  How many top values to show (default 10).
        """
        self._section(f"VALUE COUNTS — {col}")
        if col not in self.df.columns:
            print(f"  Column '{col}' not found.")
            return
        counts = self.df[col].value_counts(dropna=False).head(top_n)
        pcts = (counts / len(self.df) * 100).round(2)
        report = pd.DataFrame({"count": counts, "pct": pcts})
        print(f"  Unique values: {self.df[col].nunique():,}  "
              f"(showing top {min(top_n, len(counts))})\n")
        print(report.to_string())

    def date_range(self, col: str) -> None:
        """
        Prints the min and max of a datetime or string date column.

        Args:
            col: Column name containing dates.
        """
        self._section(f"DATE RANGE — {col}")
        if col not in self.df.columns:
            print(f"  Column '{col}' not found.")
            return
        try:
            parsed = pd.to_datetime(self.df[col], errors="coerce")
            print(f"  Min : {parsed.min()}")
            print(f"  Max : {parsed.max()}")
            nulls = parsed.isnull().sum()
            if nulls:
                print(f"  Unparseable values: {nulls:,}")
        except Exception as e:
            print(f"  Could not parse dates: {e}")

    def sample(self, n: int = 5) -> pd.DataFrame:
        """Returns n sample rows."""
        return self.df.head(n)

    # ── Private helpers ───────────────────────────────────────────────────────

    def _section(self, title: str) -> None:
        bar = "=" * 55
        print(f"\n{bar}")
        print(f"  {self._header}  {title}")
        print(f"{bar}")

    def _shape(self) -> None:
        rows, cols = self.df.shape
        print(f"\n  Rows   : {rows:,}")
        print(f"  Columns: {cols}")

    def _dtypes(self) -> None:
        self._section("COLUMN TYPES")
        dtype_counts = self.df.dtypes.value_counts()
        print(f"  {dict(dtype_counts)}\n")
        print(self.df.dtypes.to_string())
