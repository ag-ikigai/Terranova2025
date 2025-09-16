"""
Thin wrapper so older import styles won't break immediately.
"""
from .runner import run_m3 as run

__all__ = ["run"]
