# SPDX-License-Identifier: MIT
"""
M2 — Working Capital + PL (skeleton) package export.
Only the WC schedule is produced here. (PL is handled downstream by M3/M4/M7.5B.)
"""
from .runner import run_m2  # re-export
__all__ = ["run_m2"]
