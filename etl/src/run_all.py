"""
NHS TRUD Pipeline - Full Entrypoint
Runs download (main_pipeline.py) then build_spine (main_parser.py) sequentially.
"""
import sys

print("=" * 60)
print("Stage 1: Downloading TRUD data...")
print("=" * 60)
from main_pipeline import process_trud
process_trud()

print()
print("=" * 60)
print("Stage 2: Building analytical spine + writing outputs...")
print("=" * 60)
from main_parser import build_spine
build_spine()

print()
print("Pipeline complete.")
