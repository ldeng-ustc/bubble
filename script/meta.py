import os
_current_dir = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR: str = os.path.realpath(os.path.join(_current_dir, ".."))
SCRIPT_DIR: str = os.path.join(PROJECT_DIR, "script")
EXPERIMENTS_DIR: str = os.path.join(PROJECT_DIR, "data/experiments")
WORKS: list[str] = [
    "lsgraph", 
    "graphone", 
    "xpgraph", 
    "bubble",
    "bubble_ordered",
]