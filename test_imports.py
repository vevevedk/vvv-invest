import os
import sys

# Add the current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

try:
    from collectors.tasks import run_darkpool_collector
    print("Successfully imported run_darkpool_collector")
except ImportError as e:
    print(f"Import error: {e}")
    print(f"Python path: {sys.path}")
    print(f"Current directory: {current_dir}")
    print(f"Directory contents: {os.listdir(current_dir)}")
    print(f"Collectors directory contents: {os.listdir(os.path.join(current_dir, 'collectors'))}") 