#!/bin/bash
# This script runs during the Render build process

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Initialize database tables
echo "Initializing database tables..."
python init_db.py

echo "Build process completed!"