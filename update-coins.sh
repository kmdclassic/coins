#!/bin/bash

# Script for local updating of coin configs
# Creates a virtual environment, installs dependencies, and runs the config generator

set -e  # Exit on error

VENV_DIR="venv"
REQUIREMENTS_FILE="./utils/requirements.txt"
SCRIPT_FILE="./utils/generate_app_configs.py"

# Check if virtual environment exists, create if not
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
    echo "Virtual environment created successfully."
else
    echo "Virtual environment already exists."
fi

# Activate virtual environment
echo "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

# Upgrade pip to ensure latest version
echo "Upgrading pip..."
pip install --upgrade pip --quiet

# Install dependencies
if [ -f "$REQUIREMENTS_FILE" ]; then
    echo "Installing dependencies from $REQUIREMENTS_FILE..."
    pip install -r "$REQUIREMENTS_FILE" --quiet
    echo "Dependencies installed successfully."
else
    echo "Error: Requirements file not found at $REQUIREMENTS_FILE"
    exit 1
fi

# Run the config generator script
if [ -f "$SCRIPT_FILE" ]; then
    echo "Running coin config generator..."
    python3 "$SCRIPT_FILE"
    echo "Coin configs updated successfully."
else
    echo "Error: Script file not found at $SCRIPT_FILE"
    exit 1
fi

# Deactivate virtual environment
deactivate
echo "Done."