import os
import subprocess
import sys
import venv

def create_venv(venv_dir):
    """Create a virtual environment in the specified directory."""
    if not os.path.exists(venv_dir):
        print(f"Creating virtual environment in {venv_dir}...")
        venv.create(venv_dir, with_pip=True)
    else:
        print(f"Virtual environment already exists in {venv_dir}.")

def install_requirements(venv_dir):
    """Install the dependencies from requirements.txt using pip in the venv."""
    requirements_path = 'requirements.txt'
    
    if not os.path.exists(requirements_path):
        print("requirements.txt not found. Please make sure the file is in the project root.")
        return
    
    # Determine the pip executable path based on the venv
    pip_executable = os.path.join(venv_dir, 'Scripts', 'pip') if sys.platform == "win32" else os.path.join(venv_dir, 'bin', 'pip')
    
    print(f"Installing dependencies from {requirements_path}...")
    
    # Run pip install with subprocess
    subprocess.check_call([pip_executable, 'install', '-r', requirements_path])

def main():
    # Set the name of the virtual environment folder
    venv_dir = 'db_venv'
    
    # Step 1: Create the virtual environment
    create_venv(venv_dir)
    
    # Step 2: Install required packages
    install_requirements(venv_dir)
    
    print("Virtual environment is set up and dependencies installed!")

if __name__ == '__main__':
    main()
