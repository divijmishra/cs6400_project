# CS-6400-Project

## **Project Structure**

```
/CS-6400-Project
├── /app                    # Core recommendation engines for Neo4j and MySQL
│   ├── collaborative_recommendation_engine.py  # Neo4j-based recommendation engine
│   └── recommender.py                          # MySQL-based recommendation engine
├── /data                   # Data manipulation files and generated CSVs used for data loading
├── /database               # Database connection and initialization scripts for MySQL and Neo4j
│   └── /mysql              # MySQL loading and similarity calculation
│   │   └── loader.py                           # Load data in MySQL
│   │   └── similarity.py                       # Calculate similarities in MySQL
│   └── /neo4j              # Neo4j loading and similarity calculation
│       └── load.py                             # Load data in Neo4j
│       └── similarity_calculator_no_cache.py   # Calculate similarities in Neo4j
├── /notebooks              # Jupyter notebooks for exploratory data analysis and testing
├── README.md               # Main project README file
├── requirements.txt        # Python dependencies
└── setup_env.py            # Script to setup environment
```

## Clone the repository
If you haven't already cloned the repository, you can clone it directly from the original project or from your fork.
- Clone the original repository
   ```bash
   git clone https://github.com/achaurasia7/CS-6400-Project.git
   cd CS-6400-Project
   ```
- Clone your forked repository
   ```bash
   git clone https://github.com/yourusername/CS-6400-Project.git
   cd CS-6400-Project
   ```

## Switch to a Different Branch
   ```bash
   git checkout branch-name
   ```

## Project Environment Setup

To set up the environment for this project, follow these steps:

#### Option 1: Using setup_env.py Script (Recommended)
1. Run the setup script:
   ```bash
   python setup_env.py
   ```

   The script will:
    - Create a new virtual environment in a folder named db_venv.
    - Install all required dependencies from the requirements.txt file.

2. Run
    ```bash
    pip install -e .
    ```
    This will install the repo as a module, allowing easy imports of functions we define.


#### Option 2: Manual Setup (Without the Script)
1. Create a new virtual environment:
   ```bash
   python -m venv db_venv
   ```

2. Activate the virtual environment:
   - On Windows:
     ```bash
     db_venv\Scripts\activate
     ```
   - On Unix/macOS:
     ```bash
     source db_venv/bin/activate
     ```

3. Install the dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Run
    ```bash
    pip install -e .
    ```
    This will install the repo as a module, allowing easy imports of functions we define.

You should now have the same environment set up with all required dependencies.



## Working on the project
#### Activating the Virtual Environment
Whenever you want to work on the project, make sure to activate the virtual environment:
- On Windows:
    ```bash
    db_venv\Scripts\activate
    ```
- On Unix/macOS:
    ```bash
    source db_venv/bin/activate
    ```

#### Deactivating the Virtual Environment
When you're done working, deactivate the virtual environment by running:

```bash
deactivate
```

#### Updating Dependencies
If new dependencies are added to the project, you can update your requirements.txt file using:
```bash
pip freeze > requirements.txt
```

## Working with databases

#### Retrieving data

Follow the instructions in ```data/README.md``` to fetch data, preprocess it, and create smaller subsets.

#### MySQL - loading data, generating similarities
Follow the instructions in ```database/mysql/README.md``` to load data and generate similarity tables for MySQL.

#### Neo4j - loading data, generating similarities

Follow the instructions in ```database/neo4j/README.md``` to load data and generate similarity tables for Neo4j.

#### Running recommendations

Follow the instructions in ```app/README.md``` to run recommendations using MySQL/Neo4j.

#### Write-Read benchmarks

Follow the instructions in ```benchmarks/README.md``` to perform write-read benchmarks on both databases.
 