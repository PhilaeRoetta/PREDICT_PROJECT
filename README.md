# Sport Prediction Project
A Python-based software that manages a sport prediction game on forums.  
It runs on Linux (Ubuntu) and can be executed locally or automatically through GitHub Actions.

## Table of Contents
- [Input tools and accounts](#inputtoolsaccounts)
- [High level overview](#overview)
- [Installation](#installation)
- [Usage - Entry points](#usage)
- [GitHub project repository tree](#githubtree)
- [Features](#feature)
- [Snowflake database architecture](#snowflakearchitecture)
- [Documentation](#documentation)

## Input tools and accounts<a name="inputtoolsaccounts"></a>

The software uses the following external tools and account:
- **Snowflake account**, to store predictions and calculations, with at least one user having DML privileges (The software uses its credentials)
- **DBT account**, to run transformations and calculations in Snowflake using sql files.
- **DropBox account**, containing some input files for a successful run, and some program files results in a stable tree. Full details in the manual
- **Gmail account** to send by email the status of the run, if ran through GitHub Actions, with details of run
- **Sport season source accounts** for each season in the scope, a possible account to get games and result
- **Forum accounts** for each forum in the scope, a possible account to read players' predictions, and post prediction template and results

## High level overview<a name="overview"></a>

```
Sources → Python → Snowflake → Python → Forums posts → DropBox (upload)
           ↑         ↑
        Dropbox     DBT
       (download)

```
The workflow:
- Fetch game schedules and results from sport seasons source
- Read predictions from forum topics sources
- Store and compute results in Snowflake, via DBT
- Generate messages, images, and rankings
- Post templates and results back to the forum with Python
- Sync all files through Dropbox for state management

## Installation<a name="installation"></a>
- For installing the project locally:  
    ```
    git clone https://github.com/PhilaeRoetta/PREDICT_PROJECT.git  
    cd PREDICT_PROJECT  
    python3 -m venv predict_env  
    source predict_env/bin/activate  
    pip install -r requirements.txt  
    ```

- For installing rclone locally and link it with DropBox
    ```
    curl https://rclone.org/install.sh | sudo bash
    rclone config
    ```

## Input Parameters<a name="inputparameters"></a>

Set the required parameters (Snowflake, Dropbox, imgbb, Gmail, forum credentials).
Some of them are set on GitHub secrets, others set before running.
See the Full Manual for the complete list.

## Usage - Entry points<a name="usage"></a>

The system exposes several entry points:  
- init competition: to add new competitions to predict from potential new sources
- init snowflake: copy of the snowflake database on a new account
- playoffs table: creates a playoffs bracket with prediction results for playoffs competition
- main entry point: run automatically and manually the daily business  path with reading predictions, updating database, posting results,...

- Typical local manual usage of main entry point
    ```
        cd PYTHON_PREDICT
        python exe_main.py
    ```
- Typical GitHub Actions usage of main entry point through *gitrun_main_auto_prod.yml*  

See more on the full manual.

## GitHub project repository tree<a name="githubtree"></a>

```
PREDICT_PROJECT/
├── .github/
│   └── workflows/
│       └── # yml files to run the program through GitHub Actions
│
├── code_archive/
│   └── # important obsolete code (Python + dbt) for reference
│
├── DBT_PREDICT/
│   └── # DBT project folders and files
│       # includes dbt_project.yml and profiles.yml modified during runtime
|       # also includes yml files for test while running dbt
│
├── file_exemples/
│   └── # files copied from Dropbox as exemple for Section "Files required"
│
├── PYTHON_PREDICT/
│   ├── # all Python modules except those in output_actions/
│   │   # includes SQL queries to adress to Snowflake
│   │
│   ├── output_actions/
│   │   └── # Python modules that prepare messages for forums,
│   │       # capture results, modify templates
│   │       # includes SQL queries to adress to Snowflake
│   │
│   └── tests/
│       └── # tests for every Python module (happy paths + edge cases)
│           # includes material files needed to run them
│
├── manual.md
├── .gitignore
├── README.md
├── readme.txt
└── requirements.txt   # contains Python libraries to install
```

## Features<a name="feature"></a>

- Automatic prediction template posting for players to follow
- Automatic result calculation and posting
- Prediction championship logic between teams'fan of the predicted league 
- MVP [Most Valuable Predictor] election
- Full Prod/Test environment separation
- GitHub Actions automation scheduled
- Snowflake + DBT transformation pipeline

## Snowflake database architecture<a name="snowflakearchitecture"></a>

The program creates two databases, one for production, one for testing

There are three schemas (= layers):
- landing (managed by Python): Stores raw data from manual files, forum messages, and game extractions

- curated (managed by DBT): Performs DML operations such as updating games/competitions and extracting predictions. 

- consumpted (managed by DBT): Contains calculations and normalized tables:
    - dimensions (seasons, competitions, gameday, games, forum and topic)
    - calculations of scores and points (for each prediction, per user and gameday, per user and season, per team for the prediction championship)

To explore DBT documentation:
```
    cd DBT_PREDICT
    dbt docs generate
    dbt docs serve
```

## Documentation<a name="documentation"></a>

A full documentation can be found in the file manual.md

