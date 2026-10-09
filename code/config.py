import json
import os
import sys
import pyspark
from pathlib import Path

# --- Automatically set Windows Hadoop utilities for PySpark ---
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"

# Where to save the user's answers so we don't ask again
PROJECT_ROOT = Path(__file__).resolve().parent
CONFIG_FILE = PROJECT_ROOT / "local_settings.json"

def get_mongo_connector() -> str:
    """Dynamically select the Mongo connector based on the installed PySpark version."""
    # Get the installed PySpark version (e.g., "3.5.0")
    spark_version = pyspark.__version__
    major_minor = ".".join(spark_version.split(".")[:2])
    
    # Extract just the major.minor parts (e.g., "3.5")
    major_minor = ".".join(spark_version.split(".")[:2])
    
    # Match the Spark version to the correct official MongoDB connector
    if major_minor == "3.5":
        return "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"
    elif major_minor == "3.4":
        return "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
    elif major_minor == "3.3":
        return "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0"
    else:
        # Graceful exit with clear installation instructions
        print("\n" + "="*65)
        print(f"❌ ERROR: UNSUPPORTED PYSPARK VERSION ({spark_version})")
        print("="*65)
        print("This project requires PySpark version 3.3, 3.4, or 3.5 to safely")
        print("connect to MongoDB.")
        print("\nPlease update your environment by running the following command")
        print("in your terminal:")
        print("\n    conda install -c conda-forge pyspark=3.5 -y\n")
        print("="*65 + "\n")
        sys.exit(1)  # Halts the script immediately

def get_or_create_settings():
    """Loads existing settings, or prompts the user if they don't exist yet."""
    # 1. If the file exists, silently load it
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
            
    # 2. If the file DOES NOT exist, prompt the user
    print("\n=== First Time Setup ===")
    print("This looks like your first time running the pipeline.")
    
    path_input = input("Please enter the full path to your dataset folder: ").strip()
    path_input = path_input.replace('"', '').replace("'", "") # Clean up quotes
    
    raw_col = input("Enter name for Raw Readings (default: 'readings_raw'): ").strip() or "readings_raw"
    meta_col = input("Enter name for Schools Metadata (default: 'schools_metadata'): ").strip() or "schools_metadata"
    clean_col = input("Enter name for Clean Readings (default: 'readings_clean'): ").strip() or "readings_clean"
    
    settings = {
        "DATA_DIR": path_input,
        "COL_RAW": raw_col,
        "COL_META": meta_col,
        "COL_CLEAN": clean_col
    }
    
    # 3. Save the answers so we never have to ask again
    with open(CONFIG_FILE, "w") as f:
        json.dump(settings, f, indent=4)
        
    print(f"-> Settings saved to {CONFIG_FILE.name}! Starting process...\n")
    return settings

# --- Load the Settings ------------------------------------------------------ #
settings = get_or_create_settings()

DATA_DIR = Path(settings["DATA_DIR"])
COL_RAW = settings["COL_RAW"]
COL_META = settings["COL_META"]
COL_CLEAN = settings["COL_CLEAN"]

# --- Filesystem paths ------------------------------------------------------- #
OUTPUT_DIR = PROJECT_ROOT / "output"
AGG_DIR = OUTPUT_DIR / "aggregates"
TABLEAU_DIR = OUTPUT_DIR / "tableau"

# Create the directories if they don't exist
# parents=True creates the output folder if it's missing
# exist_ok=True ignores the command if the folder is already there
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
AGG_DIR.mkdir(parents=True, exist_ok=True)
TABLEAU_DIR.mkdir(parents=True, exist_ok=True)

# --- MongoDB ---------------------------------------------------------------- #
MONGO_HOST = "localhost"
MONGO_PORT = 27017
MONGO_DB = "energy_schools"
MONGO_URI = f"mongodb://{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}"

# --- Spark <-> Mongo connector --------------------------------------------- #
SPARK_PACKAGES = get_mongo_connector()

# --- Analysis parameters ---------------------------------------------------- #
ANALYSIS_YEAR = 2023              
NIGHT_HOURS = (1, 4)              
TOP_N_PEAKS = 20                  
TOP_N_CONSUMERS = 10