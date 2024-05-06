import streamlit as st
import pandas as pd
from pymongo import MongoClient
import findspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import min as _min, max as _max
import uuid
from pyspark.sql import functions as F


# MongoDB setup
client = MongoClient("mongodb+srv://shoon9525:WmOMn1vTg65pnXID@cluster0.iaom9xh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client["NBA2023"]
collection = db["newCol"]

# Spark setup
findspark.init()
spark = SparkSession.builder.appName("NBA Player Stats Spark").getOrCreate()

st.set_page_config(layout="wide")
st.title('NBA Player Stats Explorer for 2023')

def load_data_mongo():
    results = collection.find({})
    return pd.DataFrame(list(results))

def load_data_spark():
    # Load and return as PySpark DataFrame, not converting to Pandas
    df = spark.read.csv("part-00000-1126c397-bc9b-435a-9de9-555548b6bef6-c000.csv", header=True, inferSchema=True).dropDuplicates(['NAME'])
    return df  # Return Spark DataFrame

playerstats_spark = load_data_spark()  # Load or initialize your Spark DataFrame here

def display_stats(playerstats, columns):
    if isinstance(playerstats, pd.DataFrame):
        # Handling Pandas DataFrame
        num_rows = playerstats.shape[0]
        num_columns = playerstats.shape[1]
    else:
        # Handling PySpark DataFrame
        num_rows = playerstats.count()
        num_columns = len(playerstats.columns)

    st.header('Display Player Stats of Selected Team(s) and Filters')
    st.write('Data Dimension: {} rows and {} columns.'.format(num_rows, num_columns))
    if isinstance(playerstats, pd.DataFrame):
        st.dataframe(playerstats[columns])
    else:
        # Converting to Pandas for displaying in Streamlit, if it's small enough
        st.dataframe(playerstats.select(columns).toPandas())


def handle_player_data(source):
    if source == 'Basic Stats':
        with st.form("player_data_mongo"):
            # Collect all basic stat inputs
            name = st.text_input("Player Name")
            team = st.text_input("Team")
            pos = st.text_input("Position")
            age = st.number_input("Age", min_value=18, max_value=40)
            gp = st.number_input("Games Played")
            mpg = st.number_input("Minutes Per Game")
            fta = st.number_input("Free Throws Attempted")
            ft_pct = st.number_input("Free Throw Percentage")
            twopa = st.number_input("Two-Point Attempts")
            twop_pct = st.number_input("Two-Point Percentage")
            threepa = st.number_input("Three-Point Attempts")
            threep_pct = st.number_input("Three-Point Percentage")
            ppg = st.number_input("Points Per Game")
            rpg = st.number_input("Rebounds Per Game")
            apg = st.number_input("Assists Per Game")
            spg = st.number_input("Steals Per Game")
            bpg = st.number_input("Blocks Per Game")
            tpg = st.number_input("Turnovers Per Game")
            
            submit = st.form_submit_button("Add Player to MongoDB")
            if submit:
                new_data = {
                    "_id": name,
                    "TEAM": team,
                    "POS": pos,
                    "AGE": age,
                    "GP": gp,
                    "MPG": mpg,
                    "FTA": fta,
                    "FT%": ft_pct,
                    "2PA": twopa,
                    "2P%": twop_pct,
                    "3PA": threepa,
                    "3P%": threep_pct,
                    "PPG": ppg,
                    "RPG": rpg,
                    "APG": apg,
                    "SPG": spg,
                    "BPG": bpg,
                    "TPG": tpg
                }
                collection.insert_one(new_data)
                st.success("Player added to MongoDB successfully!")

            delete_name = st.text_input("Delete Player Name")
            if st.form_submit_button("Delete Player from MongoDB"):
                result = collection.delete_one({'_id': delete_name})
                if result.deleted_count > 0:
                    st.success(f"Player {delete_name} deleted successfully.")
                else:
                    st.error("No player found with that name.")
    else:
        handle_advanced_data(spark, playerstats_spark)


def handle_advanced_data(spark, playerstats_spark, csv_path = "part-00000-1126c397-bc9b-435a-9de9-555548b6bef6-c000.csv"):
    form_key = "add_player_to_spark"
    with st.form(form_key):
        # Input fields for player data
        new_player_data = {
            "NAME": st.text_input("Player Name"),
            "TEAM": st.text_input("Team"),
            "POS": st.text_input("Position"),
            "EFG": st.number_input("Effective Field Goal Percentage", format="%.2f"),
            "TS": st.number_input("True Shooting Percentage", format="%.2f"),
            "USG": st.number_input("Usage Percentage", format="%.2f"),
            "TO": st.number_input("Turnover Percentage", format="%.2f"),
            "P+R": st.number_input("Points + Rebounds"),
            "P+A": st.number_input("Points + Assists"),
            "P+R+A": st.number_input("Points + Rebounds + Assists"),
            "VI": st.number_input("Versatility Index"),
            "ORtg": st.number_input("Offensive Rating"),
            "DRtg": st.number_input("Defensive Rating")
        }

        submit = st.form_submit_button("Add Player")
        if submit:
            # Add data to Spark DataFrame
            new_row = spark.createDataFrame([Row(**new_player_data)])
            playerstats_spark = playerstats_spark.union(new_row)
            st.success("Player added to Spark successfully!")

            # Write the updated DataFrame back to the CSV
            playerstats_spark.write.format("csv").mode("overwrite").option("header", "true").save(csv_path)
            st.success("Changes saved to CSV successfully!")

            return playerstats_spark
    
    form_key = "delete_player_from_spark"
    with st.form(form_key):
        delete_name = st.text_input("Enter Player Name to Delete")
        delete_button = st.form_submit_button("Delete Player")

        if delete_button:
            initial_count = playerstats_spark.count()
            # Ensure you match the DataFrame column correctly; assuming it's "NAME"
            playerstats_spark = playerstats_spark.filter(playerstats_spark.NAME != delete_name)
            final_count = playerstats_spark.count()

            if initial_count > final_count:
                st.success("Player deleted successfully!")
                # Write the updated DataFrame back to the CSV
                playerstats_spark.write.format("csv").mode("overwrite").option("header", "true").save(csv_path)
                st.success("Changes saved to CSV successfully!")
            else:
                st.error("Player not found.")

            return playerstats_spark

    
def search_and_display_sparkplayer_by_id(playerstats_spark):
    search_id = st.text_input("Search Player by ID (Spark)")
    
    if search_id:
        # Correctly reference the column 'NAME' in the filter condition
        player_data = playerstats_spark.filter(playerstats_spark.NAME == search_id).collect()
        
        if player_data:
            # Format and display the found player data
            st.write("Player Found:", player_data)
        else:
            st.error("No player found with that ID in Spark DataFrame.")


def advanced_filters(playerstats_spark):
    # Define advanced stats and display sliders for ranges
    advanced_stats = ['eFG%', 'TS%', 'USG%', 'TO%', 'P+R', 'P+A', 'P+R+A', 'VI', 'ORtg', 'DRtg']
    numeric_filters = {}
    
    # Selecting unique values for NAME, TEAM, and POS
    names = [row.NAME for row in playerstats_spark.select("NAME").distinct().collect()]
    teams = [row.TEAM for row in playerstats_spark.select("TEAM").distinct().collect()]
    positions = [row.POS for row in playerstats_spark.select("POS").distinct().collect()]

    # Multi-select for NAME, TEAM, and POS
    selected_names = st.sidebar.multiselect("Filter by Player Name", names)
    selected_teams = st.sidebar.multiselect("Filter by Team", teams)
    selected_positions = st.sidebar.multiselect("Filter by Position", positions)
    
    # Applying filters for NAME, TEAM, and POS
    if selected_names:
        playerstats_spark = playerstats_spark.filter(playerstats_spark.NAME.isin(selected_names))
    if selected_teams:
        playerstats_spark = playerstats_spark.filter(playerstats_spark.TEAM.isin(selected_teams))
    if selected_positions:
        playerstats_spark = playerstats_spark.filter(playerstats_spark.POS.isin(selected_positions))
    
    # Display sliders for numeric stats
    for stat in advanced_stats:
        min_val, max_val = playerstats_spark.select(F.min(stat), F.max(stat)).first()
        # Use a range slider to get a tuple of (min_value, max_value)
        value_range = st.sidebar.slider(f"Filter by {stat}", float(min_val), float(max_val), (float(min_val), float(max_val)))
        numeric_filters[stat] = value_range
        
        # Apply filter based on the range selected by the user
        playerstats_spark = playerstats_spark.filter((F.col(stat) >= value_range[0]) & (F.col(stat) <= value_range[1]))
    
    # Ensure only relevant columns are displayed
    displayed_columns = ['NAME', 'TEAM', 'POS'] + advanced_stats
    display_stats(playerstats_spark, displayed_columns)



def basic_filters(playerstats):
    # Adding sliders for statistical filters
    min_age, max_age = int(playerstats['AGE'].min()), int(playerstats['AGE'].max())
    age_slider = st.sidebar.slider("Filter by Age", min_age, max_age, (min_age, max_age))

    min_ppg, max_ppg = float(playerstats['PPG'].min()), float(playerstats['PPG'].max())
    ppg_slider = st.sidebar.slider("Filter by Points Per Game", min_ppg, max_ppg, (min_ppg, max_ppg))

    min_rpg, max_rpg = float(playerstats['RPG'].min()), float(playerstats['RPG'].max())
    rpg_slider = st.sidebar.slider("Filter by Rebounds Per Game", min_rpg, max_rpg, (min_rpg, max_rpg))

    min_apg, max_apg = float(playerstats['APG'].min()), float(playerstats['APG'].max())
    apg_slider = st.sidebar.slider("Filter by Assists Per Game", min_apg, max_apg, (min_apg, max_apg))

    # Adding sliders for Steals, Blocks, and Turnovers
    min_spg, max_spg = float(playerstats['SPG'].min()), float(playerstats['SPG'].max())
    spg_slider = st.sidebar.slider("Filter by Steals Per Game", min_spg, max_spg, (min_spg, max_spg))

    min_bpg, max_bpg = float(playerstats['BPG'].min()), float(playerstats['BPG'].max())
    bpg_slider = st.sidebar.slider("Filter by Blocks Per Game", min_bpg, max_bpg, (min_bpg, max_bpg))

    min_tpg, max_tpg = float(playerstats['TPG'].min()), float(playerstats['TPG'].max())
    tpg_slider = st.sidebar.slider("Filter by Turnovers Per Game", min_tpg, max_tpg, (min_tpg, max_tpg))

    # Adding sliders for Games Played, Minutes Per Game, Free Throws Attempted, Two-Point and Three-Point Attempts
    min_gp, max_gp = int(playerstats['GP'].min()), int(playerstats['GP'].max())
    gp_slider = st.sidebar.slider("Filter by Games Played", min_gp, max_gp, (min_gp, max_gp))

    min_mpg, max_mpg = float(playerstats['MPG'].min()), float(playerstats['MPG'].max())
    mpg_slider = st.sidebar.slider("Filter by Minutes Per Game", min_mpg, max_mpg, (min_mpg, max_mpg))

    min_fta, max_fta = int(playerstats['FTA'].min()), int(playerstats['FTA'].max())
    fta_slider = st.sidebar.slider("Filter by Free Throws Attempted", min_fta, max_fta, (min_fta, max_fta))

    min_2pa, max_2pa = int(playerstats['2PA'].min()), int(playerstats['2PA'].max())
    two_pa_slider = st.sidebar.slider("Filter by Two-Point Attempts", min_2pa, max_2pa, (min_2pa, max_2pa))

    min_3pa, max_3pa = int(playerstats['3PA'].min()), int(playerstats['3PA'].max())
    three_pa_slider = st.sidebar.slider("Filter by Three-Point Attempts", min_3pa, max_3pa, (min_3pa, max_3pa))

    # Applying the filters
    filtered_data = playerstats[
        (playerstats['TEAM'].isin(selected_team)) & 
        (playerstats['POS'].isin(selected_pos)) & 
        (playerstats['AGE'].between(*age_slider)) &
        (playerstats['PPG'].between(*ppg_slider)) &
        (playerstats['RPG'].between(*rpg_slider)) &
        (playerstats['APG'].between(*apg_slider)) &
        (playerstats['SPG'].between(*spg_slider)) &
        (playerstats['BPG'].between(*bpg_slider)) &
        (playerstats['TPG'].between(*tpg_slider)) &
        (playerstats['GP'].between(*gp_slider)) &
        (playerstats['MPG'].between(*mpg_slider)) &
        (playerstats['FTA'].between(*fta_slider)) &
        (playerstats['2PA'].between(*two_pa_slider)) &
        (playerstats['3PA'].between(*three_pa_slider))
    ]

    if 'RANK' in filtered_data.columns:
        filtered_data = filtered_data.drop(columns=['RANK', 'P+R','P+A','P+R+A', 'VI', 'ORtg', 'DRtg', 'TS%', 'eFG%', 'USG%', 'TO%'])
    
    display_stats(filtered_data, ['_id', 'TEAM', 'POS', 'AGE', 'PPG', 'RPG', 'APG', 'SPG', 'BPG', 'TPG', 'GP', 'MPG', 'FTA', '2PA', '3PA'])

source = st.radio("Select Stat Type", ('Basic Stats', 'Advanced Stats'))

path = "part-00000-1126c397-bc9b-435a-9de9-555548b6bef6-c000.csv"

if source == 'Basic Stats':
    playerstats = load_data_mongo()
else:
    #playerstats_spark = handle_advanced_data(spark, playerstats_spark, path)
    playerstats = playerstats_spark.toPandas()
    
if not playerstats.empty:
    
    if source == 'Advanced Stats':
        advanced_filters(playerstats_spark)  # Call function to handle advanced stats filtering
    else:
        sorted_unique_team = sorted(playerstats['TEAM'].unique())
        selected_team = st.sidebar.multiselect('Team', sorted_unique_team, sorted_unique_team)
        unique_pos = ['C', 'F', 'G', 'C-F', 'G-F', 'F-G', 'F-C']
        selected_pos = st.sidebar.multiselect('Position', unique_pos, unique_pos)
        playerstats = playerstats[playerstats['TEAM'].isin(selected_team) & playerstats['POS'].isin(selected_pos)]
        basic_filters(playerstats)     # Call function to handle basic stats filtering
else:
    st.error("No player stats available. Check the data source.")

def search_and_display_player_by_id():
    search_id = st.text_input("Search Player by ID")
    if search_id:
        player_data = collection.find_one({'_id': search_id})
        if player_data:
            st.json(player_data)
        else:
            st.error("No player found with that ID.")

# Insert or delete players based on the selected source
handle_player_data(source)

# Search and display player by ID
if source == "Basic Stats":
    search_and_display_player_by_id()
else:
    search_and_display_sparkplayer_by_id(playerstats_spark)

