import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pymongo import MongoClient

# MongoDB setup
client = MongoClient("mongodb+srv://shoon9525:WmOMn1vTg65pnXID@cluster0.iaom9xh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client["NBA2023"]  # Create or use existing database
collection = db["newCol"]  # Create or use existing collection

st.set_page_config(layout="wide")
st.title('NBA Player Stats Explorer for 2023')

def load_data():
    results = collection.find({})
    # Convert each document into a DataFrame row
    all_data = pd.DataFrame(list(results))  # Convert the cursor to a list and then create a DataFrame
    return all_data

playerstats = load_data()

if not playerstats.empty:


    # Sidebar - Team selection
    sorted_unique_team = sorted(playerstats['TEAM'].unique())
    selected_team = st.sidebar.multiselect('Team', sorted_unique_team, sorted_unique_team)

    # Sidebar - Position selection

    unique_pos = ['C', 'F', 'G']
    selected_pos = st.sidebar.multiselect('Position', unique_pos, unique_pos)

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


    st.header('Display Player Stats of Selected Team(s) and Filters')
    st.write('Data Dimension: {} rows and {} columns.'.format(filtered_data.shape[0], filtered_data.shape[1]))
    st.dataframe(filtered_data)
else:
    st.error("No player stats available. Check the MongoDB data.")

    # Form for new data entry
with st.form("player_input"):
    st.write("## Enter new player data")
    name = st.text_input("Player Name", "")
    team = st.text_input("Team", "")
    pos = st.text_input("Position", "")
    age = st.number_input("Age", min_value=18, max_value=40, value=30)
    gp = st.number_input("Games Played", min_value=0, max_value=100, value=82)
    mpg = st.number_input("Minutes Per Game", min_value=0.0, max_value=60.0, value=35.0)
    fta = st.number_input("Free Throws Attempted", min_value=0, max_value=1000, value=300)
    ft_pct = st.number_input("Free Throw Percentage", min_value=0.0, max_value=100.0, value=75.0)
    twopa = st.number_input("Two-Point Attempts", min_value=0, max_value=1000, value=500)
    twop_pct = st.number_input("Two-Point Percentage", min_value=0.0, max_value=100.0, value=50.0)
    threepa = st.number_input("Three-Point Attempts", min_value=0, max_value=1000, value=200)
    threep_pct = st.number_input("Three-Point Percentage", min_value=0.0, max_value=100.0, value=35.0)
    ppg = st.number_input("Points Per Game", min_value=0.0, max_value=50.0, value=25.0)
    rpg = st.number_input("Rebounds Per Game", min_value=0.0, max_value=20.0, value=7.0)
    apg = st.number_input("Assists Per Game", min_value=0.0, max_value=20.0, value=7.0)
    spg = st.number_input("Steals Per Game", min_value=0.0, max_value=10.0, value=1.0)
    bpg = st.number_input("Blocks Per Game", min_value=0.0, max_value=10.0, value=0.5)
    tpg = st.number_input("Turnovers Per Game", min_value=0.0, max_value=10.0, value=2.0)
    

    submit_button = st.form_submit_button("Submit")

# Inserting new data to MongoDB
if submit_button:
    new_data = {
        "_id": name,
        "TEAM": team,
        "POS": pos,
        "AGE": age,
        "GP": gp,
        "MPG": mpg,
        "USG%": usg,
        "TO%": to_pct,
        "FTA": fta,
        "FT%": ft_pct,
        "2PA": twopa,
        "2P%": twop_pct,
        "3PA": threepa,
        "3P%": threep_pct,
        "eFG%": efg_pct,
        "TS%": ts_pct,
        "PPG": ppg,
        "RPG": rpg,
        "APG": apg,
        "SPG": spg,
        "BPG": bpg,
        "TPG": tpg,
        "P+R": pr,
        "P+A": pa,
        "P+R+A": pra,
        "VI": vi,
        "ORtg": ortg,
        "DRtg": drtg
    }
    try:
        collection.insert_one(new_data)
        st.success("Player data inserted successfully!")
    except Exception as e:
        st.error(f"Error inserting data: {e}")

with st.form("player_delete"):
    st.write("## Delete Player Data")
    delete_name = st.text_input("Enter Player Name to Delete", "")
    delete_button = st.form_submit_button("Delete")

# Delete action
if delete_button and delete_name:
    try:
        result = collection.delete_one({"_id": delete_name})
        if result.deleted_count > 0:
            st.success(f"Player data for {delete_name} deleted successfully!")
        else:
            st.error("No player found with that name.")
    except Exception as e:
        st.error(f"Error deleting player data: {e}")
