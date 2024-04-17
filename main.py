import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pymongo import MongoClient

# MongoDB setup
client = MongoClient("mongodb+srv://shoon9525:WmOMn1vTg65pnXID@cluster0.iaom9xh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client["NBA2023"]  # Create or use existing database
collection = db["please"]  # Create or use existing collection

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

    # Applying the filters
    filtered_data = playerstats[
        (playerstats['TEAM'].isin(selected_team)) & 
        (playerstats['POS'].isin(selected_pos)) & 
        (playerstats['AGE'].between(*age_slider)) &
        (playerstats['PPG'].between(*ppg_slider)) &
        (playerstats['RPG'].between(*rpg_slider)) &
        (playerstats['APG'].between(*apg_slider))
    ]

    st.header('Display Player Stats of Selected Team(s) and Filters')
    st.write('Data Dimension: {} rows and {} columns.'.format(filtered_data.shape[0], filtered_data.shape[1]))
    st.dataframe(filtered_data)
else:
    st.error("No player stats available. Check the MongoDB data.")

    # Form for new data entry
with st.form("player_input"):
    st.write("## Enter new player data")
    name = st.text_input("Player Name", "LeBron James")
    team = st.text_input("Team", "LAL")
    pos = st.text_input("Position", "F")
    age = st.number_input("Age", min_value=18, max_value=40, value=30)
    ppg = st.number_input("Points Per Game", min_value=0.0, max_value=50.0, value=25.0)
    rpg = st.number_input("Rebounds Per Game", min_value=0.0, max_value=20.0, value=7.0)
    apg = st.number_input("Assists Per Game", min_value=0.0, max_value=20.0, value=7.0)
    submit_button = st.form_submit_button("Submit")

# Inserting new data to MongoDB
if submit_button:
    new_data = {
        "_id": name,
        "TEAM": team,
        "POS": pos,
        "AGE": age,
        "PPG": ppg,
        "RPG": rpg,
        "APG": apg
    }
    try:
        collection.insert_one(new_data)
        st.success("Player data inserted successfully!")
    except Exception as e:
        st.error(f"Error inserting data: {e}")
