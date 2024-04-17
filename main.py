import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pymongo import MongoClient

# MongoDB setup
client = MongoClient("mongodb+srv://shoon9525:WmOMn1vTg65pnXID@cluster0.iaom9xh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client["NBA_DB"]  # Create or use existing database
collection = db["2023"]  # Create or use existing collection

st.set_page_config(layout="wide")
st.title('NBA Player Stats Explorer from MongoDB')

st.sidebar.header('User Input Features')
selected_year = st.sidebar.selectbox('Year', list(reversed(range(1950,2020))))

@st.cache
def load_data(year):
    query = {"Year": year}
    results = collection.find(query)
    df = pd.DataFrame(list(results))
    return df

playerstats = load_data(selected_year)

# Sidebar - Team selection
sorted_unique_team = sorted(playerstats['Tm'].unique())
selected_team = st.sidebar.multiselect('Team', sorted_unique_team, sorted_unique_team)

# Sidebar - Position selection
unique_pos = ['C','PF','SF','PG','SG']
selected_pos = st.sidebar.multiselect('Position', unique_pos, unique_pos)

# Filtering data
df_selected_team = playerstats[(playerstats['Tm'].isin(selected_team)) & (playerstats['Pos'].isin(selected_pos))]

st.header('Display Player Stats of Selected Team(s)')
st.write('Data Dimension: ' + str(df_selected_team.shape[0]) + ' rows and ' + str(df_selected_team.shape[1]) + ' columns.')
st.dataframe(df_selected_team)

# Download function
def filedownload(df):
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()  # strings <-> bytes conversions
    href = f'<a href="data:file/csv;base64,{b64}" download="playerstats.csv">Download CSV File</a>'
    return href

st.markdown(filedownload(df_selected_team), unsafe_allow_html=True)

# Heatmap and bar chart logic remains the same
