import os

import streamlit as st
import pandas as pd
import pymysql

PROCEDURES = {
    "Get games released within a certain date range": "get_games_by_price_and_date",
    "See how reviews and recommendations are affected by price range": "get_reco_by_price_range",
    "See the relationship between genre and language to reviews and recommendations": "analyze_genre_language_to_reviews_recommendations",
    "See the relationship between game price and developers": "sp_analyze_game_price_developer_relationship",
}

pd.set_option("styler.render.max_elements", 1000000)

# Function to connect to MySQL
def connect_to_db():
    connection = pymysql.connect(
        host=os.environ.get('DB_URL'),
        port=3306,
        user=os.environ.get('DB_USERNAME'),
        password=os.environ.get('DB_PASSWORD'),
        database="gamesdb"
        # host="localhost",
        # user="root",
        # password="12345678",
        # database="gamesdb",
        # port=3000
    )
    return connection

def get_genres_and_languages():
    conn = connect_to_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT name FROM genres")
            genres = cursor.fetchall()
            genres = [genre[0] for genre in genres]

            cursor.execute("SELECT DISTINCT name FROM languages")
            languages = cursor.fetchall()
            languages = [language[0] for language in languages]

        return genres, languages
    finally:
        conn.close()

# Function to execute a stored procedure
def call_procedure(procedure_name, params, formatter):
    procedure_func = PROCEDURES[procedure_name]
    conn = connect_to_db()
    try:
        with conn.cursor() as cursor:
            cursor.callproc(procedure_func, params)
            result = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

        df = formatter(pd.DataFrame(result, columns=columns))
        return df.style.hide(axis="index").format(precision=2)
    finally:
        conn.close()

# Streamlit app
def main():
    genres, languages = get_genres_and_languages()

    st.title("STADVDB MCO1 - OLAP Application")
    st.markdown("Use the sidebar to select and run stored procedures.")
    
    # Sidebar for options
    st.sidebar.header("Options")
    procedure = st.sidebar.selectbox("Select Procedure", list(PROCEDURES.keys()))

    if procedure == "Get games released within a certain date range":
        start_date = st.sidebar.date_input("Start Date").strftime("%Y-%m-%d")
        end_date = st.sidebar.date_input("End Date").strftime("%Y-%m-%d")
        date_interval = st.sidebar.selectbox("Date Interval", ['yearly', 'quarterly', 'monthly', 'daily'])

        if st.sidebar.button("Run Procedure"):
            match date_interval:
                case 'yearly':
                    def formatter(df):
                        return (
                            df.groupby(['Year', 'count', 'name', 'price'], sort=False, as_index=False)
                                .agg({
                                    'developer': lambda x: set(x.dropna().tolist()),
                                    'genre': lambda x: set(x.dropna().tolist()),
                                    'website': lambda x: set(x.dropna().tolist()),
                                    'platform': lambda x: set(x.dropna().tolist()),
                                })
                                .reset_index(drop=True)
                        )

                case 'quarterly':
                    def formatter(df):
                        return (
                            df.groupby(['Year', 'Quarter', 'count', 'name', 'price'], sort=False, as_index=False)
                                .agg({
                                    'developer': lambda x: set(x.dropna().tolist()),
                                    'genre': lambda x: set(x.dropna().tolist()),
                                    'website': lambda x: set(x.dropna().tolist()),
                                    'platform': lambda x: set(x.dropna().tolist()),
                                })
                                .reset_index(drop=True)
                        )
                
                case 'monthly':
                    def formatter(df):
                        return (
                            df.groupby(['Year', 'Month', 'count', 'name', 'price'], sort=False, as_index=False)
                                .agg({
                                    'developer': lambda x: set(x.dropna().tolist()),
                                    'genre': lambda x: set(x.dropna().tolist()),
                                    'website': lambda x: set(x.dropna().tolist()),
                                    'platform': lambda x: set(x.dropna().tolist()),
                                })
                                .reset_index(drop=True)
                        )
                    
                case 'daily':
                    def formatter(df):
                        return (
                            df.groupby(['Date', 'count', 'name', 'price'], sort=False, as_index=False)
                                .agg({
                                    'developer': lambda x: set(x.dropna().tolist()),
                                    'genre': lambda x: set(x.dropna().tolist()),
                                    'website': lambda x: set(x.dropna().tolist()),
                                    'platform': lambda x: set(x.dropna().tolist()),
                                })
                                .reset_index(drop=True)
                        )
                    
            df = call_procedure(
                procedure, 
                [start_date, end_date, date_interval], 
                formatter
            )
            st.write(df)

    elif procedure == "See how reviews and recommendations are affected by price range":
        price_interval = st.sidebar.number_input("Price Interval", min_value=10.0, step=0.1)

        if st.sidebar.button("Run Procedure"):
            def formatter(df):
                return df

            df = call_procedure(procedure, [price_interval], formatter)
            st.write(df)

    elif procedure == "See the relationship between genre and language to reviews and recommendations":
        selected_genres = st.sidebar.multiselect("Genres", genres, default=genres[0] if genres else None)
        selected_languages = st.sidebar.multiselect("Languages", languages, default=languages[0] if languages else None)
        pivot_axis = st.sidebar.selectbox("Pivot Axis", ["genre", "language"])

        if not selected_genres or not selected_languages:
            st.sidebar.warning("Please select at least one genre and one language.")
            return

        if st.sidebar.button("Run Procedure"):
            def formatter(df):
                return df

            df = call_procedure(
                procedure, 
                [",".join(selected_genres), ",".join(selected_languages), pivot_axis], 
                formatter
            )
            st.write(df)

    elif procedure == "See the relationship between game price and developers":
        price_interval = st.sidebar.number_input("Price Interval", min_value=10.0, step=0.01)
        pivot_axis = st.sidebar.selectbox("Pivot Axis", ["price", "developer"])

        if st.sidebar.button("Run Procedure"):
            def formatter(df):
                return df

            df = call_procedure(procedure, [price_interval, pivot_axis], formatter)
            st.write(df)

if __name__ == "__main__":
    main()