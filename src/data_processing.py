import pandas as pd
import os

def load_raw_data(path):
    """
    Reads the raw Netflix CSV file.
    """
    return pd.read_csv(path)

def clean_data(df):
    """
    Basic cleaning:
    - Standardize column names
    - Convert date_added to datetime
    - Create year_added and month_added
    - Clean duration column
    """

    # make all column names lower_case_with_underscores
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # convert date_added to datetime
    df["date_added"] = pd.to_datetime(df["date_added"], errors="coerce")

    # extract year and month added
    df["year_added"] = df["date_added"].dt.year
    df["month_added"] = df["date_added"].dt.month

    # clean duration (movies → minutes, shows → seasons)
    df["duration_int"] = df["duration"].str.extract(r"(\d+)").astype(float)

        # --- handle missing values ---
    df["country"] = df["country"].fillna("Unknown")
    df["rating"] = df["rating"].fillna("Not Rated")

    # --- split multi-value columns ---
    df["listed_in"] = df["listed_in"].str.split(", ")
    df["cast"] = df["cast"].fillna("").str.split(", ")
    df["director"] = df["director"].fillna("").str.split(", ")

    # --- extract main genre ---
    df["main_genre"] = df["listed_in"].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)

    # --- duration handling ---
    df["duration_int"] = df["duration"].str.extract(r"(\d+)").astype(float)
    df["duration_type"] = df["duration"].str.extract(r"([A-Za-z]+)")

    # Convert minutes to consistent format
    df["duration_minutes"] = df.apply(
        lambda row: row["duration_int"] if row["duration_type"] == "min" else row["duration_int"] * 45,
        axis=1
    )

    # --- create content category ---
    def categorize(genre):
        if genre is None:
            return "Other"
        genre = genre.lower()
        if "children" in genre or "kids" in genre:
            return "Kids"
        elif "documentary" in genre:
            return "Documentary"
        elif "comedy" in genre:
            return "Comedy"
        elif "action" in genre:
            return "Action"
        elif "romance" in genre:
            return "Romance"
        elif "horror" in genre:
            return "Horror"
        elif "drama" in genre:
            return "Drama"
        else:
            return "Other"

    df["content_category"] = df["main_genre"].apply(categorize)


    return df

def save_processed(df, output_path):
    """
    Saves cleaned dataset into data/processed folder.
    """
    df.to_csv(output_path, index=False)


if __name__ == "__main__":
    # File locations
    raw_path = r"C:/Users/jayesh/py proj/netflix-analysis/data/raw/netflix_titles.csv"
    processed_path = r"C:\Users\jayesh\py proj\netflix-analysis\data\processed/netflix_titles.csv"

    # Load → Clean → Save
    df = load_raw_data(raw_path)
    df = clean_data(df)
    save_processed(df, processed_path)

    print("Cleaning complete! Saved to:", processed_path)
