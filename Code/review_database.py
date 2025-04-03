#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 2 14:19:20 2025

This script uses the TMDB (The Movie Database) API to aggregate a dataset of movies and their associated reviews.
The script fetches reviews for a list of movie IDs stored in a text file, processes them in parallel using 
multiprocessing, and saves the aggregated data into a CSV file for further analysis.

The Movie Database (TMDb) provides access to a massive amount of movie metadata. This dataset is sourced from their public API.

API Documentation: https://www.themoviedb.org/documentation/api

@author: aj
"""

import requests
import pandas as pd
import os
from dotenv import load_dotenv
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

# Load API Key from .env file
load_dotenv()
API_KEY = os.getenv("API_KEY")

HEADERS = {
    "accept": "application/json",
    "Authorization": f"Bearer {API_KEY}"
}

# Read movie IDs from file


def read_movie_ids(filename='movie_id.txt'):
    with open(filename, 'r') as f:
        return [int(line.strip()) for line in f if line.strip()]

# Fetch movie reviews


def fetch_reviews(movie_id):
    movie_review_url = f"https://api.themoviedb.org/3/movie/{movie_id}/reviews"

    try:
        response = requests.get(movie_review_url, headers=HEADERS)

        if response.status_code == 200:
            data = response.json()
            reviews_list = data.get('results', [])

            # Return reviews or a placeholder if none exist
            return movie_id, [{"content": review.get("content", "NA")} for review in reviews_list] or [{"content": "NA"}]

        print(
            f"Failed to fetch reviews for movie ID {movie_id}. Status: {response.status_code}")
        return movie_id, [{"content": "NA"}]

    except Exception as e:
        print(f"Error fetching reviews for movie ID {movie_id}: {e}")
        return movie_id, [{"content": "NA"}]

# Parallelized review fetching using multiprocessing


def get_reviews_map(movie_ids):
    reviews_map = {}

    # Adjust based on CPU cores
    with ProcessPoolExecutor(max_workers=10) as executor:
        future_to_movie = {executor.submit(
            fetch_reviews, movie_id): movie_id for movie_id in movie_ids}

        for future in tqdm(as_completed(future_to_movie),total=len(movie_ids),desc="Fetching Reviews"):
            movie_id = future_to_movie[future]
            try:
                reviews_map[movie_id] = future.result()[1]
            except Exception as e:
                print(f"Error fetching reviews for movie ID {movie_id}: {e}")
                reviews_map[movie_id] = [{"content": "NA"}]

    return reviews_map

# Save to CSV


def save_reviews_to_csv(reviews_map, filename='movie_reviews.csv'):
    rows = []

    for movie_id, reviews in reviews_map.items():
        for review in reviews:
            rows.append({
                'movie_id': movie_id,
                'review': review['content']
            })

    df = pd.DataFrame(rows)
    df.to_csv(filename, index=False)
    print(f"Saved {len(df)} reviews to {filename}")


# Main Execution
if __name__ == "__main__":
    movie_ids = read_movie_ids()
    reviews_map = get_reviews_map(movie_ids)
    save_reviews_to_csv(reviews_map)
