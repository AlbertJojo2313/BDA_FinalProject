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
import time
import pandas as pd
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
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

def fetch_reviews(movie_id, retries=3, backoff=2):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}/reviews"

    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=5)

            if response.status_code == 200:
                data = response.json()
                reviews_list = data.get('results', [])

                return movie_id, [
                    {
                        "content": review.get("content", "NA"),
                        "rating": review.get("author_details", {}).get("rating", "NA")
                    } for review in reviews_list
                ] or [{"content": "NA", "rating": "NA"}]
            elif response.status_code == 429:
                wait_time = backoff * (attempt + 1)
                print(f"Rate Limited! Warning {wait_time}s...")
                time.sleep(wait_time)
                continue
            print(
                f"Failed to fetch reviews for movie ID {movie_id}. Status: {response.status_code}")
            return movie_id, [{"content": "NA", "rating": "NA"}]

        except Exception as e:
            print(
                f"Error fetching reviews for movie ID {movie_id}(attempt {attempt+1}): {e}")
            time.sleep(backoff)
    return movie_id, [{"content": "NA", "rating": "NA"}]

# Parallelized review fetching


def get_reviews_map(movie_ids, max_workers=10):
    reviews_map = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(
            fetch_reviews, movie_id): movie_id for movie_id in movie_ids}

        for future in tqdm(as_completed(futures), total=len(movie_ids), desc="Fetching Reviews"):
            movie_id = futures[future]
            try:
                reviews_map[movie_id] = future.result()[1]
            except Exception as e:
                print(f"Error processing movie ID {movie_id}: {e}")
                reviews_map[movie_id] = [{"content": "NA", "rating": "NA"}]

    return reviews_map

# Save to CSV


def save_reviews_to_csv(reviews_map, filename='movie_reviews.csv'):
    directory = '../data'

    # Create the data directory if it does not exist
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    file_path = os.join(directory, filename)

    # Collects rows from the review to make a dataframe
    rows = []
    for movie_id, reviews in reviews_map.items():
        for review in reviews:
            rows.append({
                'movie_id': movie_id,
                'review': review['content'],
                'rating': review['rating']
            })

    df = pd.DataFrame(rows)
    df.to_csv(filepath, index=False)
    print(f"Saved {len(df)} reviews to {file_path}")


# Main Execution
if __name__ == "__main__":
    movie_ids = read_movie_ids()
    print(f"Found {len(movie_ids)} movie IDs. Fetching reviews...")

    reviews_map = get_reviews_map(movie_ids, max_workers=10)
    save_reviews_to_csv(reviews_map)

    print("All reviews fetched and saved!")
