#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 1 18:54:50 2025

This script retrieves movie IDs from The Movie Database (TMDB) API based on 
their release dates over a specified time range. It efficiently collects 
movie IDs using parallel processing and saves them into a text file.

### How It Works:
- The script generates date ranges spanning the last 20 years (default).
- It fetches movie IDs for each date range using TMDB's API, handling API limits 
  by dynamically adjusting date chunks if needed.
- It uses a thread pool to parallelize API requests for faster data collection.
- The collected movie IDs are saved in `movie_id.txt` for retrieving reviews efficiently.

### API Information:
- Source: The Movie Database (TMDB)
- API Documentation: https://www.themoviedb.org/documentation/api
- Note: This script respects API rate limits and automatically adjusts requests
  to prevent overloading.

@author: aj
"""

import requests
import time
import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

load_dotenv()
API_KEY = os.getenv("API_KEY")
MOVIE_ID_URL = os.getenv("MOVIE_ID_URL")

headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {API_KEY}"
}
# Uses the movie api to get movie id.


def generate_date_range(years=20, initial_chunk=2):
    end_date = datetime.date.today()
    cutoff_date = end_date - datetime.timedelta(days=years*365)
    date_ranges = []

    chunk_size = initial_chunk
    while end_date > cutoff_date:
        start_date = end_date - datetime.timedelta(days=chunk_size * 365)
        date_ranges.append((start_date, end_date))
        end_date = start_date  # Makes the end_date the prev start_date
    return date_ranges


def fetch_movie_ids(start_date, end_date):
    movie_ids = []
    page = 1

    while True:
        params = {
            "primary_release_date.gte": start_date.strftime("%Y-%m-%d"),
            "page": page,
            "end_date": end_date.strftime("%Y-%m-%d"),
            "include_adult": True,
        }

        try:
            response = requests.get(
                MOVIE_ID_URL, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            if "total_ranges" in data and data['total_ranges'] >= 500:
                print(
                    f"Too many requests for {start_date} - {end_date}. Splitting furter...")
                mid_date = start_date + datetime.timedelta(days=365)
                return fetch_movie_ids(start_date, mid_date) + fetch_movie_ids(mid_date, end_date)

            if "results" in data:
                movie_ids.extend(movie['id'] for movie in data['results'])

            if page >= data.get('total_pages', 1):
                break

            page += 1
            time.sleep(0.2)
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {start_date} - {end_date}: {e}")
            break

    return movie_ids


def get_all_id(years=20, chunk_size=2, max_workers=10):
    date_ranges = generate_date_range(years, chunk_size)
    movie_ids = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_range = {executor.submit(fetch_movie_ids, start, end): (
            start, end) for start, end in date_ranges}

        for future in as_completed(future_to_range):
            movie_ids.extend(future.result())
        print(
            f"Collected {len(movie_ids)} movie IDs from the last {years} years (chunk size: {chunk_size} years).")
    return movie_ids


def write_movie_ids(movie_ids):
    with open('movie_id.txt', 'w') as f:
        f.write("\n".join(map(str, movie_ids)))


write_movie_ids(get_all_id())
