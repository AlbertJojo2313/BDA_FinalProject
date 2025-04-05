#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 1 18:54:50 2025

This script retrieves movie IDs from The Movie Database (TMDB) API based on 
their release dates over a specified time range. It efficiently collects 
movie IDs using parallel processing and saves them into a text file.

@author aj
"""

import requests
import time
import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from tqdm import tqdm

load_dotenv()
API_KEY = os.getenv("API_KEY")
MOVIE_ID_URL = os.getenv("MOVIE_ID_URL")

headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {API_KEY}"
}


def generate_date_range(years=20, initial_chunk=4):
    end_date = datetime.date.today()
    cutoff_date = end_date - datetime.timedelta(days=years*365)
    date_ranges = []

    chunk_size = initial_chunk
    while end_date > cutoff_date:
        start_date = end_date - datetime.timedelta(days=chunk_size * 365)
        date_ranges.append((start_date, end_date))
        end_date = start_date
    return date_ranges


def fetch_movie_ids(start_date, end_date, session):
    movie_ids = []
    page = 1

    while True:
        params = {
            "primary_release_date.gte": start_date.strftime("%Y-%m-%d"),
            "primary_release_date.lte": end_date.strftime("%Y-%m-%d"),
            "page": page,
            "include_adult": True,
        }

        try:
            response = session.get(MOVIE_ID_URL, params=params)
            response.raise_for_status()
            data = response.json()

            if "total_ranges" in data and data['total_ranges'] >= 1000:
                print(f"Too many requests for {start_date} - {end_date}. Splitting further...")
                mid_date = start_date + datetime.timedelta(days=365)
                return fetch_movie_ids(start_date, mid_date, session) + fetch_movie_ids(mid_date, end_date, session)

            if "results" in data:
                movie_ids.extend(movie['id'] for movie in data['results'])

            if page >= data.get('total_pages', 1):
                break

            page += 1
            time.sleep(0.3)
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {start_date} - {end_date}: {e}")
            break

    return movie_ids


def get_all_id(years=20, chunk_size=4, max_workers=12):
    date_ranges = generate_date_range(years, chunk_size)
    movie_ids = []

    session = requests.Session()
    session.headers.update(headers)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_movie_ids, start, end, session): (start, end)
            for start, end in date_ranges
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching Movie IDs"):
            try:
                movie_ids.extend(future.result())
            except Exception as e:
                start, end = futures[future]
                print(f"Error fetching range {start} to {end}: {e}")

    session.close()
    print(f"\nCollected {len(movie_ids)} movie IDs from the last {years} years (chunk size: {chunk_size} years).")
    return movie_ids


def write_movie_ids(movie_ids):
    with open('movie_id.txt', 'w') as f:
        f.write("\n".join(map(str, movie_ids)))


write_movie_ids(get_all_id())
