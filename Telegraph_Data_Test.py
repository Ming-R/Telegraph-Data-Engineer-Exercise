# Generate Fake Data Using Faker

import pandas as pd
from faker import Faker
import random

# Initialize Faker
fake = Faker()

# Generate fake hitlog data
data = []
for _ in range(1000):
    user_id = fake.uuid4()
    # Generate article visit events
    for _ in range(random.randint(1, 5)):
        article_num = random.randint(1, 10)
        timestamp = fake.date_time_this_year()
        page_name = f"Article {article_num}"
        page_url = f"/articles/{article_num}"
        data.append([page_name, page_url, user_id, timestamp])

    # Randomly add a registration event
    if random.choice([True, False]):
        data.append(["Register", "/register", user_id, fake.date_time_this_year()])

# Save to CSV
df = pd.DataFrame(data, columns=["page_name", "page_url", "user_id", "timestamp"])
df.to_csv("hitlog.csv", index=False)

# Display the first few rows
df.head()


# Pipeline Functions
# Define the pipeline functions to read the hitlog.csv, group user journeys, identify articles before registration, and output the top 3 influential articles.

import csv
from collections import defaultdict, Counter
from datetime import datetime

# Function to read hitlog.csv
def read_hitlog(file_path):
    data = []
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            row['timestamp'] = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S.%f')
            data.append(row)
    return data

# Function to group by user_id and sort by timestamp
def group_by_user(hitlog):
    user_journeys = defaultdict(list)
    for entry in hitlog:
        user_journeys[entry['user_id']].append(entry)
    # Sort each user's journey by timestamp
    for user_id in user_journeys:
        user_journeys[user_id].sort(key=lambda x: x['timestamp'])
    return user_journeys

# Function to extract articles before registration
def extract_articles_before_registration(user_journeys):
    article_counter = Counter()
    for entries in user_journeys.values():
        article_set = set()
        for i, entry in enumerate(entries):
            if entry['page_url'] == '/register':
                # Gather all articles before registration
                for prior in entries[:i]:
                    if prior['page_url'].startswith('/articles/'):
                        article_set.add((prior['page_name'], prior['page_url']))
                break  # Only consider the first registration per user
        for article in article_set:
            article_counter[article] += 1
    return article_counter

# Function to write top articles to a CSV
def write_top_articles(counter, output_path, top_n=3):
    top_articles = counter.most_common(top_n)
    with open(output_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['page_name', 'page_url', 'total'])
        for (name, url), total in top_articles:
            writer.writerow([name, url, total])


# Run the Pipeline
# Process the fake data and generate the top 3 articles.

# Process the hitlog.csv
hitlog = read_hitlog("hitlog.csv")
journeys = group_by_user(hitlog)
counter = extract_articles_before_registration(journeys)
write_top_articles(counter, "top_articles.csv")

# Display the top 3 articles
pd.read_csv("top_articles.csv")


# Unit Tests


import unittest

class TestTelegraphPipeline(unittest.TestCase):

    def setUp(self):
        # Build sample data based on the PDF example
        self.sample_data = [
            {'page_name': 'Article 1', 'page_url': '/articles/1', 'user_id': 'user1', 'timestamp': datetime(2025, 3, 1, 10, 0, 0)},
            {'page_name': 'Article 2', 'page_url': '/articles/2', 'user_id': 'user1', 'timestamp': datetime(2025, 3, 1, 10, 1, 0)},
            {'page_name': 'Article 3', 'page_url': '/articles/3', 'user_id': 'user1', 'timestamp': datetime(2025, 3, 1, 10, 2, 0)},
            {'page_name': 'Register',   'page_url': '/register',   'user_id': 'user1', 'timestamp': datetime(2025, 3, 1, 10, 3, 0)},

            {'page_name': 'Article 1', 'page_url': '/articles/1', 'user_id': 'user2', 'timestamp': datetime(2025, 3, 2, 9, 0, 0)},
            {'page_name': 'Register',   'page_url': '/register',   'user_id': 'user2', 'timestamp': datetime(2025, 3, 2, 9, 1, 0)},

            {'page_name': 'Article 2', 'page_url': '/articles/2', 'user_id': 'user3', 'timestamp': datetime(2025, 3, 3, 8, 0, 0)},
            {'page_name': 'Article 1', 'page_url': '/articles/1', 'user_id': 'user3', 'timestamp': datetime(2025, 3, 3, 8, 1, 0)},
            {'page_name': 'Register',   'page_url': '/register',   'user_id': 'user3', 'timestamp': datetime(2025, 3, 3, 8, 2, 0)},
        ]

    def test_article_counts(self):
        # Use the real pipeline logic
        journeys = group_by_user(self.sample_data)
        counter = extract_articles_before_registration(journeys)

        self.assertEqual(counter[('Article 1', '/articles/1')], 3)
        self.assertEqual(counter[('Article 2', '/articles/2')], 2)
        self.assertEqual(counter[('Article 3', '/articles/3')], 1)

    def test_only_top_3_returned(self):
        journeys = group_by_user(self.sample_data)
        counter = extract_articles_before_registration(journeys)
        top_articles = counter.most_common(3)
        self.assertEqual(len(top_articles), 3)

# Run the tests inside the notebook
unittest.main(argv=[''], verbosity=2, exit=False)






