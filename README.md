# Telegraph-Data-Engineer-Exercise

This project processes user journeys from the hitlog and identifies the top 3 articles that most frequently lead to registration on the Telegraph website.

## Steps:
1. Generate fake data using `Faker`.
2. Group user journeys and extract articles before `/register` events.
3. Count and rank articles based on influence.