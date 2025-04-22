Movie Recommendation System with PySpark & Delta Lake
Project Description:
A scalable movie recommendation engine built using PySpark's MLlib for distributed machine learning and Delta Lake for reliable data storage. The system uses collaborative filtering (Alternating Least Squares algorithm) to analyze user-movie ratings and generate personalized recommendations.

Key Features:
✅ Distributed Processing: Handles large-scale datasets (tested on 10M+ ratings) using PySpark
✅ Delta Lake Integration: Ensures ACID compliance, versioning, and time-travel capabilities
✅ Hybrid Approach: Combines user-based and item-based filtering for better accuracy
✅ Optimized Performance: Achieves RMSE 0.85 through hyperparameter tuning and cross-validation
✅ Production-Ready: Includes data pipelines, model evaluation, and API-serving design

Tech Stack:
PySpark (MLlib for ALS, Spark SQL for ETL)

Delta Lake (for versioned storage and reliability)

Python (for preprocessing and evaluation)

AWS S3/DBFS (cloud storage integration)

Use Cases:
Streaming platforms (personalized "For You" recommendations)

E-commerce (cross-movie suggestions)

Research (benchmarking recommendation algorithms)
