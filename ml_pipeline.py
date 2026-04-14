import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error

# 1. CONNECT TO YOUR EXISTING DOCKER DATABASE
# Format: postgresql://username:password@host:port/database_name
# (Using the port 5432 mapped in your docker-compose.yml)
db_url = "postgresql://shiny_user:shiny_password@localhost:5432/sales_db"
engine = create_engine(db_url)

# 2. EXTRACT THE DATA
query = """
    SELECT e.primary_name, s.report_date, s.amt_bid, s.qty_bid, s.amt_booked 
    FROM monthly_sales s
    JOIN estimators e ON s.estimator_id = e.id
"""
df = pd.read_sql(query, engine)

print(f"Successfully loaded {len(df)} records from PostgreSQL.")

# 3. FEATURE ENGINEERING (Preparing data for the ML model)
# Extract the month (1-12) to capture Seasonality
df['month'] = pd.to_datetime(df['report_date']).dt.month

# Define our Features (X) and Target (y)
# We want to predict Booked Revenue based on Bid Amount, Bid Qty, and Seasonality
X = df[['amt_bid', 'qty_bid', 'month']]
y = df['amt_booked']

# Split data into Training and Testing sets (A core CS229 concept)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 4. TRAIN THE MACHINE LEARNING MODEL
print("Training Random Forest Regressor...")
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# 5. EVALUATE THE MODEL
predictions = model.predict(X_test)
error = mean_absolute_error(y_test, predictions)
print(f"Model Mean Absolute Error: ${error:,.2f}")

# 6. MAKE A NEW PREDICTION FOR THE BOSS
# Scenario: Larry Howard just bid $5,000,000 across 8 jobs in Month 4 (April).
# What will he actually book?
new_data = pd.DataFrame({'amt_bid': [5000000], 'qty_bid': [8], 'month': [4]})
predicted_revenue = model.predict(new_data)[0]

print(f"\n--- EXECUTIVE ML PREDICTION ---")
print(f"Based on a $5M pipeline of 8 jobs in April,")
print(f"the ML Model predicts we will book: ${predicted_revenue:,.2f}")
