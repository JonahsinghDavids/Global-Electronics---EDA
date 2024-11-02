import pandas as pd
from sqlalchemy import create_engine, text

# Load CSV files
print("Loading CSV files...")
data_dict_df = pd.read_csv(r"C:\Users\Lenovo\Downloads\DataSet-20240730T140623Z-001\DataSet\Data_Dictionary.csv")
sales_df = pd.read_csv(r"C:\Users\Lenovo\Downloads\DataSet-20240730T140623Z-001\DataSet\Sales.csv")
exchange_rates_df = pd.read_csv(r"C:\Users\Lenovo\Downloads\DataSet-20240730T140623Z-001\DataSet\Exchange_Rates.csv")
stores_df = pd.read_csv(r"C:\Users\Lenovo\Downloads\DataSet-20240730T140623Z-001\DataSet\Stores.csv")
products_df = pd.read_csv(r"C:\Users\Lenovo\Downloads\DataSet-20240730T140623Z-001\DataSet\Products.csv")
customers_df = pd.read_csv(r"C:\Users\Lenovo\Downloads\DataSet-20240730T140623Z-001\DataSet\Customers.csv", encoding='latin1')

print("CSV files loaded.")

# Data Cleaning and Preparation
print("Cleaning and preparing data...")
sales_df['Order Date'] = pd.to_datetime(sales_df['Order Date'])
sales_df['Delivery Date'] = pd.to_datetime(sales_df['Delivery Date'])
exchange_rates_df['Date'] = pd.to_datetime(exchange_rates_df['Date'])
stores_df['Open Date'] = pd.to_datetime(stores_df['Open Date'])

sales_df_cleaned = sales_df.dropna(subset=['Delivery Date'])
median_square_meters = stores_df['Square Meters'].median()
stores_df['Square Meters'].fillna(median_square_meters, inplace=True)
print("Data cleaning complete.")

# Verify and Rename Columns in stores_df
stores_df.rename(columns={
    'Square Meters': 'SquareMeters',
    'Open Date': 'OpenDate'
}, inplace=True)

# Verify and Rename Columns in products_df
products_df.rename(columns={
    'Product Name': 'ProductName',
    'Unit Cost USD' : 'UnitCostUSD',
    'Unit Price USD': 'UnitPriceUSD'  
}, inplace=True)

# Verify and Rename Columns in customers_df
customers_df.rename(columns={
    'Name': 'CustomerName',
    'Country': 'Country'
}, inplace=True)

# Convert the Birthday column to datetime and calculate Age
customers_df['Birthday'] = pd.to_datetime(customers_df['Birthday'])
customers_df['Age'] = customers_df['Birthday'].apply(lambda x: pd.Timestamp.now().year - x.year)

# Merge datasets
print("Merging datasets...")
merged_sales_stores_df = sales_df_cleaned.merge(stores_df, on='StoreKey', how='left')
merged_sales_stores_products_df = merged_sales_stores_df.merge(products_df, on='ProductKey', how='left')
final_merged_df = merged_sales_stores_products_df.merge(customers_df, on='CustomerKey', how='left')

# Rename DataFrame columns to match SQL schema
final_merged_df.rename(columns={
    'Order Number': 'OrderNumber',
    'Line Item': 'LineItem',
    'Order Date': 'OrderDate',
    'Delivery Date': 'DeliveryDate',
    'Currency Code': 'CurrencyCode',
    'Product Name': 'ProductName',
    'Open Date': 'OpenDate',
    'Square Meters': 'SquareMeters',
    'Customer Name': 'CustomerName',
    'Unit Cost USD': 'UnitCostUSD',
    'Country_y': 'Country'
}, inplace=True)

# Select only relevant columns for each table
sales_columns = [
    'OrderNumber', 'LineItem', 'OrderDate', 'DeliveryDate', 'CustomerKey',
    'StoreKey', 'ProductKey', 'Quantity', 'CurrencyCode'
]
exchange_rates_columns = ['Date', 'Currency', 'Exchange']
stores_columns = ['StoreKey', 'Country', 'State', 'SquareMeters', 'OpenDate']
products_columns = ['ProductKey', 'ProductName', 'Category', 'UnitPriceUSD','UnitCostUSD']  
customers_columns = ['CustomerKey', 'Gender', 'CustomerName', 'Country', 'Age']

# Verify the Columns Exist Before Selecting
filtered_sales_df = final_merged_df[sales_columns]
filtered_exchange_rates_df = exchange_rates_df[exchange_rates_columns]
filtered_stores_df = stores_df[stores_columns]
filtered_products_df = products_df[products_columns]
filtered_customers_df = customers_df[customers_columns]

# URL-encode password if needed
encoded_password = 'Jsd%401908'

# Create SQLAlchemy engine
engine = create_engine(f'mysql+pymysql://root:{encoded_password}@localhost:3306/sales_data_db')

# Create tables using SQLAlchemy
with engine.connect() as connection:
    # Ensure correct SQL command format and use text() for raw SQL
    connection.execute(text('''
    CREATE TABLE IF NOT EXISTS Sales (
        OrderNumber INT,
        LineItem INT,
        OrderDate DATE,
        DeliveryDate DATE,
        CustomerKey INT,
        StoreKey INT,
        ProductKey INT,
        Quantity INT,
        CurrencyCode VARCHAR(3),
        PRIMARY KEY (OrderNumber, LineItem)
    )
    '''))

    connection.execute(text('''
    CREATE TABLE IF NOT EXISTS ExchangeRates (
        Date DATE,
        Currency VARCHAR(3),
        ExchangeRate FLOAT,
        PRIMARY KEY (Date, Currency)
    )
    '''))

    connection.execute(text('''
    CREATE TABLE IF NOT EXISTS Stores (
        StoreKey INT,
        Country VARCHAR(50),
        State VARCHAR(50),
        SquareMeters FLOAT,
        OpenDate DATE,
        PRIMARY KEY (StoreKey)
    )
    '''))

    connection.execute(text('''
    CREATE TABLE IF NOT EXISTS Products (
        ProductKey INT,
        ProductName VARCHAR(100),
        Category VARCHAR(50),
        UnitCostUSD FLOAT,
        UnitPriceUSD FLOAT,
        PRIMARY KEY (ProductKey)
    )
    '''))

    connection.execute(text('''
    CREATE TABLE IF NOT EXISTS Customers (
        CustomerKey INT,
        CustomerName VARCHAR(100),
        Country VARCHAR(50),
        Gender VARCHAR(10),
        Age INT,
        PRIMARY KEY (CustomerKey)
    )
    '''))

print("Database and tables created.")

# Sales data
filtered_sales_df.to_csv("final_sales.csv", index=False, mode='w')
print("Filtered Sales data saved to CSV.")
print("Inserting data into Sales table...")
filtered_sales_df.to_sql('sales', engine, if_exists='replace', index=False, method='multi')

# Exchange Rates data
filtered_exchange_rates_df.to_csv("final_exchange_rates.csv", index=False, mode='w')
print("Filtered Exchange Rates data saved to CSV.")
print("Inserting data into ExchangeRates table...")
filtered_exchange_rates_df.rename(columns={'Exchange': 'ExchangeRate'}, inplace=True)
filtered_exchange_rates_df.to_sql('exchangerates', engine, if_exists='replace', index=False, method='multi')

# Stores data
filtered_stores_df.to_csv("final_stores.csv", index=False, mode='w')
print("Filtered Stores data saved to CSV.")
print("Inserting data into Stores table...")
filtered_stores_df.to_sql('stores', engine, if_exists='replace', index=False, method='multi')

# Products data
filtered_products_df.to_csv("final_products.csv", index=False, mode='w')
print("Filtered Products data saved to CSV.")
print("Inserting data into Products table...")
filtered_products_df.to_sql('products', engine, if_exists='replace', index=False, method='multi')

# Customers data
filtered_customers_df.to_csv("final_customers.csv", index=False, mode='w')
print("Filtered Customers data saved to CSV.")
print("Inserting data into Customers table...")
filtered_customers_df.to_sql('customers', engine, if_exists='replace', index=False, method='multi')
print("Data inserted")


# 1. Demographic Distribution of Customers
query = """
SELECT Country, COUNT(*) AS CustomerCount
FROM Customers
GROUP BY Country;
"""
customer_distribution = pd.read_sql(query, engine)
print("Query 1")
print("Demographic Distribution of Customers:\n", customer_distribution)
print("#------------------------------------------------------------------------")
# 2. Average Order Value
query = """
SELECT AVG(s.Quantity * CAST(REPLACE(TRIM(BOTH '$' FROM p.UnitPriceUSD), ',', '') AS DECIMAL(10, 2))) AS AverageOrderValue
FROM Sales s
JOIN Products p ON s.ProductKey = p.ProductKey;
"""
average_order_value = pd.read_sql(query, engine)
print("Query 2")
print("Average Order Value:\n", average_order_value)
print("#------------------------------------------------------------------------")
# 3. Frequency of Purchases by Customers
query = """
SELECT CustomerKey, COUNT(OrderNumber) AS PurchaseFrequency
FROM Sales
GROUP BY CustomerKey;
"""
purchase_frequency = pd.read_sql(query, engine)
print("Query 3")
print("Frequency of Purchases by Customers:\n", purchase_frequency)
print("#------------------------------------------------------------------------")
# 4. Top Performing Products by Quantity Sold
query = """
SELECT p.ProductName, SUM(s.Quantity) AS TotalQuantitySold
FROM Sales s
JOIN Products p ON s.ProductKey = p.ProductKey
GROUP BY p.ProductName
ORDER BY TotalQuantitySold DESC
LIMIT 10;
"""
top_products = pd.read_sql(query, engine)
print("Query 4")
print("Top Performing Products by Quantity Sold:\n", top_products)
print("#------------------------------------------------------------------------")
# 5. Store Performance by Sales
query = """
SELECT s.StoreKey, 
       SUM(s.Quantity * CAST(REPLACE(TRIM(BOTH '$' FROM p.UnitPriceUSD), ',', '') AS DECIMAL(10, 2))) AS TotalSales
FROM Sales s
JOIN Products p ON s.ProductKey = p.ProductKey
GROUP BY s.StoreKey
ORDER BY TotalSales DESC;
"""
store_performance = pd.read_sql(query, engine)
print("Query 5")
print("Store Performance by Sales:\n", store_performance)
print("#------------------------------------------------------------------------")
# 6. Sales by Currency
query = """
SELECT s.CurrencyCode, 
       SUM(s.Quantity * CAST(REPLACE(TRIM(BOTH '$' FROM p.UnitPriceUSD), ',', '') AS DECIMAL(10, 2))) AS TotalSales
FROM Sales s
JOIN Products p ON s.ProductKey = p.ProductKey
GROUP BY s.CurrencyCode;
"""
sales_by_currency = pd.read_sql(query, engine)
print("Query 6")
print("Sales by Currency:\n", sales_by_currency)
print("#------------------------------------------------------------------------")
#7. Sales Distribution by Category:
query = """
SELECT Category, SUM(Quantity) AS TotalSales
FROM Sales
JOIN Products ON Sales.ProductKey = Products.ProductKey
GROUP BY Category
ORDER BY TotalSales DESC;
"""
sales_by_category = pd.read_sql(query, engine)
print("Query 7")
print(sales_by_category)
print("#------------------------------------------------------------------------")
#8. Sales Trend by Year:
query = """
SELECT YEAR(OrderDate) AS Year, SUM(Quantity) AS TotalSales
FROM Sales
GROUP BY Year
ORDER BY Year;
"""
sales_trend_by_year = pd.read_sql(query, engine)
print("Query 8")
print(sales_trend_by_year)
print("#------------------------------------------------------------------------")
#9. Sales by Country:
query = """
SELECT Customers.Country, SUM(Sales.Quantity) AS TotalSales
FROM Sales
JOIN Customers ON Sales.CustomerKey = Customers.CustomerKey
GROUP BY Customers.Country
ORDER BY TotalSales DESC;
"""
sales_by_country = pd.read_sql(query, engine)
print("Query 9")
print(sales_by_country)
print("#------------------------------------------------------------------------")
#10. Total Sales Over Time:
query = """
SELECT OrderDate, SUM(Quantity) AS TotalSales
FROM Sales
GROUP BY OrderDate
ORDER BY OrderDate;
"""
total_sales_over_time = pd.read_sql(query, engine)
print("Query 10")
print(total_sales_over_time)


# Import libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans

# Set style for visualization
sns.set(style='whitegrid')
#%matplotlib inline

# Load and prepare data (replace final_merged_df with your final DataFrame)
df = final_merged_df.copy()

# Convert necessary columns to appropriate data types
df['OrderDate'] = pd.to_datetime(df['OrderDate'])
df['DeliveryDate'] = pd.to_datetime(df['DeliveryDate'])
df['OpenDate'] = pd.to_datetime(df['OpenDate'])
df['Age'] = pd.to_datetime('today').year - pd.to_datetime(df['Birthday']).dt.year
df['UnitPriceUSD'] = pd.to_numeric(df['UnitPriceUSD'].replace('[\$,]', '', regex=True), errors='coerce')

# **Customer Analysis**

# 1. Demographic Distribution
plt.figure(figsize=(10, 5))
sns.histplot(df['Age'], bins=20, kde=True)
plt.title("Age Distribution of Customers")
plt.xlabel("Age")
plt.show()

plt.figure(figsize=(6, 4))
sns.countplot(data=df, x='Gender')
plt.title("Gender Distribution")
plt.show()

plt.figure(figsize=(10, 6))
df['Country'].value_counts().head(10).plot(kind='bar')
plt.title("Top 10 Customer Countries")
plt.xlabel("Country")
plt.ylabel("Count")
plt.show()

# **Sales Analysis**

# 1. Sales by Product
product_sales = df.groupby('ProductName')['Quantity'].sum().sort_values(ascending=False).head(10)
product_sales.plot(kind='bar', figsize=(10, 6))
plt.title("Top 10 Products by Quantity Sold")
plt.xlabel("Product Name")
plt.ylabel("Quantity Sold")
plt.show()

# 2. Sales by Store
store_sales = df.groupby('StoreKey')['Quantity'].sum().sort_values(ascending=False).head(10)
store_sales.plot(kind='bar', figsize=(10, 6))
plt.title("Top 10 Stores by Quantity Sold")
plt.xlabel("Store ID")
plt.ylabel("Quantity Sold")
plt.show()

# 3. Sales by Currency
df['RevenueUSD'] = df['Quantity'] * df['UnitPriceUSD']
currency_sales = df.groupby('CurrencyCode')['RevenueUSD'].sum().sort_values(ascending=False)

currency_sales.plot(kind='bar', figsize=(10, 6))
plt.title("Sales by Currency in USD")
plt.xlabel("Currency Code")
plt.ylabel("Total Revenue (USD)")
plt.show()

# **Product Analysis**

# 1. Product Popularity
df.groupby('ProductName')['Quantity'].sum().sort_values(ascending=False).head(10).plot(kind='bar')
plt.title("Most Popular Products")
plt.xlabel("Product Name")
plt.ylabel("Quantity Sold")
plt.show()

# 2. Profitability Analysis
# Remove dollar signs and convert to numeric
df['UnitCostUSD'] = pd.to_numeric(df['UnitCostUSD'].replace('[\$,]', '', regex=True), errors='coerce')
df['UnitPriceUSD'] = pd.to_numeric(df['UnitPriceUSD'].replace('[\$,]', '', regex=True), errors='coerce')

# Calculate profit
df['Profit'] = (df['UnitPriceUSD'] - df['UnitCostUSD']) * df['Quantity']
profit_by_product = df.groupby('ProductName')['Profit'].sum().sort_values(ascending=False).head(10)
profit_by_product.plot(kind='bar', figsize=(10, 6))
plt.title("Top 10 Profitable Products")
plt.xlabel("Product Name")
plt.ylabel("Total Profit")
plt.show()

# 3. Category Analysis
category_sales = df.groupby('Category')['Quantity'].sum().sort_values(ascending=False)

category_sales.plot(kind='bar', figsize=(10, 6))
plt.title("Sales by Category")
plt.xlabel("Category")
plt.ylabel("Quantity Sold")
plt.show()

# **Store Analysis**

# 1. Store Performance
store_size_sales = df.groupby('SquareMeters')['Quantity'].sum().sort_values(ascending=False)

store_size_sales.plot(kind='bar', figsize=(12, 6))
plt.title("Sales by Store Size")
plt.xlabel("Square Meters")
plt.ylabel("Quantity Sold")
plt.show()

# 2. Geographical Analysis
country_sales = df.groupby('Country')['Quantity'].sum().sort_values(ascending=False).head(10)

country_sales.plot(kind='bar', figsize=(10, 6))
plt.title("Sales by Country")
plt.xlabel("Country")
plt.ylabel("Quantity Sold")
plt.show()

# **Summary**
print("EDA and Analysis Complete: Insights into customer demographics, purchasing patterns, top products, store performance, and geographic sales distribution.")
