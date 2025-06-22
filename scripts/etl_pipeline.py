import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv # type: ignore

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', 'configs', '.env'))

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')


def extract():
    data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'rawsales.csv')
    df = pd.read_csv(data_path)
    return df


def transform(df):
    # 1. Drop exact duplicate rows
    df = df.drop_duplicates()

    # 2. Drop rows with missing critical values
    df = df.dropna(subset=['order_date', 'product_id', 'sales_amount'])

    # 3. Convert order_date to datetime, drop invalid dates
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    df = df.dropna(subset=['order_date'])

    # 4. Ensure correct types
    df['product_id'] = df['product_id'].astype(str).str.strip()
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0).astype(int)
    df['sales_amount'] = pd.to_numeric(df['sales_amount'], errors='coerce').fillna(0.0).astype(float)

    # 5. Filter out zero or negative sales and quantity
    df = df[(df['sales_amount'] > 0) & (df['quantity'] > 0)]

    # 6. Calculate total price
    df['total_price'] = df['quantity'] * df['sales_amount']

    # 7. Add year, month, day columns
    df['year'] = df['order_date'].dt.year
    df['month'] = df['order_date'].dt.month
    df['day'] = df['order_date'].dt.day

    # 8. Remove outliers: sales_amount > 10,000 or quantity > 1000
    df = df[(df['sales_amount'] <= 10000) & (df['quantity'] <= 1000)]

    # 9. Flag high value orders (total_price > 1000)
    df['high_value_order'] = df['total_price'] > 1000

    # 10. Normalize product_id (trim spaces)
    df['product_id'] = df['product_id'].str.upper()

    # 11. Fill missing customer_id with 'Unknown' if exists
    if 'customer_id' in df.columns:
        df['customer_id'] = df['customer_id'].fillna('Unknown').astype(str)

    # 12. Sort by order_date and product_id
    df = df.sort_values(['order_date', 'product_id'])

    # 13. Filter orders to last 2 years
    cutoff_date = pd.Timestamp.now() - pd.DateOffset(years=2)
    df = df[df['order_date'] >= cutoff_date]

    # 14. Create revenue category bins
    bins = [0, 100, 500, 1000, float('inf')]
    labels = ['Low', 'Medium', 'High', 'Very High']
    df['revenue_category'] = pd.cut(df['total_price'], bins=bins, labels=labels)

    # 15. Add day of week name
    df['day_of_week'] = df['order_date'].dt.day_name()

    # 16. Average price per unit
    df['avg_price_per_unit'] = df['total_price'] / df['quantity']

    # 17. Add is_weekend flag
    df['is_weekend'] = df['day_of_week'].isin(['Saturday', 'Sunday'])

    # 18. Add cumulative sales per product (running total)
    df['cum_sales_qty'] = df.groupby('product_id')['quantity'].cumsum()
    df['cum_sales_amount'] = df.groupby('product_id')['total_price'].cumsum()

    # 19. Add sales rank per day (by total_price)
    df['daily_sales_rank'] = df.groupby('order_date')['total_price'].rank(method='dense', ascending=False)

    # 20. Add month name
    df['month_name'] = df['order_date'].dt.month_name()

    # 21. Calculate sales growth vs previous day per product (lag)
    df = df.sort_values(['product_id', 'order_date'])
    df['prev_day_sales'] = df.groupby('product_id')['total_price'].shift(1)
    df['sales_growth'] = ((df['total_price'] - df['prev_day_sales']) / df['prev_day_sales']).fillna(0)

    # 22. Flag new product sales (first sale per product)
    df['first_sale_date'] = df.groupby('product_id')['order_date'].transform('min')
    df['is_first_sale'] = df['order_date'] == df['first_sale_date']

    # 23. Flag products with consistently increasing sales over last 3 days
    df['sales_3day_avg'] = df.groupby('product_id')['total_price'].rolling(window=3).mean().reset_index(0,drop=True)
    df['prev_sales_3day_avg'] = df.groupby('product_id')['sales_3day_avg'].shift(1)
    df['increasing_sales'] = df['sales_3day_avg'] > df['prev_sales_3day_avg']

    # 24. Fill any remaining missing avg_price_per_unit with sales_amount
    df['avg_price_per_unit'] = df['avg_price_per_unit'].fillna(df['sales_amount'])

    # 25. Flag orders from customers with multiple purchases in a day (repeat buyer flag)
    if 'customer_id' in df.columns:
        df['customer_daily_purchases'] = df.groupby(['customer_id', 'order_date'])['order_date'].transform('count')
        df['repeat_buyer_flag'] = df['customer_daily_purchases'] > 1
    else:
        df['repeat_buyer_flag'] = False

    return df


def load(df):
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    cursor = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS sales (
        order_id SERIAL PRIMARY KEY,
        order_date DATE NOT NULL,
        product_id VARCHAR(50) NOT NULL,
        quantity INT NOT NULL,
        sales_amount FLOAT NOT NULL,
        total_price FLOAT NOT NULL,
        year INT,
        month INT,
        day INT,
        high_value_order BOOLEAN,
        revenue_category VARCHAR(20),
        day_of_week VARCHAR(20),
        avg_price_per_unit FLOAT,
        is_weekend BOOLEAN,
        cum_sales_qty INT,
        cum_sales_amount FLOAT,
        daily_sales_rank INT,
        month_name VARCHAR(20),
        sales_growth FLOAT,
        is_first_sale BOOLEAN,
        increasing_sales BOOLEAN,
        repeat_buyer_flag BOOLEAN,
        customer_id VARCHAR(50)
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Insert rows (optimize later with batch insert or COPY)
    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO sales (
                order_date, product_id, quantity, sales_amount, total_price,
                year, month, day, high_value_order, revenue_category,
                day_of_week, avg_price_per_unit, is_weekend, cum_sales_qty, cum_sales_amount,
                daily_sales_rank, month_name, sales_growth, is_first_sale, increasing_sales,
                repeat_buyer_flag, customer_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row['order_date'], row['product_id'], row['quantity'], row['sales_amount'], row['total_price'],
                row['year'], row['month'], row['day'], row['high_value_order'], row['revenue_category'],
                row['day_of_week'], row['avg_price_per_unit'], row['is_weekend'], row['cum_sales_qty'], row['cum_sales_amount'],
                int(row['daily_sales_rank']), row['month_name'], row['sales_growth'], row['is_first_sale'], row['increasing_sales'],
                row['repeat_buyer_flag'], row.get('customer_id')
            )
        )
    conn.commit()
    cursor.close()
    conn.close()


def load_summary():
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    cursor = conn.cursor()

    create_summary_query = """
    CREATE TABLE IF NOT EXISTS sales_summary (
        summary_date DATE NOT NULL,
        product_id VARCHAR(50) NOT NULL,
        total_quantity INT NOT NULL,
        total_sales FLOAT NOT NULL,
        average_price FLOAT,
        high_value_orders INT,
        weekend_sales FLOAT,
        repeat_buyer_orders INT,
        PRIMARY KEY (summary_date, product_id)
    );
    """
    cursor.execute(create_summary_query)
    conn.commit()

    cursor.execute("DELETE FROM sales_summary;")
    conn.commit()

    insert_summary_query = """
    INSERT INTO sales_summary (summary_date, product_id, total_quantity, total_sales, average_price, high_value_orders, weekend_sales, repeat_buyer_orders)
    SELECT
        order_date as summary_date,
        product_id,
        SUM(quantity) AS total_quantity,
        SUM(total_price) AS total_sales,
        AVG(avg_price_per_unit) AS average_price,
        SUM(CASE WHEN high_value_order THEN 1 ELSE 0 END) AS high_value_orders,
        SUM(CASE WHEN is_weekend THEN total_price ELSE 0 END) AS weekend_sales,
        SUM(CASE WHEN repeat_buyer_flag THEN 1 ELSE 0 END) AS repeat_buyer_orders
    FROM sales
    GROUP BY order_date, product_id;
    """
def run_etl():
    df = extract()
    df = transform(df)
    load(df)
    load_summary()
