import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

def connect_db():
    return psycopg2.connect(
        host="",
        database="ecommerce_source",
        user="airflow",
        password="airflow",
        port="5432"
    )

def generate_customers(conn, num=1000):
    cursor = conn.cursor()
    customers = []

    print(f"Generating {num} customers...")

    countries = ['USA', 'UK', 'Germany','France','Japan','Australia']

    for i in range(num):
        try:
            cursor.execute("""
                    INSERT INTO ecommerce.customers
                    (email, first_name, last_name, country, city, created_at, updated_at)
                    VALUES(%s, %s, %s, %s, %s, %s, %s)
                    RETURNING customer_id
            """, (
                fake.unique.email(),
                fake.first_name(),
                fake.last_name(),
                random.choice(countries),
                fake.city(),
                fake.date_time_between(start_date='-2y', end_date='-1y'),
                fake.date_time_between(start_date='-2y', end_date='-1y')
            ))

            customers.append(cursor.fetchone()[0])

            if (i + 1) % 100 == 0:
                conn.commit()
                print(f" Created {i + 1} customers")
        except Exception as e:
            conn.rollback()
            if i == 0:
                print(f"DEBUG ERROR: {e}")
    
    conn.commit()
    print(f" Created {len(customers)} customers\n")
    return customers

def generate_products(conn, num=500):
    cursor = conn.cursor()
    products = []

    print(f"Generating {num} products")
    
    categories = {
        'Electronics': ['Apple','Samsung','Sony','LG'],
        'Clothing': ['Nike','Adidas','Zara','H&M'],
        'Books': ['Penguin', 'Random House'],
        'Home': ['IKEA','Wayfair'],
        'Sports': ['Nike','Adidas','Puma']
    }

    for i in range(num):
        category = random.choice(list(categories.keys()))
        brand = random.choice(categories[category])
        cost = round(random.uniform(5, 200), 2)
        price = round(cost * random.uniform(1.5, 2.5), 2)

        cursor.execute("""
                INSERT INTO ecommerce.products
               (product_name, category, brand, price, cost, stock_quantity)
               VALUES (%s, %s, %s, %s, %s, %s)
               RETURNING product_id
                """, (
                    f"{brand} {fake.word().title()}",
                    category,
                    brand,
                    price,
                    cost,
                    random.randint(0, 1000)
                ))
        
        products.append(cursor.fetchone()[0])

        if (i + 1) % 100 == 0:
            conn.commit()
            print(f" Created {i + 1} products")
    

    conn.commit()
    print(f"  Created{len(products)} products\n")
    return products

def generate_orders(conn, customers, products, num=5000):
    cursor = conn.cursor()

    print(f"Generating {num} orders...")

    statuses = ['pending','processing','shipped','delivered','cancelled']
    weights = [0.05, 0.10, 0.15, 0.65, 0.05]
    payments = ['credit_card','debit_card','paypal']

    for i in range(num):
        customer_id = random.choice(customers)
        order_date = fake.date_time_between(start_date='-1y', end_date='now')
        status = random.choices(statuses, weights=weights)[0]

        cursor.execute("""
            INSERT INTO ecommerce.orders
            (customer_id, order_date, status, payment_method, shipping_address, total_amount, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING order_id
        """, (
            customer_id,
            order_date,
            status,
            random.choice(payments),
            fake.address(),
            0,
            order_date,
            order_date
        ))

        order_id = cursor.fetchone()[0]

        num_items = random.randint(1, 5)
        total = 0

        for _ in range(num_items):
            product_id = random.choice(products)
            cursor.execute("SELECT price FROM ecommerce.products WHERE product_id = %s", (product_id,))
            price = float(cursor.fetchone()[0])

            quantity = random.randint(1, 3)
            discount = random.choice([0, 0, 0, 5, 10, 15])

            amount = price * quantity * (1 - discount/100)
            total += amount

            cursor.execute("""
                INSERT INTO ecommerce.order_items
                (order_id, product_id, quantity, unit_price, discount_percent)
                VALUES (%s, %s, %s, %s, %s)
                """, (
                    order_id,
                    product_id,
                    quantity,
                    price,
                    discount
                ))

            cursor.execute("UPDATE ecommerce.orders SET total_amount = %s WHERE order_id = %s", (round(total, 2), order_id))

            if (i + 1) % 500 == 0:
                conn.commit()
                print(f" Created {i + 1} orders")
    
    conn.commit()
    print(f" Created {num} orders\n")


if __name__ == "__main__":
    print("=" * 60)
    print("E-COMMERCE DATA GENERATOR")
    print("=" * 60 + "\n")

    conn = connect_db()

    customers = generate_customers(conn, 1000)
    products = generate_products(conn, 500)
    generate_orders(conn, customers, products, 5000)

    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM ecommerce.customers")
    print(f"Total Customers: {cursor.fetchone()[0]:,}")

    cursor.execute("SELECT COUNT(*) FROM ecommerce.products")
    print(f"Total Products: {cursor.fetchone()[0]:,}")

    cursor.execute("SELECT COUNT(*) FROM ecommerce.orders")
    print(f"Total Orders: {cursor.fetchone()[0]:,}")

    cursor.execute("SELECT COUNT(*) FROM ecommerce.order_items")
    print(f"Total order Items: {cursor.fetchone()[0]:,}")

    cursor.execute("SELECT SUM(total_amount) FROM ecommerce.orders WHERE status = 'delivered'")
    revenue = cursor.fetchone()[0] or 0
    print(f"Total Revenue: ${revenue:,.2f} ")

    print("\n" + "=" * 60)
    print("DATA GENERATION COMPLETE!")
    print("=" * 60)

    conn.close()