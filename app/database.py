import sqlite3
import pandas as pd

import yaml

from app.schemas.request import UserRequest


class DBHelper:
    def __init__(self, config=None):
        """Initialize the database and create required tables."""
        if config is None:
            with open("./app/config.yml", "r") as f:
                self.config = yaml.safe_load(f)
        else:
            self.config = config
        self.conn = sqlite3.connect(self.config["DB_NAME"])
        self.cursor = self.conn.cursor()
        self.create_tables()
        self.populate_products_table()

    def create_tables(self):
        """Create required tables if they don't exist."""
        # Users Table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Products Table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                sku TEXT UNIQUE NOT NULL,
                price REAL NOT NULL,
                stock INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Orders Table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                total_price REAL NOT NULL,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(id),
                FOREIGN KEY(product_id) REFERENCES products(id)
            )
        """)

        # Order Items Table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS order_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                FOREIGN KEY(order_id) REFERENCES orders(id),
                FOREIGN KEY(product_id) REFERENCES products(id)
            )
        """)

        self.conn.commit()

    def populate_products_table(self):
        self.cursor.execute("SELECT count(*) FROM products")
        res = self.cursor.fetchone()
        if res[0] == 0:
            self.insert_products_from_csv()

    def insert_products_from_csv(self):
        """Insert products from a CSV file into the products table."""
        df = pd.read_csv("./app/data/products.csv")
        for index, row in df.iterrows():
            self.cursor.execute(
                "INSERT INTO products (name, sku, price, stock) VALUES (?, ?, ?, ?)",
                (row["name"], row["sku"], float(row["price"]), int(row["stock"])),
            )
        self.conn.commit()

    def get_db(self):
        """Returns a new database connection."""
        conn = sqlite3.connect(self.config["DB_NAME"])
        return conn, conn.cursor()

    def get_user(self, email: str):
        """Fetch user details by user_id."""
        self.cursor.execute(
            "SELECT name, email, password FROM users WHERE email = ?", (email,)
        )
        user = self.cursor.fetchone()
        if user:
            name, email, password = user
            user = UserRequest(name=name, email=email, password=password)
        return user

    def validate_user(self, email: str, password: str):
        """Fetch user details by user_id."""
        self.cursor.execute(
            "SELECT id FROM users WHERE email = ? and password = ?", (email, password)
        )
        user = self.cursor.fetchone()
        self.conn.close()
        return user

    def add_user(self, user: UserRequest):
        """Check if user exists, else insert a new user into the database."""
        if self.get_user(user.email):
            return {"message": "User already exists."}
        try:
            self.cursor.execute(
                "INSERT INTO users (name, email, password) VALUES (?, ?, ?)",
                (user.name, user.email, user.password),
            )
            self.conn.commit()
        except:
            return None
        return {"message": "User registered successfully!"}

    def get_product_by_sku(self, sku: str):
        """Fetch product details by SKU."""
        self.cursor.execute(
            "SELECT id, name, price, stock FROM products WHERE sku = ?", (sku,)
        )
        product = self.cursor.fetchone()
        return product

    def update_stock(self, product_id: int, quantity: int):
        """Reduce product stock after an order."""
        self.cursor.execute(
            "UPDATE products SET stock = stock - ? WHERE id = ?", (quantity, product_id)
        )
        self.conn.commit()

    def get_schema(self):
        """Retrieve the schema of the database (tables and columns)."""
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = self.cursor.fetchall()
        schema = {}
        for table in tables:
            table_name = table[0]
            self.cursor.execute(f"PRAGMA table_info({table_name});")
            columns = [column[1] for column in self.cursor.fetchall()]
            schema[table_name] = columns
        return schema

    def close(self):
        """Close the database connection."""
        self.connection.close()
