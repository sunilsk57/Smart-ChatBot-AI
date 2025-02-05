import sqlite3

from app.schemas.request import UserRequest


DB_NAME = "Greenlife.db"


class DBHelper:
    def __init__(self):
        """Initialize the database and create required tables."""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()

        # Users Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Products Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                sku TEXT UNIQUE NOT NULL,
                price REAL NOT NULL,
                stock INTEGER NOT NULL
            )
        """)

        # Orders Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                total_price REAL NOT NULL,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
        """)

        # Order Items Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS order_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                FOREIGN KEY(order_id) REFERENCES orders(id),
                FOREIGN KEY(product_id) REFERENCES products(id)
            )
        """)

        conn.commit()
        conn.close()

    def get_db(self):
        """Returns a new database connection."""
        conn = sqlite3.connect(DB_NAME)
        return conn, conn.cursor()

    def get_user(self, email: str):
        """Fetch user details by user_id."""
        conn, cursor = self.get_db()
        cursor.execute(
            "SELECT name, email, password FROM users WHERE email = ?", (email,)
        )
        user = cursor.fetchone()
        conn.close()
        if user:
            name, email, password = user
            user = UserRequest(name=name, email=email, password=password)
        return user

    def validate_user(self, email: str, password: str):
        """Fetch user details by user_id."""
        conn, cursor = self.get_db()
        cursor.execute(
            "SELECT id FROM users WHERE email = ? and password = ?", (email, password)
        )
        user = cursor.fetchone()
        conn.close()
        return user

    def add_user(self, user: UserRequest):
        """Check if user exists, else insert a new user into the database."""
        if self.get_user(user.email):
            return {"message": "User already exists."}
        try:
            conn, cursor = self.get_db()
            cursor.execute(
                "INSERT INTO users (name, email, password) VALUES (?, ?, ?)",
                (user.name, user.email, user.password),
            )
            conn.commit()
            conn.close()
        except:
            return None
        return {"message": "User registered successfully!"}

    def get_product_by_sku(self, sku: str):
        """Fetch product details by SKU."""
        conn, cursor = self.get_db()
        cursor.execute(
            "SELECT id, name, price, stock FROM products WHERE sku = ?", (sku,)
        )
        product = cursor.fetchone()
        conn.close()
        return product

    def update_stock(self, product_id: int, quantity: int):
        """Reduce product stock after an order."""
        conn, cursor = self.get_db()
        cursor.execute(
            "UPDATE products SET stock = stock - ? WHERE id = ?", (quantity, product_id)
        )
        conn.commit()
        conn.close()
