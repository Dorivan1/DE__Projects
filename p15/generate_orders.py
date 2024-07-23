import csv
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()


def load_users(filename):
    users = []
    with open(filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            users.append(row)
    return users


def load_products(filename):
    products = []
    with open(filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            products.append(row)
    return products


def generate_orders(num_orders, user_emails, product_ids, products):
    orders = []
    order_items = []
    order_id = 1
    order_item_id = 1
    
    for _ in range(num_orders):
        user_email = random.choice(user_emails)
        order_date = fake.date_between(start_date='-2y', end_date='today')
        order_status = random.choice(["Pending", "Shipped", "Delivered", "Cancelled"])
        
       
        orders.append({
            "order_id": order_id,
            "user_email": user_email,
            "order_date": order_date,
            "order_status": order_status
        })
        
      
        num_items = random.randint(1, 5)
        for _ in range(num_items):
            product_id = random.choice(product_ids)
            product_price = next(item['price'] for item in products if item['id'] == product_id)
            quantity = random.randint(1, 5)
            total_price = product_price * quantity
            
           
            order_items.append({
                "order_item_id": order_item_id,
                "order_id": order_id,
                "product_id": product_id,
                "quantity": quantity,
                "price": product_price,
                "total_price": total_price
            })
            order_item_id += 1
        
        order_id += 1
    
    return orders, order_items


def save_to_csv(data, filename, fieldnames):
    with open(filename, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


users = load_users('users.csv')
products = load_products('products_transformed.csv')

user_emails = [user['email'] for user in users]
product_ids = [product['id'] for product in products]

num_orders = 700
orders, order_items = generate_orders(num_orders, user_emails, product_ids, products)

save_to_csv(orders, 'orders.csv', fieldnames=["order_id", "user_email", "order_date", "order_status"])
save_to_csv(order_items, 'order_items.csv', fieldnames=["order_item_id", "order_id", "product_id", "quantity", "price", "total_price"])