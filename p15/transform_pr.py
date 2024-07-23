import json
import csv
import random

# Function to load products from JSON file
def load_products(filename):
    with open(filename, 'r') as file:
        return json.load(file)

# Function to transform products and generate random reviews
def transform_products(products):
    transformed_data = []
    for product in products:
        transformed_product = {
            "id": product["_id"],
            "name": product["name"],
            "price": product["price"],
            "category": product.get("category", "Unknown"),  # Get category, default to "Unknown" if not present
            "rating": round(random.uniform(1, 5), 2),  # Random rating between 1 and 5, rounded to 2 decimal places
            "numberOfReviews": random.randint(1, 1000)  # Random number of reviews between 1 and 1000
        }
        transformed_data.append(transformed_product)
    return transformed_data

# Function to save transformed data to CSV file
def save_to_csv(data, filename):
    with open(filename, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=["id", "name", "price", "category", "rating", "numberOfReviews"])
        writer.writeheader()
        writer.writerows(data)

# Main script
if __name__ == "__main__":
    products = load_products('data/products.json')
    transformed_products = transform_products(products)
    save_to_csv(transformed_products, 'products_transformed.csv')