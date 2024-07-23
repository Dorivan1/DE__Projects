import requests
import json
import os

# Base URL for the API
BASE_URL = "https://fake-e-commerce-api.onrender.com"

# API endpoints
PRODUCTS_ENDPOINT = f"{BASE_URL}/product"

# Function to fetch data from the API
def fetch_data(session, api_url):
    try:
        response = session.get(api_url)
        response.raise_for_status()
        print(f"Response text from {api_url}: {response.text}")
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Error occurred: {err}")
    except ValueError:
        print(f"Invalid JSON response from {api_url}: {response.text}")
    return None

# Function to save data to a JSON file
def save_data(data, filename):
    os.makedirs("data", exist_ok=True)
    filepath = os.path.join("data", filename)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)
    print(f"Data saved to {filepath}")

# Main function to fetch and save product data
def main():
    with requests.Session() as session:
        print("Fetching products data...")
        products = fetch_data(session, PRODUCTS_ENDPOINT)
        if products is not None:
            print(f"Fetched {len(products)} products.")
            save_data(products, "products.json")
        else:
            print("Failed to fetch products data.")

if __name__ == "__main__":
    main()