import csv
from faker import Faker

fake = Faker()

# Function to generate users
def generate_users(num_users):
    users = []
    for _ in range(num_users):
        user = {
            "name": fake.name(),
            "email": fake.email(),
            "age": fake.random_int(min=18, max=80),  # Random age between 18 and 80
            "nationality": fake.country(),
            "sex": fake.random_element(elements=("Male", "Female"))
        }
        users.append(user)
    return users

# Function to save data to CSV file
def save_to_csv(data, filename):
    with open(filename, 'w', newline='') as file:
        fieldnames = ["name", "email", "age", "nationality", "sex"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

# Generate and save users
num_users = 150
users = generate_users(num_users)
save_to_csv(users, 'users.csv')


