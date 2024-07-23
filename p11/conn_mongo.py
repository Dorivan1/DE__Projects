# import pymongo

# # Connect to MongoDB
# client = pymongo.MongoClient("mongodb://localhost:27017/")

# # Create a database
# mydb = client["mydatabase"]

# # Create a collection
# mycol = mydb["customers"]

# # Insert a document into the collection
# mydict = { "name": "John", "address": "Highway 37" }
# x = mycol.insert_one(mydict)

# # Print the inserted document's ID
# print(x.inserted_id)
import pymongo

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
# Access the database
mydb = client["mydatabase"]
# Access the collection
mycol = mydb["processed_data"]

# Find all documents in the collection and print them
for document in mycol.find():
    print(document)