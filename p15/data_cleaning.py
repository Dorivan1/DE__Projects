import csv

def remove_order_item_id_in_place(file_path):
    # Read the original data
    with open(file_path, 'r') as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)
        fieldnames = [field for field in reader.fieldnames if field != 'order_item_id']

    # Write the modified data back to the same file
    with open(file_path, 'w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            del row['order_item_id']
            writer.writerow(row)

# Path to  CSV file
file_path = 'order_items.csv'
remove_order_item_id_in_place(file_path)