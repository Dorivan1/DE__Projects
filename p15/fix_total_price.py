import pandas as pd


order_items = pd.read_csv('order_items.csv')
order_items['total_price'] = order_items['quantity'] * order_items['price']

order_items.to_csv('order_items_fixed.csv', index=False)

print("Total price has been corrected and saved to 'order_items_fixed.csv'.")

