# Imports
import pandas as pd
from pyravendb.store import document_store


# Represents a kpop sale
class KpopSale(object):
    def __init__(self, artist, title, date, country, sales, peak_chart):
        self.artist = artist
        self.title = title
        self.date = date
        self.country = country
        self.sales = sales
        self.peak_chart = peak_chart


# Load data from CSV
sales = pd.read_csv("./data/kpop_sales.csv")


# Convert dataframe rows to list of objects
list_sales = []
for id, row in sales.iterrows():
    list_sales.append(
        KpopSale(
            row.artist, row.title, row.date, row.country, row.sales, row.peak_chart
        )
    )


# Get access to the database
store = document_store.DocumentStore(urls=["http://localhost:8080"], database="Kpop")
store.initialize()


# Open a connexion to the database
with store.open_session() as session:
    # Remove all previous documents from the database
    for i in range(len(list_sales)):
        session.delete(f"sales/{i}")

    # Save the changes
    session.save_changes()

    # Store all the sales in the database
    for i in range(len(list_sales)):
        session.store(list_sales[i], f"sales/{i}")

    # Save the changes
    session.save_changes()
