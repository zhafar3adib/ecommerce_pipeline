import pandas as pd
import random
from datetime import datetime, timedelta
from faker import Faker
import os

fake = Faker("en_US")

#random choice
channels = ["website", "ecommerce_1", "ecommerce_2", "ecommerce_3", "ecommerce_4"]
statuses = ["paid", "returned", "arrived", "cancel"]
sent_to_location = [
    'aceh',
    'bali',
    'bangka belitung',
    'banten',
    'bengkulu',
    'daerah istimewa yogyakarta',
    'dki jakarta',
    'gorontalo',
    'jambi',
    'jawa barat',
    'jawa tengah',
    'jawa timur',
    'kalimantan barat',
    'kalimantan selatan',
    'kalimantan tengah',
    'kalimantan timur',
    'kalimantan utara',
    'kepulauan bangka belitung',
    'kepulauan riau',
    'lampung',
    'maluku',
    'maluku utara',
    'nusa tenggara barat',
    'nusa tenggara timur',
    'papua',
    'papua barat',
    'riau',
    'sulawesi barat',
    'sulawesi selatan',
    'sulawesi tengah',
    'sulawesi tenggara',
    'sulawesi utara',
    'sumatera barat',
    'sumatera selatan',
    'sumatera utara'
]

#dummy data generator
def generate_transaction_id():
    return fake.uuid4()[:8].upper()

def generate_random_data(n_transactions=10000, max_products=20, days_back=365, n_customers=1000):
    data = []
    used_ids = set()
    email_pool = [fake.email() for _ in range(n_customers)]

    paid_dates = [datetime.now() - timedelta(days=random.randint(0, days_back)) for _ in range(n_transactions)]

    for i in range(n_transactions):
        trx_id = generate_transaction_id()
        while trx_id in used_ids:
            trx_id = generate_transaction_id()
        used_ids.add(trx_id)

        base_date = paid_dates[i].replace(hour=0, minute=0, second=0)
        paid_at = base_date + timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )

        channel = random.choice(channels)
        status = random.choice(statuses)
        sent_to = random.choice(sent_to_location)
        email = random.choice(email_pool)

        n_products = random.randint(1, max_products)
        for _ in range(n_products):
            product = fake.word(ext_word_list=["shirt", "pants", "dress", "blouse", "skirt", "shoes","kids"])
            quantity = random.randint(1, 10)
            price = random.randint(50000, 500000)
            revenue = quantity * price
            color = fake.color_name()

            data.append({
                "transaction_id": trx_id,
                "channel": channel,
                "email": email,
                "product": product,
                "quantity": quantity,
                "revenue": revenue,
                "paid_at": paid_at,
                "status": status,
                "color": color,
                "sent_to": sent_to
            })
    return pd.DataFrame(data)

#generate discount list
def generate_discount_list():
    data = [
        {"campaign_name": "Eid al-Fitr Sale 2025", "start_date": datetime(2025, 3, 20, 0, 0, 0), "end_date": datetime(2025, 3, 31, 23, 59, 59), "discount": 30},
        {"campaign_name": "Eid al-Adha Sale 2025", "start_date": datetime(2025, 6, 5, 0, 0, 0), "end_date": datetime(2025, 6, 7, 23, 59, 59), "discount": 30},
        {"campaign_name": "Mid Year Sale 2025", "start_date": datetime(2025, 6, 20, 0, 0, 0), "end_date": datetime(2025, 6, 30, 23, 59, 59), "discount": 25},
        {"campaign_name": "End Year Sale 2024", "start_date": datetime(2024, 12, 20, 0, 0, 0), "end_date": datetime(2024, 12, 31, 23, 59, 59), "discount": 35},
        {"campaign_name": "January Payday 2025", "start_date": datetime(2025, 1, 25, 0, 0, 0), "end_date": datetime(2025, 1, 31, 23, 59, 59), "discount": 20},
        {"campaign_name": "February Payday 2025", "start_date": datetime(2025, 2, 22, 0, 0, 0), "end_date": datetime(2025, 2, 28, 23, 59, 59), "discount": 20},
        {"campaign_name": "April Payday 2025", "start_date": datetime(2025, 4, 24, 0, 0, 0), "end_date": datetime(2025, 4, 30, 23, 59, 59), "discount": 20},
        {"campaign_name": "May Payday 2025", "start_date": datetime(2025, 5, 25, 0, 0, 0), "end_date": datetime(2025, 5, 31, 23, 59, 59), "discount": 20},
        {"campaign_name": "July Payday 2025", "start_date": datetime(2025, 7, 25, 0, 0, 0), "end_date": datetime(2025, 7, 31, 23, 59, 59), "discount": 20},
        {"campaign_name": "August Payday 2025", "start_date": datetime(2025, 8, 25, 0, 0, 0), "end_date": datetime(2025, 8, 31, 23, 59, 59), "discount": 20},
        {"campaign_name": "September Payday 2025", "start_date": datetime(2025, 9, 24, 0, 0, 0), "end_date": datetime(2025, 9, 30, 23, 59, 59), "discount": 20},
        {"campaign_name": "Independence Sale 2025", "start_date": datetime(2025, 8, 15, 0, 0, 0), "end_date": datetime(2025, 8, 18, 23, 59, 59), "discount": 17},
        {"campaign_name": "September Payday 2024", "start_date": datetime(2024, 9, 24, 0, 0, 0), "end_date": datetime(2024, 9, 30, 23, 59, 59), "discount": 20},
        {"campaign_name": "October Payday 2024", "start_date": datetime(2024, 10, 25, 0, 0, 0), "end_date": datetime(2024, 10, 31, 23, 59, 59), "discount": 20},
        {"campaign_name": "November Payday 2024", "start_date": datetime(2024, 11, 24, 0, 0, 0), "end_date": datetime(2024, 11, 30, 23, 59, 59), "discount": 20}
    ]
    return pd.DataFrame(data)

def enrich_with_discount(transactions_df):
    list_discount = generate_discount_list()
    result = []

    for _, campaign in list_discount.iterrows():
        mask = (transactions_df["paid_at"] >= campaign["start_date"]) & (transactions_df["paid_at"] <= campaign["end_date"])
        temp = transactions_df.loc[mask, ["transaction_id"]].copy()
        temp["campaign_name"] = campaign["campaign_name"]
        temp["discount"] = campaign["discount"]
        result.append(temp)

    if result:
        return pd.concat(result, ignore_index=True).drop_duplicates()
    else:
        return pd.DataFrame(columns=["transaction_id", "campaign_name", "discount"])

#save to csv
def save_csv(df, output_dir="/opt/airflow/generated_dummy_data", prefix="transactions"):
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"{prefix}_{ts}.csv"
    path = os.path.join(output_dir, fname)

    df2 = df.copy()

    df2.to_csv(path, index=False)
    return path

if __name__ == "__main__":
    df_trx = generate_random_data(n_transactions=10000, max_products=20, n_customers=1000)
    trx_path = save_csv(df_trx, prefix="transactions")
    print("transaction data saved to:", trx_path)

    df_diskon = enrich_with_discount(df_trx)
    diskon_path = save_csv(df_diskon, prefix="discounts")
    print("discounted transaction data saved to:", diskon_path)
