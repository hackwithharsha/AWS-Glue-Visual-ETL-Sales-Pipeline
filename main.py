import csv
import os
import random
from faker import Faker
from datetime import datetime
from collections import defaultdict
from loguru import logger

fake = Faker()
Faker.seed(42)
random.seed(42)

base_output_dir = "data"

num_rows = 500
partitioned_data = defaultdict(list)

for _ in range(num_rows):
    order_id = fake.uuid4() if random.random() > 0.05 else ""
    order_date = fake.date_between(start_date="-90d", end_date="today")
    country = fake.country()
    currency = random.choice(["USD", "EUR", "INR", "GBP"])
    amount = round(random.uniform(20.0, 1000.0), 2)

    year = order_date.year
    month = f"{order_date.month:02d}"
    day = f"{order_date.day:02d}"

    row = [order_id, order_date.strftime("%Y-%m-%d"), country, amount, currency]

    partition_key = (year, month, day)
    partitioned_data[partition_key].append(row)

for (year, month, day), rows in partitioned_data.items():
    dir_path = os.path.join(base_output_dir, f"year={year}", f"month={month}", f"day={day}")
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, "sales_data.csv")

    with open(file_path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["order_id", "order_date", "country", "amount", "currency"])
        writer.writerows(rows)

logger.info(f"✅ Partitioned sales data written to: {base_output_dir}")
