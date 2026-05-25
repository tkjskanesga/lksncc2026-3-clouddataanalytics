"""
generate_sample_data.py
Script to generate sample CSV data and upload to S3 RAW_BUCKET.

Run once before deploy if data/ folder does not exist:
  python3 generate_sample_data.py --bucket nusa-raw-data-ACCOUNTID --region ap-southeast-1

OR: ensure data/ folder contains:
  data/transactions.csv
  data/shipments.csv
  data/sellers.csv
  data/user_events.csv
"""

import csv, uuid, random, argparse, os, subprocess
from datetime import datetime, timedelta

random.seed(42)

def random_date(start_days_ago=180, end_days_ago=0):
    d = datetime.utcnow() - timedelta(days=random.randint(end_days_ago, start_days_ago))
    return d.strftime('%Y-%m-%d')

def random_ts(start_days_ago=180):
    d = datetime.utcnow() - timedelta(
        days=random.randint(0, start_days_ago),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    return d.strftime('%Y-%m-%dT%H:%M:%S')

PROVINCES = ['DKI JAKARTA','JAWA BARAT','JAWA TENGAH','JAWA TIMUR','BANTEN',
             'SUMATERA UTARA','SULAWESI SELATAN','KALIMANTAN TIMUR']
CATEGORIES = ['ELECTRONICS','FASHION','FOOD','HOME','BEAUTY','SPORTS','BOOKS','TOYS']
PAYMENT    = ['BANK_TRANSFER','CREDIT_CARD','E_WALLET','COD','INSTALLMENT']
COURIERS   = ['JNE','SICEPAT','ANTERAJA','TIKI','GOSEND']
TIERS      = ['BRONZE','SILVER','GOLD','PLATINUM']

def gen_sellers(n=50):
    rows = []
    for i in range(n):
        rows.append({
            'seller_id':      str(uuid.uuid4()),
            'seller_name':    f'Toko {chr(65+i%26)}{i}',
            'tier':           random.choice(TIERS),
            'province':       random.choice(PROVINCES),
            'join_date':      random_date(720, 30),
            'total_products': random.randint(5, 500),
            'rating':         round(random.uniform(3.0, 5.0), 2),
            'is_official':    random.choice(['true', 'false'])
        })
    return rows

def gen_transactions(sellers, n=500):
    rows = []
    seller_ids = [s['seller_id'] for s in sellers]
    for _ in range(n):
        qty   = random.randint(1, 10)
        price = round(random.uniform(10000, 5000000), 2)
        rows.append({
            'transaction_id': str(uuid.uuid4()),
            'order_date':     random_date(180, 0),
            'seller_id':      random.choice(seller_ids),
            'buyer_id':       str(uuid.uuid4()),
            'product_id':     str(uuid.uuid4()),
            'category':       random.choice(CATEGORIES),
            'province':       random.choice(PROVINCES),
            'quantity':       qty,
            'unit_price':     price,
            'total_amount':   round(qty * price, 2),
            'payment_method': random.choice(PAYMENT),
            'status':         random.choice(['COMPLETED','COMPLETED','COMPLETED','CANCELLED','PENDING']),
            'created_at':     random_ts(180)
        })
    return rows

def gen_shipments(transactions, n=400):
    txn_ids = [t['transaction_id'] for t in transactions[:n]]
    rows = []
    for tid in txn_ids:
        pickup = random_date(60, 0)
        rows.append({
            'shipment_id':       str(uuid.uuid4()),
            'transaction_id':    tid,
            'courier':           random.choice(COURIERS),
            'origin_province':   random.choice(PROVINCES),
            'dest_province':     random.choice(PROVINCES),
            'weight_kg':         round(random.uniform(0.1, 30.0), 2),
            'shipping_cost':     round(random.uniform(5000, 150000), 2),
            'pickup_date':       pickup,
            'estimated_arrival': random_date(30, 0),
            'actual_arrival':    random_date(25, 0),
            'status':            random.choice(['DELIVERED','DELIVERED','IN_TRANSIT','PICKED_UP','RETURNED'])
        })
    return rows

def gen_user_events(n=1000):
    event_types = ['PAGE_VIEW','PAGE_VIEW','PAGE_VIEW','ADD_TO_CART','ADD_TO_CART','CHECKOUT','PURCHASE']
    rows = []
    for _ in range(n):
        rows.append({
            'event_id':        str(uuid.uuid4()),
            'user_id':         str(uuid.uuid4()),
            'event_type':      random.choice(event_types),
            'event_timestamp': random_ts(7),
            'platform':        random.choice(['WEB','ANDROID','IOS']),
            'session_id':      str(uuid.uuid4()),
            'product_id':      str(uuid.uuid4()),
            'amount':          round(random.uniform(0, 2000000), 2)
        })
    return rows

def write_csv(rows, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    print(f"  Written: {path} ({len(rows)} rows)")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket',  help='S3 RAW_BUCKET name (optional, uploads if provided)')
    parser.add_argument('--region',  default='ap-southeast-1')
    parser.add_argument('--outdir',  default='data')
    args = parser.parse_args()

    print("[INFO] Generating sample data...")
    sellers      = gen_sellers(50)
    transactions = gen_transactions(sellers, 500)
    shipments    = gen_shipments(transactions, 400)
    user_events  = gen_user_events(1000)

    write_csv(sellers,      f'{args.outdir}/sellers.csv')
    write_csv(transactions, f'{args.outdir}/transactions.csv')
    write_csv(shipments,    f'{args.outdir}/shipments.csv')
    write_csv(user_events,  f'{args.outdir}/user_events.csv')

    if args.bucket:
        print(f"\n[INFO] Uploading to s3://{args.bucket}/...")
        for fname, key in [
            ('sellers.csv',      'sellers/sellers.csv'),
            ('transactions.csv', 'transactions/transactions.csv'),
            ('shipments.csv',    'shipments/shipments.csv'),
            ('user_events.csv',  'user_events/user_events.csv'),
        ]:
            src = f'{args.outdir}/{fname}'
            cmd = ['aws', 's3', 'cp', src, f's3://{args.bucket}/{key}', '--region', args.region]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"  Uploaded: {key}")
            else:
                print(f"  FAILED:   {key} — {result.stderr.strip()}")
    else:
        print(f"\n[INFO] Data saved to folder '{args.outdir}/'")
        print("[INFO] Upload manual: aws s3 cp data/transactions.csv s3://BUCKET/transactions/transactions.csv")

if __name__ == '__main__':
    main()
