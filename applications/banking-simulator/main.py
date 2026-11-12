"""Synthetic, non-production banking source data generator."""
import argparse
import os
import random
import time
import uuid
from contextlib import ExitStack, contextmanager
from decimal import Decimal
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from faker import Faker

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / '.env')
DBS = ['core_banking', 'payment_processing', 'card_management', 'loan_management', 'customer_management', 'branch_management']
SEED_BATCH_SIZE = 1000

@contextmanager
def connect(db):
    with psycopg.connect(host=os.getenv('POSTGRES_HOST', '127.0.0.1'),
                         port=int(os.getenv('POSTGRES_PORT', '5432')),
                         user=os.getenv('POSTGRES_USER', 'bedp_admin'),
                         password=os.environ['POSTGRES_PASSWORD'], dbname=db) as conn:
        yield conn

def seed(count):
    rng = random.Random(int(os.getenv('SIMULATOR_SEED', '42')))
    fake = Faker('en_US')
    fake.seed_instance(int(os.getenv('SIMULATOR_SEED', '42')))
    branches = [(1,'Colombo Central','Colombo'),(2,'Kandy City','Kandy'),(3,'Galle Fort','Galle'),(4,'Negombo City','Negombo'),(5,'Jaffna City','Jaffna')]
    with connect('branch_management') as conn:
        with conn.cursor() as cur:
            for bid,name,city in branches:
                cur.execute('INSERT INTO branches(branch_id,branch_name,city,opened_at) VALUES (%s,%s,%s,%s) ON CONFLICT (branch_id) DO NOTHING', (bid,name,city,'2020-01-01'))
    with ExitStack() as stack:
        connections = {
            db: stack.enter_context(connect(db))
            for db in ('customer_management', 'core_banking', 'card_management', 'loan_management')
        }
        cursors = {
            db: stack.enter_context(conn.cursor())
            for db, conn in connections.items()
        }
        for start in range(1, count + 1, SEED_BATCH_SIZE):
            customer_rows = []
            account_rows = []
            card_rows = []
            loan_rows = []
            end = min(start + SEED_BATCH_SIZE, count + 1)
            for cid in range(start, end):
                name = fake.name()
                email = f'customer{cid}@example.invalid'
                city = rng.choice(branches)[2]
                customer_rows.append((cid,name,email,'+94000000000',city))
                account_rows.append((100000+cid,cid,rng.randint(1,5),'SAVINGS',Decimal(rng.randint(10000,100000))))
                card_rows.append((200000+cid,100000+cid,cid,f'SYNTHETIC-CARD-{cid}',f'{cid%10000:04d}','DEBIT'))
                principal = Decimal(rng.randint(100000,1000000))
                loan_rows.append((300000+cid,cid,principal,principal,Decimal('12.50')))
            cursors['customer_management'].executemany(
                'INSERT INTO customers(customer_id,full_name,email,phone,city) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (customer_id) DO NOTHING',
                customer_rows)
            cursors['core_banking'].executemany(
                'INSERT INTO accounts(account_id,customer_id,branch_id,account_type,balance) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (account_id) DO NOTHING',
                account_rows)
            cursors['card_management'].executemany(
                'INSERT INTO cards(card_id,account_id,customer_id,card_token,last_four,card_type) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (card_id) DO NOTHING',
                card_rows)
            cursors['loan_management'].executemany(
                'INSERT INTO loans(loan_id,customer_id,principal,outstanding,interest_rate) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (loan_id) DO NOTHING',
                loan_rows)
            for conn in connections.values():
                conn.commit()
            if end - 1 == count or (end - 1) % 10000 == 0:
                print(f'Seed progress: {end - 1}/{count} customers', flush=True)
    print(f'Seed complete: up to {count} customers/accounts/cards/loans and {len(branches)} branches (idempotent).')

def activity(rng):
    with connect('core_banking') as conn:
        row = conn.execute('SELECT account_id,customer_id FROM accounts ORDER BY random() LIMIT 1').fetchone()
    if not row:
        raise RuntimeError('No accounts found. Run seed first.')
    account_id, customer_id = row
    kind = rng.choice(['DEPOSIT','WITHDRAWAL','ADDRESS_UPDATE'])
    if kind == 'ADDRESS_UPDATE':
        city = rng.choice(['Colombo','Kandy','Galle','Negombo','Jaffna'])
        with connect('customer_management') as conn:
            conn.execute('UPDATE customers SET city=%s,updated_at=now() WHERE customer_id=%s',(city,customer_id))
        print(f'CUSTOMER_UPDATE customer={customer_id} city={city}', flush=True)
        return
    amount = Decimal(rng.randint(100,5000))
    # Update one account atomically in its source DB. Cross-database event publishing
    # is a demonstration only; production needs an outbox/reconciliation design.
    with connect('core_banking') as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT balance FROM accounts WHERE account_id=%s FOR UPDATE',(account_id,))
            balance = cur.fetchone()[0]
            if kind == 'WITHDRAWAL' and balance < amount:
                print('WITHDRAWAL skipped: insufficient funds', flush=True)
                return
            new_balance = balance + amount if kind == 'DEPOSIT' else balance - amount
            cur.execute('UPDATE accounts SET balance=%s,updated_at=now() WHERE account_id=%s',(new_balance,account_id))
    event_id = uuid.uuid4()
    with connect('payment_processing') as conn:
        conn.execute('INSERT INTO transactions(account_id,customer_id,transaction_type,amount,balance_after,event_id) VALUES (%s,%s,%s,%s,%s,%s)',(account_id,customer_id,kind,amount,new_balance,event_id))
    print(f'{kind} account={account_id} amount=LKR {amount} balance=LKR {new_balance}',flush=True)

def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='command',required=True)
    pseed = sub.add_parser('seed')
    pseed.add_argument('--customers',type=int,default=100)
    prun = sub.add_parser('run')
    prun.add_argument('--interval',type=float,default=3)
    prun.add_argument('--count',type=int,default=0,help='0 means continuous')
    args = parser.parse_args()
    if args.command == 'seed':
        if args.customers < 1:
            parser.error('--customers must be positive')
        seed(args.customers)
    else:
        rng = random.Random()
        if args.interval < 0 or args.count < 0:
            parser.error('--interval and --count must be nonnegative')
        try:
            i = 0
            while args.count == 0 or i < args.count:
                activity(rng)
                i += 1
                if args.count == 0 or i < args.count:
                    time.sleep(args.interval)
        except KeyboardInterrupt:
            print('\nSimulator stopped.')

if __name__ == '__main__':
    main()
