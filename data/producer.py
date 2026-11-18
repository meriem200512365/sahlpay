#!/usr/bin/env python3
"""
producer.py - Generateur de transactions SahlPay (donnees fictives).

Modes :
  --mode file  : ecrit N evenements dans transactions_history.jsonl
  --mode kafka : publie en continu dans le topic Kafka "transactions"

Cas reels simules :
  - ~5 % d'evenements en retard (event_time jusqu'a 15 min dans le passe)
  - ~2 % de doublons (meme tx_id et meme status)
  - 3 scenarios de fraude : appareil partage, transferts circulaires A->B->C->A,
    compte collecteur (nombreux petits virements recus)
La verite terrain des scenarios est ecrite dans fraud_ground_truth.json.
"""
import argparse
import json
import random
import sys
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from itertools import cycle
from pathlib import Path

HERE = Path(__file__).resolve().parent

# ville : (latitude, longitude, poids)
CITIES = {
    "Casablanca": (33.5731, -7.5898, 25), "Rabat": (34.0209, -6.8416, 18),
    "Marrakech": (31.6295, -7.9811, 12), "Fes": (34.0181, -5.0078, 10),
    "Tanger": (35.7595, -5.8340, 10), "Agadir": (30.4278, -9.5981, 7),
    "Meknes": (33.8935, -5.5473, 6), "Oujda": (34.6867, -1.9114, 4),
    "Kenitra": (34.2610, -6.5802, 4), "Tetouan": (35.5785, -5.3684, 4),
}
CATEGORIES = ["epicerie", "carburant", "telecom", "restauration", "pharmacie", "ecommerce"]
NAME_PARTS = {
    "epicerie": ["Epicerie Atlas", "Hanout Salam", "Superette Baraka"],
    "carburant": ["Station Atlas", "Station Oasis", "Station Horizon"],
    "telecom": ["Recharge Express", "Telecom Boutique", "Mobile Center"],
    "restauration": ["Cafe Andalous", "Restaurant Dar Salam", "Snack Marrakchi"],
    "pharmacie": ["Pharmacie Centrale", "Pharmacie Ibn Sina", "Pharmacie Al Amal"],
    "ecommerce": ["Boutique Online", "Shop Atlas", "Market Maroc"],
}


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_merchants(path, n=300, seed=7):
    rnd = random.Random(seed)
    names = list(CITIES)
    weights = [CITIES[c][2] for c in names]
    with open(path, "w", encoding="utf-8") as f:
        for i in range(1, n + 1):
            city = rnd.choices(names, weights)[0]
            lat, lon, _ = CITIES[city]
            cat = rnd.choice(CATEGORIES)
            rec = {
                "merchant_id": f"M-{i:03d}",
                "name": f"{rnd.choice(NAME_PARTS[cat])} {city} {i:03d}",
                "city": city,
                "category": cat,
                "latitude": round(lat + rnd.uniform(-0.05, 0.05), 5),
                "longitude": round(lon + rnd.uniform(-0.05, 0.05), 5),
            }
            f.write(json.dumps(rec) + "\n")
    print(f"[merchants] {n} commercants ecrits dans {path}")


class World:
    def __init__(self, merchants_path, seed):
        self.rnd = r = random.Random(seed)
        with open(merchants_path, encoding="utf-8") as f:
            self.merchants = [json.loads(line) for line in f if line.strip()]
        names = list(CITIES)
        weights = [CITIES[c][2] for c in names]
        self.wallets = [f"W-{10000 + i}" for i in range(1000)]
        self.home = {w: r.choices(names, weights)[0] for w in self.wallets}
        self.device = {w: f"D-{r.getrandbits(24):06x}" for w in self.wallets}
        # quelques paires de comptes legitimes qui partagent un appareil (faux positifs)
        for _ in range(15):
            a, b = r.sample(self.wallets, 2)
            self.device[b] = self.device[a]

        # scenarios de fraude injectes
        pool = r.sample(self.wallets, 12 + 3 + 1 + 40)
        self.shared_wallets = pool[:12]
        self.cycle_wallets = pool[12:15]
        self.collector = pool[15]
        self.collector_senders = pool[16:]
        self.shared_device = "D-c0ffee"
        a, b, c = self.cycle_wallets
        self._shared_iter = cycle(self.shared_wallets)
        self._cycle_iter = cycle([(a, b), (b, c), (c, a)])
        self.truth = {
            "shared_device": {"device_id": self.shared_device, "wallets": self.shared_wallets},
            "circular_transfers": {"wallets": self.cycle_wallets},
            "collector_account": {"wallet": self.collector, "senders": self.collector_senders},
        }
        self.tx_counter = 0

    def _lifecycle(self):
        x = self.rnd.random()
        if x < 0.88:
            return ["INITIATED", "AUTHORIZED", "COMPLETED"]
        if x < 0.97:
            return ["INITIATED", "REJECTED"]
        return ["INITIATED", "AUTHORIZED", "COMPLETED", "REVERSED"]

    @staticmethod
    def _base(tx_id, typ, payer, payee, device, city, channel, amount):
        return {"tx_id": tx_id, "type": typ, "payer_wallet": payer, "payee_id": payee,
                "device_id": device, "city": city, "channel": channel,
                "amount_mad": amount}

    def _fraud_tx(self, tx_id):
        r = self.rnd
        kind = r.choices(["shared", "cycle", "collector"], [35, 30, 35])[0]
        life = ["INITIATED", "AUTHORIZED", "COMPLETED"]
        if kind == "shared":
            payer = next(self._shared_iter)
            m = r.choice(self.merchants)
            amount = round(r.uniform(50, 800), 2)
            return self._base(tx_id, "P2M", payer, m["merchant_id"], self.shared_device,
                              m["city"], "QR", amount), life
        if kind == "cycle":
            payer, payee = next(self._cycle_iter)
            amount = round(r.uniform(1400, 1600), 2)
            return self._base(tx_id, "P2P", payer, payee, self.device[payer],
                              self.home[payer], "APP", amount), life
        payer = r.choice(self.collector_senders)
        amount = round(r.uniform(20, 100), 2)
        return self._base(tx_id, "P2P", payer, self.collector, self.device[payer],
                          self.home[payer], "APP", amount), life

    def new_tx(self, fraud_prob=0.03):
        r = self.rnd
        self.tx_counter += 1
        tx_id = f"TX-{self.tx_counter:08d}"
        if r.random() < fraud_prob:
            return self._fraud_tx(tx_id)
        payer = r.choice(self.wallets)
        if r.random() < 0.8:
            m = r.choice(self.merchants)
            typ, payee, city = "P2M", m["merchant_id"], m["city"]
            channel = r.choices(["QR", "APP", "USSD"], [80, 15, 5])[0]
        else:
            payee = r.choice(self.wallets)
            while payee == payer:
                payee = r.choice(self.wallets)
            typ, city = "P2P", self.home[payer]
            channel = r.choices(["APP", "USSD"], [70, 30])[0]
        amount = round(min(max(r.lognormvariate(4.6, 1.1), 1.0), 20000.0), 2)
        base = self._base(tx_id, typ, payer, payee, self.device[payer], city, channel, amount)
        return base, self._lifecycle()


def make_event(base, status, ts):
    ev = {k: base[k] for k in ("tx_id", "type", "payer_wallet", "payee_id",
                               "device_id", "city", "channel")}
    ev["status"] = status
    ev["amount_mad"] = base["amount_mad"]
    ev["event_time"] = iso(ts)
    return ev


def maybe_late(r, ts):
    """~5 % des evenements arrivent en retard (jusqu'a 15 min)."""
    if r.random() < 0.05:
        return ts - timedelta(seconds=r.uniform(60, 900))
    return ts


def write_truth(world, directory):
    path = Path(directory) / "fraud_ground_truth.json"
    path.write_text(json.dumps(world.truth, indent=2), encoding="utf-8")
    print(f"[truth] verite terrain des scenarios : {path}")


def run_file(world, n, out, days):
    r = world.rnd
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    rows = []
    while len(rows) < n:
        base, statuses = world.new_tx()
        t = start + timedelta(seconds=r.uniform(0, days * 86400))
        for st in statuses:
            t += timedelta(seconds=r.uniform(1, 6))
            ev = make_event(base, st, maybe_late(r, t))
            rows.append((t, ev))
            if r.random() < 0.02:  # doublon
                rows.append((t + timedelta(seconds=r.uniform(0.1, 3)), dict(ev)))
    rows.sort(key=lambda x: x[0])  # ordre d'arrivee
    rows = rows[:n]
    with open(out, "w", encoding="utf-8") as f:
        for _, ev in rows:
            f.write(json.dumps(ev) + "\n")
    print(f"[file] {len(rows)} evenements ecrits dans {out}")
    write_truth(world, Path(out).parent)


def run_kafka(world, rate, bootstrap, topic):
    try:
        from kafka import KafkaProducer
    except ImportError:
        sys.exit("Module manquant : pip install kafka-python (ou kafka-python-ng)")
    prod = KafkaProducer(bootstrap_servers=bootstrap,
                         key_serializer=lambda k: k.encode(),
                         value_serializer=lambda v: json.dumps(v).encode())
    write_truth(world, HERE)
    r = world.rnd
    active, sent = [], 0
    interval = 1.0 / rate
    next_t = time.time()
    last_log = time.time()
    print(f"[kafka] publication sur {bootstrap}, topic '{topic}', {rate} evt/s (Ctrl+C pour arreter)")
    try:
        while True:
            if len(active) < 30 or r.random() < 0.1:
                base, statuses = world.new_tx()
                active.append([base, deque(statuses)])
            i = r.randrange(len(active))
            base, q = active[i]
            status = q.popleft()
            if not q:
                active.pop(i)
            now = datetime.now(timezone.utc)
            ev = make_event(base, status, maybe_late(r, now))
            prod.send(topic, key=ev["tx_id"], value=ev)
            sent += 1
            if r.random() < 0.02:  # doublon
                prod.send(topic, key=ev["tx_id"], value=ev)
                sent += 1
            next_t += interval
            delay = next_t - time.time()
            if delay > 0:
                time.sleep(delay)
            if time.time() - last_log >= 5:
                print(f"[kafka] {sent} evenements envoyes")
                last_log = time.time()
    except KeyboardInterrupt:
        pass
    finally:
        prod.flush()
        prod.close()
        print(f"[kafka] arret, {sent} evenements envoyes")


def main():
    p = argparse.ArgumentParser(description="Generateur de transactions SahlPay")
    p.add_argument("--mode", choices=["file", "kafka"], required=True)
    p.add_argument("--n", type=int, default=50000, help="nombre d'evenements (mode file)")
    p.add_argument("--rate", type=float, default=20, help="evenements/seconde (mode kafka)")
    p.add_argument("--days", type=int, default=3, help="periode couverte par l'historique")
    p.add_argument("--out", default=str(HERE / "transactions_history.jsonl"))
    p.add_argument("--merchants", default=str(HERE / "merchants.json"))
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="transactions")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--gen-merchants", action="store_true",
                   help="regenerer merchants.json")
    a = p.parse_args()

    if a.gen_merchants or not Path(a.merchants).exists():
        generate_merchants(a.merchants)
    world = World(a.merchants, a.seed)
    if a.mode == "file":
        run_file(world, a.n, a.out, a.days)
    else:
        run_kafka(world, a.rate, a.bootstrap, a.topic)


if __name__ == "__main__":
    main()