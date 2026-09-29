"""Genera una muestra de eventos de clickstream de la app móvil (.jsonl).

Representa lo que llegaría por Event Hubs/Kafka en producción. Incluye a
propósito los problemas típicos de un stream real, que el diseño de
streaming del nivel 1 debe resolver:
  - Duplicados: el SDK reintenta y reenvía el mismo event_id (at-least-once).
  - Llegada tardía: eventos con event_ts horas antes que su sent_ts (el
    teléfono estuvo sin señal) -> requiere watermark.
  - Usuarios anónimos: customer_id nulo hasta que inician sesión.
  - Deriva de esquema: la versión 5.x del app agrega el campo "campaign".

Uso:
    python -m data_generator.clickstream --events 5000
"""
from __future__ import annotations

import argparse
import json
import uuid
from datetime import timedelta
from pathlib import Path

import numpy as np

from . import config as C
from .generate import generate

ROOT = Path(__file__).resolve().parents[1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sessions", type=int, default=900)
    ap.add_argument("--out", default=str(ROOT / "sample_data" / "clickstream_app_events.jsonl"))
    args = ap.parse_args()

    frames, _ = generate()
    customers = frames["Customers"]
    products = frames["Products"][frames["Products"]["Status"] == "Active"]
    rng = np.random.default_rng(C.SEED + 1)
    window_start = C.END_DATE - timedelta(days=2)
    events = []

    def emit(etype, ts, session, **extra):
        ev = {"event_id": str(uuid.UUID(int=int(rng.integers(2**63)) << 64 | int(rng.integers(2**63)))),
              "event_type": etype, "event_ts": ts.isoformat(timespec="milliseconds") + "Z",
              "session_id": session["id"], "customer_id": session["customer_id"],
              "device": session["device"], "country": session["country"], **extra}
        if session["device"]["app_version"].startswith("5."):
            ev["campaign"] = session["campaign"]
        # Hora de envío: normalmente segundos después; 3% llega horas tarde.
        delay = timedelta(hours=float(rng.uniform(1, 6))) if rng.random() < 0.03 \
            else timedelta(seconds=float(rng.uniform(0.2, 4)))
        ev["sent_ts"] = (ts + delay).isoformat(timespec="milliseconds") + "Z"
        events.append(ev)
        if rng.random() < 0.02:  # reenvío del SDK: mismo event_id
            events.append(dict(ev))

    for _ in range(args.sessions):
        cust = customers.iloc[int(rng.integers(len(customers)))]
        logged_in = rng.random() < 0.75
        os_ = rng.choice(["android", "ios"], p=[0.7, 0.3])
        session = {
            "id": f"s-{uuid.UUID(int=int(rng.integers(2**63))).hex[-16:]}",
            "customer_id": int(cust.CustomerId) if logged_in else None,
            "country": (cust.Country or "PE").strip().upper()[:2],
            "device": {"os": str(os_), "app_version": str(rng.choice(["4.8.2", "4.9.0", "5.0.1"], p=[0.2, 0.5, 0.3]))},
            "campaign": str(rng.choice(["cyber-sep", "push-reactivacion", "organico"])),
        }
        ts = window_start + timedelta(seconds=float(rng.uniform(0, 2 * 86400)))
        emit("session_start", ts, session)
        cart = []
        for _ in range(int(rng.integers(1, 9))):
            ts += timedelta(seconds=float(rng.uniform(5, 90)))
            p = products.iloc[int(rng.integers(len(products)))]
            emit("product_view", ts, session, product_id=int(p.ProductId), sku=p.SKU,
                 category=p.Category, price=float(p.Price))
            if rng.random() < 0.25:
                ts += timedelta(seconds=float(rng.uniform(3, 30)))
                qty = int(rng.choice([1, 1, 2]))
                cart.append((p, qty))
                emit("add_to_cart", ts, session, product_id=int(p.ProductId), sku=p.SKU,
                     quantity=qty, price=float(p.Price))
        if cart and rng.random() < 0.15:
            p, qty = cart.pop()
            ts += timedelta(seconds=float(rng.uniform(3, 30)))
            emit("remove_from_cart", ts, session, product_id=int(p.ProductId), sku=p.SKU, quantity=qty)
        if cart and rng.random() < 0.45:
            ts += timedelta(seconds=float(rng.uniform(20, 120)))
            emit("checkout_start", ts, session, items=len(cart))
            if rng.random() < 0.7 and session["customer_id"]:
                ts += timedelta(seconds=float(rng.uniform(20, 180)))
                total = round(sum(float(p.Price) * q for p, q in cart), 2)
                emit("purchase", ts, session, order_ref=f"app-{uuid.UUID(int=int(rng.integers(2**63))).hex[-10:]}",
                     items=[{"product_id": int(p.ProductId), "quantity": q, "price": float(p.Price)} for p, q in cart],
                     total=total, currency="USD")

    events.sort(key=lambda e: e["sent_ts"])  # orden de llegada al broker
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
    types = {}
    for ev in events:
        types[ev["event_type"]] = types.get(ev["event_type"], 0) + 1
    print(f"{len(events):,} eventos -> {out}\n{types}")


if __name__ == "__main__":
    main()
