"""Genera el dataset histórico de Andina Market en memoria (DataFrames).

Flujo: productos -> clientes (con comportamiento oculto) -> pedidos (proceso
de Poisson con estacionalidad) -> líneas -> pagos (con rechazos y reintentos)
-> tickets -> segmentos -> casos borde inyectados.

Las columnas con prefijo "_" son metadatos internos del generador y no se
cargan a la base de datos.
"""
from __future__ import annotations

import unicodedata
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

from . import config as C
from .catalog import BULK_CATEGORIES, build_products, price_at
from .tickets import ORDER_TICKET_MIX, STANDALONE_TICKET_MIX, TICKET_CHANNELS, make_ticket

EMAIL_DOMAINS = ["gmail.com", "hotmail.com", "outlook.com", "yahoo.com", "icloud.com"]
MAX_SEASON = max(C.MONTH_WEIGHT.values()) * max(C.WEEKDAY_WEIGHT.values())


def _pick(rng, weights: dict):
    keys = list(weights)
    p = np.array(list(weights.values()), dtype=float)
    return keys[rng.choice(len(keys), p=p / p.sum())]


def _ascii(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower().replace(" ", "")


def _random_time(rng, day: datetime) -> datetime:
    hw = np.array(C.HOUR_WEIGHT)
    hour = int(rng.choice(24, p=hw / hw.sum()))
    return day.replace(hour=hour, minute=int(rng.integers(60)), second=int(rng.integers(60)),
                       microsecond=0)


def _channel_mix(t: datetime) -> dict:
    f = (t - C.START_DATE).days / (C.END_DATE - C.START_DATE).days
    return {k: (1 - f) * C.CHANNEL_MIX_START[k] + f * C.CHANNEL_MIX_END[k] for k in C.CHANNEL_MIX_START}


# --------------------------------------------------------------------------
# Productos
# --------------------------------------------------------------------------
def _products(rng):
    prods = build_products(C.N_PRODUCTS, C.START_DATE, C.END_DATE, rng)
    for i, p in enumerate(prods, start=1):
        p["ProductId"] = i
        p["Price"] = price_at(p["_base_price"], C.END_DATE, C.START_DATE, promo=False)
        disc = p["_discontinued_at"]
        p["Status"] = "Discontinued" if disc else "Active"
        p["CreatedAt"] = p["_launch"]
        # Última modificación: el ajuste de precio más reciente o la baja.
        last_price_update = C.END_DATE - timedelta(days=int(rng.integers(5, 180)))
        p["UpdatedAt"] = disc or max(last_price_update, p["_launch"])
    return prods


# --------------------------------------------------------------------------
# Clientes
# --------------------------------------------------------------------------
def _customers(rng, fakers):
    countries = list(C.COUNTRIES)
    cweights = np.array([C.COUNTRIES[c][0] for c in countries])
    categories = ["Electrónica", "Hogar", "Moda", "Deportes", "Belleza", "Mascotas"]
    span = (C.END_DATE - C.START_DATE).days
    pre_start = datetime(2022, 1, 1)
    custs = []
    for i in range(C.N_CUSTOMERS):
        country = countries[rng.choice(len(countries), p=cweights / cweights.sum())]
        _, cities, phone_prefix, locale = C.COUNTRIES[country]
        city = _pick(rng, cities)
        fk = fakers[locale]
        first, last = fk.first_name(), f"{fk.last_name()} {fk.last_name()}"
        # 30% ya era cliente antes de la ventana; el resto llega con crecimiento.
        if rng.random() < 0.30:
            signup = pre_start + timedelta(days=int(rng.integers(0, (C.START_DATE - pre_start).days)))
        else:
            signup = C.START_DATE + timedelta(days=int(np.sqrt(rng.random()) * span))
        signup = _random_time(rng, signup)
        pref = rng.dirichlet([2.0, 1.5, 1.2])  # web, app, tienda
        store_heavy = pref[2] > 0.5
        email = None
        if not (store_heavy and rng.random() < 0.6):
            email = f"{_ascii(first)}.{_ascii(last.split()[0])}{int(rng.integers(1, 999))}@{rng.choice(EMAIL_DOMAINS)}"
        phone = None if rng.random() < 0.10 else f"{phone_prefix}{int(rng.integers(10_000_000, 99_999_999))}"
        churn_days = None if rng.random() < 0.45 else float(rng.exponential(420))
        custs.append({
            "FirstName": first, "LastName": last, "Email": email, "Phone": phone,
            "City": None if rng.random() < 0.03 else city, "Country": country,
            "SignupDate": signup.date(), "CreatedAt": signup,
            "_rate": float(rng.gamma(0.7, 0.65)),          # pedidos por 30 días
            "_never_buys": rng.random() < 0.12,
            "_churn_days": churn_days,
            "_fav_category": rng.choice(categories),
            "_channel_pref": dict(zip(["web", "app", "tienda"], pref)),
            "_is_duplicate_of": None,
        })

    # Caso borde: la misma persona con una segunda cuenta (típicamente creada
    # en tienda). Mismo nombre y teléfono; el email viene con otra forma.
    n_dups = int(C.N_CUSTOMERS * 0.015)
    for idx in rng.choice(len(custs), size=n_dups, replace=False):
        src = custs[idx]
        dup = dict(src)
        signup = src["CreatedAt"] + timedelta(days=int(rng.integers(30, 400)))
        if signup >= C.END_DATE:
            continue
        email = src["Email"]
        if email:
            email = rng.choice([email.upper(), f" {email} ", email.replace("@", "+tienda@"), email.capitalize()])
        dup.update({"Email": email, "SignupDate": signup.date(), "CreatedAt": signup,
                    "_channel_pref": {"web": 0.1, "app": 0.1, "tienda": 0.8},
                    "_rate": src["_rate"] * 0.5, "_never_buys": False, "_is_duplicate_of": idx})
        custs.append(dup)

    custs.sort(key=lambda c: c["CreatedAt"])
    for i, c in enumerate(custs, start=1):
        c["CustomerId"] = i
    return custs


# --------------------------------------------------------------------------
# Pedidos y líneas
# --------------------------------------------------------------------------
def _order_times(rng, cust):
    if cust["_never_buys"]:
        return []
    start = max(cust["CreatedAt"], C.START_DATE)
    end = C.END_DATE
    if cust["_churn_days"] is not None:
        end = min(end, start + timedelta(days=cust["_churn_days"]))
    days = (end - start).total_seconds() / 86400
    if days <= 0:
        return []
    n_cand = rng.poisson(cust["_rate"] * days / 30 * MAX_SEASON)
    times = []
    for off in rng.uniform(0, days, size=n_cand):
        t = start + timedelta(days=float(off))
        w = C.MONTH_WEIGHT[t.month] * C.WEEKDAY_WEIGHT[t.weekday()]
        if rng.random() < w / MAX_SEASON:
            times.append(_random_time(rng, t))
    # Los clientes nuevos suelen comprar en su primera semana.
    if cust["CreatedAt"] >= C.START_DATE and rng.random() < 0.6:
        t = cust["CreatedAt"] + timedelta(hours=float(rng.uniform(0.2, 7 * 24)))
        if t < C.END_DATE:
            times.append(t.replace(microsecond=0))
    return sorted(times)


def _orders_and_items(rng, custs, prods):
    launch = np.array([p["_launch"] for p in prods], dtype="datetime64[s]")
    disc = np.array([p["_discontinued_at"] or datetime(2100, 1, 1) for p in prods], dtype="datetime64[s]")
    pop = np.array([p["_popularity"] for p in prods])
    cats = np.array([p["Category"] for p in prods])

    orders, items = [], []
    for cust in custs:
        for t in _order_times(rng, cust):
            mix = _channel_mix(t)
            w = {k: 0.5 * cust["_channel_pref"][k] + 0.5 * mix[k] for k in mix}
            channel = _pick(rng, w)
            t64 = np.datetime64(t, "s")
            avail = (launch <= t64) & (disc > t64)
            weights = pop * np.where(cats == cust["_fav_category"], 3.0, 1.0) * avail
            n_items = 1 + min(int(rng.poisson(0.8)), 5)
            chosen = rng.choice(len(prods), size=min(n_items, int(avail.sum())), replace=False,
                                p=weights / weights.sum())
            promo_month = t.month == 11 or (t.month == 7 and t.day >= 15)
            lines = []
            for pi in chosen:
                p = prods[pi]
                if p["Category"] in BULK_CATEGORIES:
                    qty = int(rng.choice([1, 2, 3, 4], p=[0.55, 0.25, 0.12, 0.08]))
                else:
                    qty = int(rng.choice([1, 2, 3], p=[0.82, 0.14, 0.04]))
                promo = promo_month and rng.random() < 0.4
                lines.append({"ProductId": p["ProductId"], "Quantity": qty,
                              "UnitPrice": price_at(p["_base_price"], t, C.START_DATE, promo),
                              "_category": p["Category"], "_name": p["Name"]})
            total = round(sum(l["Quantity"] * l["UnitPrice"] for l in lines), 2)
            orders.append({"CustomerId": cust["CustomerId"], "OrderDate": t, "CreatedAt": t,
                           "Channel": channel, "TotalAmount": total, "_lines": lines,
                           "_city": cust["City"]})

    orders.sort(key=lambda o: o["CreatedAt"])
    for oid, o in enumerate(orders, start=1):
        o["OrderId"] = oid
        for l in o["_lines"]:
            items.append({"OrderId": oid, **l, "CreatedAt": o["CreatedAt"], "UpdatedAt": o["CreatedAt"]})
    return orders, items


# --------------------------------------------------------------------------
# Ciclo de vida: estado del pedido y pagos
# --------------------------------------------------------------------------
def _lifecycle(rng, orders):
    """Asigna estado final del pedido y genera los intentos de pago.

    Pedidos antiguos llegan a un estado terminal; los de los últimos días
    quedan "en vuelo" (Pendiente/Pagado/Enviado), que es justo lo que el
    simulador de cambios hará avanzar después.
    """
    payments = []

    def add_payment(o, method, status, when, updated=None):
        payments.append({"OrderId": o["OrderId"], "Method": method, "Amount": o["TotalAmount"],
                         "Status": status, "PaymentDate": when, "CreatedAt": when,
                         "UpdatedAt": updated or when})

    for o in orders:
        t0 = o["CreatedAt"]
        age_days = (C.END_DATE - t0).total_seconds() / 86400
        method = _pick(rng, C.PAYMENT_METHODS[o["Channel"]])
        o["_rejected_attempts"] = 0
        o["_refunded"] = False

        if rng.random() < 0.05:  # cancelado
            o["Status"] = "Cancelado"
            o["UpdatedAt"] = t0 + timedelta(hours=float(rng.uniform(0.5, 48)))
            if rng.random() < 0.45:
                for k in range(int(rng.integers(1, 3))):
                    add_payment(o, method, "Rechazado", t0 + timedelta(minutes=2 + 7 * k))
                    o["_rejected_attempts"] += 1
            continue

        # Intentos rechazados previos al pago bueno
        t_pay = t0 + timedelta(minutes=float(rng.uniform(1, 10)))
        while rng.random() < C.REJECTION_RATE[method] and o["_rejected_attempts"] < 2:
            add_payment(o, method, "Rechazado", t_pay)
            o["_rejected_attempts"] += 1
            t_pay += timedelta(minutes=float(rng.uniform(2, 40)))
            if rng.random() < 0.4:  # cambia de medio de pago al reintentar
                method = _pick(rng, C.PAYMENT_METHODS[o["Channel"]])

        # Transferencia/efectivo en web se confirma con demora.
        confirm_delay = timedelta(hours=float(rng.uniform(2, 30))) if method == "transferencia" else timedelta(0)
        approved_at = t_pay + confirm_delay

        if o["Channel"] == "tienda":
            delivered_at = approved_at  # se lleva el producto en el momento
            shipped_at = approved_at
        else:
            shipped_at = approved_at + timedelta(days=float(rng.uniform(1, 2.5)))
            delivered_at = shipped_at + timedelta(days=float(rng.uniform(1, 6)))

        if approved_at > C.END_DATE:
            o["Status"], o["UpdatedAt"] = "Pendiente", t0
            add_payment(o, method, "Pendiente", t_pay)
            continue
        if confirm_delay:
            add_payment(o, method, "Aprobado", t_pay, updated=approved_at)  # nace Pendiente, luego se aprueba
        else:
            add_payment(o, method, "Aprobado", t_pay)

        if shipped_at > C.END_DATE:
            o["Status"], o["UpdatedAt"] = "Pagado", approved_at
        elif delivered_at > C.END_DATE:
            o["Status"], o["UpdatedAt"] = "Enviado", shipped_at
        else:
            o["Status"], o["UpdatedAt"] = "Entregado", delivered_at
            returned_at = delivered_at + timedelta(days=float(rng.uniform(3, 25)))
            if rng.random() < 0.035 and returned_at < C.END_DATE:
                o["Status"], o["UpdatedAt"] = "Devuelto", returned_at
                o["_refunded"] = True
                payments[-1]["Status"] = "Reembolsado"
                payments[-1]["UpdatedAt"] = returned_at + timedelta(days=float(rng.uniform(1, 10)))
        o["_approved_payment_idx"] = len(payments) - 1

    return payments


# --------------------------------------------------------------------------
# Tickets de soporte
# --------------------------------------------------------------------------
def _ticket_row(rng, cust_id, order, ttype, created, city, product=None, amount=None):
    subject, body, priority = make_ticket(rng, ttype, order_id=order["OrderId"] if order else None,
                                          product=product, city=city, amount=amount)
    age = (C.END_DATE - created).total_seconds() / 86400
    speed = {"Urgente": 0.3, "Alta": 0.6, "Media": 1.0, "Baja": 1.5}[priority]
    resolve_days = float(rng.exponential(3 * speed)) + 0.1
    if age < resolve_days:
        status = "Abierto" if age < resolve_days * 0.3 else "EnProceso"
        updated = created if status == "Abierto" else created + timedelta(days=age * 0.5)
    else:
        status = "Cerrado" if rng.random() < 0.6 else "Resuelto"
        updated = created + timedelta(days=resolve_days)
    return {"CustomerId": cust_id, "OrderId": order["OrderId"] if order else None,
            "Channel": _pick(rng, TICKET_CHANNELS), "Subject": subject, "Body": body,
            "Priority": priority, "Status": status, "CreatedAt": created, "UpdatedAt": updated,
            "_type": ttype}


def _tickets(rng, custs, orders, prods):
    tickets = []
    cust_city = {c["CustomerId"]: c["City"] for c in custs}
    for o in orders:
        ttype = None
        if o.get("_duplicate_charge") and rng.random() < 0.8:
            ttype = "cobro_duplicado"
        elif o["_refunded"] and rng.random() < 0.30:
            ttype = "reembolso"
        elif o["_rejected_attempts"] and rng.random() < 0.12:
            ttype = "pago_rechazado"
        elif rng.random() < 0.055:
            ttype = _pick(rng, ORDER_TICKET_MIX)
            if ttype == "cambio_talla" and not any(l["_category"] == "Moda" for l in o["_lines"]):
                ttype = "entrega_retrasada"
            if ttype == "reembolso" and not o["_refunded"]:
                ttype = "producto_defectuoso"
        if not ttype:
            continue
        created = o["CreatedAt"] + timedelta(days=float(rng.uniform(0.1, 12)))
        if created >= C.END_DATE:
            continue
        product = o["_lines"][0]["_name"] if o["_lines"] else None
        tickets.append(_ticket_row(rng, o["CustomerId"], o, ttype, created,
                                   o["_city"], product, o["TotalAmount"]))

    # Tickets sin pedido (consultas, cuenta, fraude)
    for c in custs:
        if rng.random() < 0.10:
            created = max(c["CreatedAt"], C.START_DATE) + timedelta(
                days=float(rng.uniform(0, max((C.END_DATE - max(c["CreatedAt"], C.START_DATE)).days, 1))))
            if created >= C.END_DATE:
                continue
            ttype = _pick(rng, STANDALONE_TICKET_MIX)
            product = prods[int(rng.integers(len(prods)))]["Name"]
            tickets.append(_ticket_row(rng, c["CustomerId"], None, ttype, created, c["City"],
                                       product=product))

    # Caso borde: spam que entra por el formulario web. El simulador de
    # cambios los elimina físicamente (DELETE que la ingesta debe detectar).
    for _ in range(6):
        c = custs[int(rng.integers(len(custs)))]
        created = C.END_DATE - timedelta(days=float(rng.uniform(0.5, 5)))
        tickets.append({"CustomerId": c["CustomerId"], "OrderId": None, "Channel": "email",
                        "Subject": "¡Felicidades! Ganaste un cupón de 500 USD",
                        "Body": "Reclama tu premio aquí: http://premios-andina.example/claim",
                        "Priority": "Baja", "Status": "Abierto", "CreatedAt": created,
                        "UpdatedAt": created, "_type": "spam"})

    tickets.sort(key=lambda t: t["CreatedAt"])
    for i, t in enumerate(tickets, start=1):
        t["TicketId"] = i
    return tickets


# --------------------------------------------------------------------------
# Segmentos (regla del CRM de la fuente, calculada al cierre del histórico)
# --------------------------------------------------------------------------
def _segments(custs, orders):
    R = C.SEGMENT_RULES
    cutoff = C.END_DATE - timedelta(days=365)
    n, spend, last = {}, {}, {}
    for o in orders:
        if o["Status"] == "Cancelado":
            continue
        cid = o["CustomerId"]
        last[cid] = max(last.get(cid, o["CreatedAt"]), o["CreatedAt"])
        if o["CreatedAt"] >= cutoff:
            n[cid] = n.get(cid, 0) + 1
            spend[cid] = spend.get(cid, 0) + o["TotalAmount"]
    for c in custs:
        cid = c["CustomerId"]
        k, s = n.get(cid, 0), spend.get(cid, 0.0)
        if (C.END_DATE - c["CreatedAt"]).days < R["nuevo_days"] and k < 2:
            seg = "Nuevo"
        elif s >= R["vip_spend_365d"] or k >= R["vip_orders_365d"]:
            seg = "VIP"
        elif k >= R["frecuente_orders_365d"]:
            seg = "Frecuente"
        else:
            seg = "Regular"
        c["Segment"] = seg
        c["UpdatedAt"] = max(c["CreatedAt"], last.get(cid, c["CreatedAt"]))


# --------------------------------------------------------------------------
# Casos borde de calidad de datos (documentados en EDGE_CASES.md)
# --------------------------------------------------------------------------
def _inject_edge_cases(rng, custs, orders, items, payments, prods):
    log = {"clientes_duplicados": sum(1 for c in custs if c["_is_duplicate_of"] is not None)}

    # Emails nulos (tienda) y teléfonos/ciudades nulos ya vienen del generador.
    log["email_nulo"] = sum(1 for c in custs if c["Email"] is None)
    log["telefono_nulo"] = sum(1 for c in custs if c["Phone"] is None)
    log["ciudad_nula"] = sum(1 for c in custs if c["City"] is None)

    # País con formato sucio
    dirty_country = {"PE": ["Peru", "pe", "PERÚ", " PE"], "CO": ["Colombia", "co", " CO"],
                     "CL": ["Chile", "cl"], "MX": ["México", "mx", "MEX"], "EC": ["Ecuador", "ec"]}
    idx = rng.choice(len(custs), size=int(len(custs) * 0.02), replace=False)
    for i in idx:
        custs[i]["Country"] = rng.choice(dirty_country[custs[i]["Country"].strip().upper()[:2]])
    log["pais_formato_sucio"] = len(idx)

    # Emails con formato inválido
    with_email = [c for c in custs if c["Email"]]
    idx = rng.choice(len(with_email), size=int(len(custs) * 0.005), replace=False)
    for i in idx:
        e = with_email[i]["Email"].strip()
        with_email[i]["Email"] = rng.choice([e.split("@")[0], e.replace("@", "@@"), e.rsplit(".", 1)[0], "sin-correo"])
    log["email_invalido"] = len(idx)

    # Pedidos cuyo total no cuadra con sus líneas (cupón no itemizado en legacy)
    delivered = [o for o in orders if o["Status"] == "Entregado"]
    idx = rng.choice(len(delivered), size=int(len(orders) * 0.005), replace=False)
    for i in idx:
        o = delivered[i]
        o["TotalAmount"] = round(o["TotalAmount"] * float(rng.uniform(0.88, 0.97)), 2)
        payments[o["_approved_payment_idx"]]["Amount"] = o["TotalAmount"]
    log["total_no_cuadra_con_lineas"] = len(idx)

    # Pedidos sin líneas (cabecera creada, detalle perdido por bug de la app)
    idx = set(rng.choice(len(orders), size=int(len(orders) * 0.002), replace=False).tolist())
    lost_ids = {orders[i]["OrderId"] for i in idx}
    items[:] = [it for it in items if it["OrderId"] not in lost_ids]
    log["pedidos_sin_lineas"] = len(lost_ids)

    # Cantidad cero
    idx = rng.choice(len(items), size=int(len(items) * 0.001), replace=False)
    for i in idx:
        items[i]["Quantity"] = 0
    log["lineas_cantidad_cero"] = len(idx)

    # Huérfanos: ProductId inexistente (migración legacy de fines de 2024)
    old_items = [it for it in items if it["CreatedAt"] < datetime(2024, 12, 1)]
    idx = rng.choice(len(old_items), size=25, replace=False)
    for i in idx:
        old_items[i]["ProductId"] = int(rng.integers(9001, 9011))
    log["lineas_producto_inexistente"] = 25

    # Fecha de pedido con año mal digitado (2027) en pedidos de tienda
    store = [o for o in orders if o["Channel"] == "tienda"]
    for i in rng.choice(len(store), size=3, replace=False):
        store[i]["OrderDate"] = store[i]["OrderDate"].replace(year=2027)
    log["fecha_pedido_futura"] = 3

    # Doble cobro: el mismo pago aprobado se registra dos veces (doble clic)
    single = [o for o in orders if o.get("_approved_payment_idx") is not None
              and payments[o["_approved_payment_idx"]]["Status"] == "Aprobado"
              and o["_rejected_attempts"] == 0]
    idx = rng.choice(len(single), size=int(len(orders) * 0.0025), replace=False)
    for i in idx:
        o = single[i]
        dup = dict(payments[o["_approved_payment_idx"]])
        dup["PaymentDate"] = dup["CreatedAt"] = dup["UpdatedAt"] = dup["PaymentDate"] + timedelta(
            seconds=int(rng.integers(2, 20)))
        payments.append(dup)
        o["_duplicate_charge"] = True
    log["pago_duplicado"] = len(idx)

    log["productos_descontinuados"] = sum(1 for p in prods if p["Status"] == "Discontinued")
    return log


# --------------------------------------------------------------------------
# API pública
# --------------------------------------------------------------------------
TABLE_COLUMNS = {
    "Customers": ["CustomerId", "FirstName", "LastName", "Email", "Phone", "City", "Country",
                  "Segment", "SignupDate", "CreatedAt", "UpdatedAt"],
    "Products": ["ProductId", "SKU", "Name", "Category", "Subcategory", "Brand", "Price",
                 "Description", "Status", "CreatedAt", "UpdatedAt"],
    "Orders": ["OrderId", "CustomerId", "OrderDate", "Channel", "Status", "TotalAmount",
               "CreatedAt", "UpdatedAt"],
    "OrderItems": ["OrderItemId", "OrderId", "ProductId", "Quantity", "UnitPrice", "CreatedAt",
                   "UpdatedAt"],
    "Payments": ["PaymentId", "OrderId", "Method", "Amount", "Status", "PaymentDate", "CreatedAt",
                 "UpdatedAt"],
    "SupportTickets": ["TicketId", "CustomerId", "OrderId", "Channel", "Subject", "Body",
                       "Priority", "Status", "CreatedAt", "UpdatedAt"],
}
LOAD_ORDER = ["Customers", "Products", "Orders", "OrderItems", "Payments", "SupportTickets"]


def generate(seed: int = C.SEED):
    rng = np.random.default_rng(seed)
    fakers = {}
    for loc in {v[3] for v in C.COUNTRIES.values()}:
        fakers[loc] = Faker(loc)
        fakers[loc].seed_instance(seed)

    prods = _products(rng)
    custs = _customers(rng, fakers)
    orders, items = _orders_and_items(rng, custs, prods)
    payments = _lifecycle(rng, orders)
    _segments(custs, orders)
    edge_log = _inject_edge_cases(rng, custs, orders, items, payments, prods)
    tickets = _tickets(rng, custs, orders, prods)  # después: usa la marca de doble cobro
    edge_log["tickets_cuerpo_vacio"] = sum(1 for t in tickets if t["Body"] == "")

    items.sort(key=lambda it: (it["CreatedAt"], it["OrderId"]))
    for i, it in enumerate(items, start=1):
        it["OrderItemId"] = i
    payments.sort(key=lambda p: p["CreatedAt"])
    for i, p in enumerate(payments, start=1):
        p["PaymentId"] = i

    raw = {"Customers": custs, "Products": prods, "Orders": orders, "OrderItems": items,
           "Payments": payments, "SupportTickets": tickets}
    frames = {name: pd.DataFrame(rows)[TABLE_COLUMNS[name]] for name, rows in raw.items()}
    frames["SupportTickets"]["OrderId"] = frames["SupportTickets"]["OrderId"].astype("Int64")
    return frames, edge_log
