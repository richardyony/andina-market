"""Simula un "día de operación" sobre la fuente para probar la ingesta incremental.

Cada ejecución hace, en una sola transacción:
  - INSERT: clientes nuevos, pedidos con líneas y pagos, tickets, productos nuevos.
  - UPDATE: pagos Pendiente -> Aprobado/Rechazado (con reintento), avance de
    pedidos (Pagado -> Enviado -> Entregado), devoluciones con reembolso,
    recálculo de segmento del CRM (SCD2 aguas abajo), mudanzas de clientes,
    cambios de precio, producto descontinuado, avance de tickets.
  - DELETE: tickets marcados como spam y una línea quitada de un pedido pendiente.
  - Opcional --schema-change: agrega Orders.CouponCode (evolución de esquema).

Uso:
    python -m data_generator.simulate_changes                 # un día de cambios
    python -m data_generator.simulate_changes --orders 300    # más volumen
    python -m data_generator.simulate_changes --schema-change # + columna nueva
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import numpy as np
from faker import Faker

from . import config as C
from .db import connect
from .generate import _ascii, _pick
from .tickets import ORDER_TICKET_MIX, make_ticket


def now_utc():
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def D(x) -> Decimal:
    return Decimal(str(round(float(x), 2)))


class Simulator:
    def __init__(self, conn, rng, n_orders: int, schema_change: bool):
        self.conn, self.cur, self.rng = conn, conn.cursor(), rng
        self.n_orders, self.schema_change = n_orders, schema_change
        self.now = now_utc()
        self.stats: dict[str, int] = {}

    def _count(self, key, n=1):
        self.stats[key] = self.stats.get(key, 0) + n

    def _insert(self, sql, params):
        """INSERT ... OUTPUT INSERTED.<id> y devuelve el ID generado."""
        self.cur.execute(sql, params)
        return int(self.cur.fetchone()[0])

    # ------------------------------------------------------------------
    def schema_evolution(self):
        self.cur.execute("SELECT COL_LENGTH('dbo.Orders', 'CouponCode')")
        exists = self.cur.fetchone()[0] is not None
        if self.schema_change and not exists:
            self.cur.execute("ALTER TABLE dbo.Orders ADD CouponCode NVARCHAR(20) NULL")
            self._count("schema_change: Orders.CouponCode agregada")
            exists = True
        self.has_coupon = exists

    def new_customers(self, n=25):
        fk = Faker("es_CO")
        fk.seed_instance(int(self.rng.integers(1e9)))
        countries = list(C.COUNTRIES)
        w = np.array([C.COUNTRIES[c][0] for c in countries])
        ids = []
        for _ in range(n):
            country = countries[self.rng.choice(len(countries), p=w / w.sum())]
            _, cities, prefix, _ = C.COUNTRIES[country]
            first, last = fk.first_name(), f"{fk.last_name()} {fk.last_name()}"
            email = f"{_ascii(first)}.{_ascii(last.split()[0])}{int(self.rng.integers(1, 999))}@gmail.com"
            ids.append(self._insert(
                """INSERT INTO dbo.Customers (FirstName, LastName, Email, Phone, City, Country,
                       Segment, SignupDate, CreatedAt, UpdatedAt)
                   OUTPUT INSERTED.CustomerId
                   VALUES (?, ?, ?, ?, ?, ?, 'Nuevo', ?, ?, ?)""",
                (first, last, email, f"{prefix}{int(self.rng.integers(10_000_000, 99_999_999))}",
                 _pick(self.rng, cities), country, self.now.date(), self.now, self.now)))
        self._count("clientes nuevos (INSERT)", n)
        return ids

    def new_orders(self, new_customer_ids):
        self.cur.execute("SELECT ProductId, Price, Category FROM dbo.Products WHERE Status = 'Active'")
        products = self.cur.fetchall()
        self.cur.execute("SELECT TOP 3000 CustomerId FROM dbo.Customers ORDER BY UpdatedAt DESC")
        customers = [r[0] for r in self.cur.fetchall()] + new_customer_ids * 3

        for _ in range(self.n_orders):
            cid = int(customers[self.rng.integers(len(customers))])
            channel = _pick(self.rng, C.CHANNEL_MIX_END)
            t = self.now - timedelta(minutes=int(self.rng.integers(5, 600)))
            lines = []
            for pi in self.rng.choice(len(products), size=1 + min(int(self.rng.poisson(0.8)), 3), replace=False):
                pid, price, _ = products[pi]
                lines.append((int(pid), int(self.rng.choice([1, 1, 1, 2])), D(price)))
            total = D(sum(q * float(p) for _, q, p in lines))
            method = _pick(self.rng, C.PAYMENT_METHODS[channel])

            cols, vals = "CustomerId, OrderDate, Channel, Status, TotalAmount, CreatedAt, UpdatedAt", [
                cid, t, channel, "Pendiente", total, t, t]
            if self.has_coupon:
                cols += ", CouponCode"
                vals.append("CYBER10" if self.rng.random() < 0.2 else None)
            oid = self._insert(f"INSERT INTO dbo.Orders ({cols}) OUTPUT INSERTED.OrderId "
                               f"VALUES ({', '.join('?' * len(vals))})", vals)
            self.cur.executemany(
                "INSERT INTO dbo.OrderItems (OrderId, ProductId, Quantity, UnitPrice, CreatedAt, UpdatedAt) "
                "VALUES (?, ?, ?, ?, ?, ?)", [(oid, pid, q, p, t, t) for pid, q, p in lines])

            pay_sql = ("INSERT INTO dbo.Payments (OrderId, Method, Amount, Status, PaymentDate, CreatedAt, UpdatedAt) "
                       "VALUES (?, ?, ?, ?, ?, ?, ?)")
            if self.rng.random() < C.REJECTION_RATE[method]:
                self.cur.execute(pay_sql, (oid, method, total, "Rechazado", t, t, t))
                self._count("pagos rechazados (INSERT)")
            if method in ("transferencia",) or (channel != "tienda" and self.rng.random() < 0.25):
                self.cur.execute(pay_sql, (oid, method, total, "Pendiente", t, t, t))
            else:
                self.cur.execute(pay_sql, (oid, method, total, "Aprobado", t, t, t))
                status = "Entregado" if channel == "tienda" else "Pagado"
                self.cur.execute("UPDATE dbo.Orders SET Status = ?, UpdatedAt = ? WHERE OrderId = ?",
                                 (status, t, oid))
        self._count("pedidos nuevos (INSERT)", self.n_orders)

    def progress_payments(self):
        """Pagos pendientes: la mayoría se aprueba; algunos se rechazan y el
        cliente reintenta (nueva fila) o el pedido se cancela."""
        self.cur.execute("""SELECT PaymentId, OrderId, Method, Amount FROM dbo.Payments
                            WHERE Status = 'Pendiente' AND CreatedAt < DATEADD(MINUTE, -30, ?)""", self.now)
        for pay_id, oid, method, amount in self.cur.fetchall():
            r = self.rng.random()
            if r < 0.85:
                self.cur.execute("UPDATE dbo.Payments SET Status = 'Aprobado', UpdatedAt = ? WHERE PaymentId = ?",
                                 (self.now, pay_id))
                self.cur.execute("""UPDATE dbo.Orders SET Status = 'Pagado', UpdatedAt = ?
                                    WHERE OrderId = ? AND Status = 'Pendiente'""", (self.now, oid))
                self._count("pagos Pendiente -> Aprobado (UPDATE)")
            else:
                self.cur.execute("UPDATE dbo.Payments SET Status = 'Rechazado', UpdatedAt = ? WHERE PaymentId = ?",
                                 (self.now, pay_id))
                self._count("pagos Pendiente -> Rechazado (UPDATE)")
                if r < 0.95:  # reintento con otro medio: queda pendiente para la próxima corrida
                    self.cur.execute("""INSERT INTO dbo.Payments (OrderId, Method, Amount, Status, PaymentDate,
                                        CreatedAt, UpdatedAt) VALUES (?, 'tarjeta', ?, 'Pendiente', ?, ?, ?)""",
                                     (oid, amount, self.now, self.now, self.now))
                    self._count("reintentos de pago (INSERT)")
                else:
                    self.cur.execute("UPDATE dbo.Orders SET Status = 'Cancelado', UpdatedAt = ? WHERE OrderId = ?",
                                     (self.now, oid))
                    self._count("pedidos cancelados (UPDATE)")

    def progress_orders(self):
        for src, dst, share in (("Enviado", "Entregado", 0.7), ("Pagado", "Enviado", 0.8)):
            self.cur.execute(f"""UPDATE TOP ({share * 100:.0f}) PERCENT dbo.Orders
                                 SET Status = ?, UpdatedAt = ?
                                 WHERE Status = ? AND UpdatedAt < DATEADD(MINUTE, -30, ?)""",
                             (dst, self.now, src, self.now))
            self._count(f"pedidos {src} -> {dst} (UPDATE)", self.cur.rowcount)

        # Devoluciones: pedido Devuelto + pago Reembolsado
        self.cur.execute("""SELECT TOP 3 OrderId FROM dbo.Orders
                            WHERE Status = 'Entregado' AND OrderDate > DATEADD(DAY, -30, ?)
                            ORDER BY NEWID()""", self.now)
        for (oid,) in self.cur.fetchall():
            self.cur.execute("UPDATE dbo.Orders SET Status = 'Devuelto', UpdatedAt = ? WHERE OrderId = ?",
                             (self.now, oid))
            self.cur.execute("""UPDATE dbo.Payments SET Status = 'Reembolsado', UpdatedAt = ?
                                WHERE OrderId = ? AND Status = 'Aprobado'""", (self.now, oid))
            self._count("devoluciones con reembolso (UPDATE)")

        # El cliente quita una línea de un pedido pendiente (DELETE en OrderItems)
        self.cur.execute("""SELECT TOP 1 oi.OrderItemId, oi.OrderId, oi.Quantity * oi.UnitPrice
                            FROM dbo.OrderItems oi JOIN dbo.Orders o ON o.OrderId = oi.OrderId
                            WHERE o.Status = 'Pendiente'
                              AND (SELECT COUNT(*) FROM dbo.OrderItems x WHERE x.OrderId = o.OrderId) > 1
                            ORDER BY NEWID()""")
        row = self.cur.fetchone()
        if row:
            item_id, oid, amount = row
            self.cur.execute("DELETE FROM dbo.OrderItems WHERE OrderItemId = ?", item_id)
            self.cur.execute("""UPDATE dbo.Orders SET TotalAmount = TotalAmount - ?, UpdatedAt = ?
                                WHERE OrderId = ?""", (amount, self.now, oid))
            self.cur.execute("""UPDATE dbo.Payments SET Amount = Amount - ?, UpdatedAt = ?
                                WHERE OrderId = ? AND Status = 'Pendiente'""", (amount, self.now, oid))
            self._count("líneas eliminadas de pedido pendiente (DELETE)")

    def recompute_segments(self):
        """Job nocturno del CRM: misma regla que la carga inicial (config.SEGMENT_RULES).
        Solo toca filas cuyo segmento cambia -> esas filas generan una versión SCD2."""
        R = C.SEGMENT_RULES
        self.cur.execute(f"""
            WITH agg AS (
                SELECT c.CustomerId, c.SignupDate, c.Segment,
                       COUNT(o.OrderId) AS n365, COALESCE(SUM(o.TotalAmount), 0) AS spend365
                FROM dbo.Customers c
                LEFT JOIN dbo.Orders o ON o.CustomerId = c.CustomerId AND o.Status <> 'Cancelado'
                     AND o.OrderDate >= DATEADD(DAY, -365, ?) AND o.OrderDate <= ?
                GROUP BY c.CustomerId, c.SignupDate, c.Segment
            ), target AS (
                SELECT CustomerId, Segment,
                    CASE WHEN DATEDIFF(DAY, SignupDate, ?) < {R['nuevo_days']} AND n365 < 2 THEN 'Nuevo'
                         WHEN spend365 >= {R['vip_spend_365d']} OR n365 >= {R['vip_orders_365d']} THEN 'VIP'
                         WHEN n365 >= {R['frecuente_orders_365d']} THEN 'Frecuente'
                         ELSE 'Regular' END AS NewSegment
                FROM agg
            )
            UPDATE c SET Segment = t.NewSegment, UpdatedAt = ?
            FROM dbo.Customers c JOIN target t ON t.CustomerId = c.CustomerId
            WHERE t.NewSegment <> t.Segment""", (self.now, self.now, self.now, self.now))
        self._count("cambios de segmento del CRM (UPDATE)", self.cur.rowcount)

    def customer_updates(self):
        self.cur.execute("SELECT TOP 10 CustomerId, Country FROM dbo.Customers "
                         "WHERE Country IN ('PE','CO','CL','MX','EC') ORDER BY NEWID()")
        for cid, country in self.cur.fetchall():
            city = _pick(self.rng, C.COUNTRIES[country][1])
            self.cur.execute("UPDATE dbo.Customers SET City = ?, UpdatedAt = ? WHERE CustomerId = ?",
                             (city, self.now, cid))
        self._count("mudanzas de clientes (UPDATE)", 10)

    def product_updates(self):
        self.cur.execute("SELECT TOP 8 ProductId, Price FROM dbo.Products WHERE Status = 'Active' ORDER BY NEWID()")
        for pid, price in self.cur.fetchall():
            new = float(price) * float(self.rng.choice([0.9, 0.95, 1.05, 1.1, 1.15]))
            self.cur.execute("UPDATE dbo.Products SET Price = ?, UpdatedAt = ? WHERE ProductId = ?",
                             (D(int(new) + 0.9), self.now, pid))
        self._count("cambios de precio (UPDATE)", 8)
        self.cur.execute("""UPDATE TOP (1) dbo.Products SET Status = 'Discontinued', UpdatedAt = ?
                            WHERE Status = 'Active'""", self.now)
        self._count("productos descontinuados (UPDATE)", self.cur.rowcount)

    def tickets(self):
        self.cur.execute("""SELECT TOP 15 o.OrderId, o.CustomerId, o.TotalAmount, p.Name, c.City
                            FROM dbo.Orders o
                            JOIN dbo.OrderItems oi ON oi.OrderId = o.OrderId
                            JOIN dbo.Products p ON p.ProductId = oi.ProductId
                            JOIN dbo.Customers c ON c.CustomerId = o.CustomerId
                            WHERE o.OrderDate > DATEADD(DAY, -20, ?) ORDER BY NEWID()""", self.now)
        for oid, cid, amount, pname, city in self.cur.fetchall():
            ttype = _pick(self.rng, ORDER_TICKET_MIX)
            subject, body, prio = make_ticket(self.rng, ttype, oid, pname, city, float(amount))
            self.cur.execute("""INSERT INTO dbo.SupportTickets (CustomerId, OrderId, Channel, Subject, Body,
                                Priority, Status, CreatedAt, UpdatedAt)
                                VALUES (?, ?, 'chat', ?, ?, ?, 'Abierto', ?, ?)""",
                             (cid, oid, subject, body, prio, self.now, self.now))
        self._count("tickets nuevos (INSERT)", 15)
        for src, dst in (("EnProceso", "Resuelto"), ("Abierto", "EnProceso")):
            self.cur.execute("""UPDATE TOP (60) PERCENT dbo.SupportTickets SET Status = ?, UpdatedAt = ?
                                WHERE Status = ? AND CreatedAt < DATEADD(HOUR, -1, ?)
                                  AND Subject NOT LIKE N'¡Felicidades!%'""", (dst, self.now, src, self.now))
            self._count(f"tickets {src} -> {dst} (UPDATE)", self.cur.rowcount)
        self.cur.execute("DELETE FROM dbo.SupportTickets WHERE Subject LIKE N'¡Felicidades!%'")
        self._count("tickets spam eliminados (DELETE)", self.cur.rowcount)

    def run(self):
        self.schema_evolution()
        # Primero avanzan los estados existentes; los pedidos nuevos quedan
        # "en vuelo" para la siguiente corrida.
        self.progress_payments()
        self.progress_orders()
        self.tickets()
        self.customer_updates()
        self.product_updates()
        new_ids = self.new_customers()
        self.new_orders(new_ids)
        self.recompute_segments()
        self.conn.commit()
        self.cur.execute("SELECT CHANGE_TRACKING_CURRENT_VERSION()")
        return self.cur.fetchone()[0]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--orders", type=int, default=120)
    ap.add_argument("--schema-change", action="store_true")
    ap.add_argument("--seed", type=int, default=None, help="Semilla (por defecto aleatoria)")
    args = ap.parse_args()

    conn = connect()
    sim = Simulator(conn, np.random.default_rng(args.seed), args.orders, args.schema_change)
    try:
        version = sim.run()
    except Exception:
        conn.rollback()  # todo o nada: la fuente nunca queda a medio actualizar
        raise
    print(f"Cambios aplicados a las {sim.now:%Y-%m-%d %H:%M:%S} UTC:")
    for k, v in sim.stats.items():
        print(f"  {k:<48} {v:>5}")
    print(f"Versión de Change Tracking: {version}")
    conn.close()


if __name__ == "__main__":
    main()
