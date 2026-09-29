"""Plantillas de tickets de soporte en español latinoamericano.

Cada tipo de ticket tiene asunto, cuerpos con variaciones de tono y una
distribución de prioridad. La prioridad la asigna el agente humano, así que
se agrega ruido: no todo "URGENTE" en el texto termina siendo Urgente, y
viceversa. Eso hace que el modelo de tickets urgentes (nivel 4) sea un
problema realista y no trivial.
"""
import numpy as np

# tipo -> (asuntos, cuerpos, {prioridad: prob}, requiere_pedido)
TICKET_TYPES = {
    "entrega_retrasada": (
        ["Mi pedido #{order} no ha llegado", "Pedido #{order} retrasado", "¿Dónde está mi pedido?"],
        [
            "Hola, hice el pedido #{order} hace {days} días y todavía no llega. El tracking no se actualiza. ¿Me pueden ayudar?",
            "Buenas, mi compra #{order} debía llegar el {weekday} y nada. Necesito el producto para esta semana.",
            "Ya van {days} días esperando el pedido #{order}. Nadie me responde por el chat. Es la segunda vez que me pasa.",
            "Pedido #{order}: el courier dice entregado pero yo no recibí nada!! Revisen por favor, es urgente.",
        ],
        {"Baja": 0.10, "Media": 0.50, "Alta": 0.30, "Urgente": 0.10}, True),
    "producto_defectuoso": (
        ["Producto llegó dañado", "Falla en {product}", "Producto defectuoso pedido #{order}"],
        [
            "El {product} llegó con la caja golpeada y no enciende. Quiero cambio o devolución.",
            "Compré {product} (pedido #{order}) y a los {days} días dejó de funcionar. Está en garantía.",
            "Hola, el {product} vino incompleto, falta el cable. Adjunto fotos.",
            "El {product} se sobrecalienta y huele a quemado, me da miedo usarlo. Necesito solución urgente.",
        ],
        {"Baja": 0.05, "Media": 0.40, "Alta": 0.40, "Urgente": 0.15}, True),
    "cobro_duplicado": (
        ["Me cobraron dos veces", "Cobro duplicado pedido #{order}", "URGENTE cobro doble"],
        [
            "Me aparecen dos cargos por el mismo pedido #{order} en mi tarjeta. Necesito que devuelvan uno YA.",
            "Buenas, pagué el pedido #{order} una vez pero el banco muestra dos cobros de {amount} USD.",
            "URGENTE: doble cobro en mi tarjeta por el pedido #{order}. Es mi sueldo, necesito el reembolso hoy.",
        ],
        {"Media": 0.10, "Alta": 0.40, "Urgente": 0.50}, True),
    "pago_rechazado": (
        ["No puedo pagar", "Pago rechazado", "Error al pagar pedido #{order}"],
        [
            "Intenté pagar con mi tarjeta y me sale rechazado, pero tengo saldo. ¿Qué pasa?",
            "El pago del pedido #{order} fue rechazado y luego lo intenté con billetera. ¿Se va a cobrar dos veces?",
            "Me sale error al pagar desde la app, ya probé 3 veces.",
        ],
        {"Baja": 0.20, "Media": 0.55, "Alta": 0.20, "Urgente": 0.05}, True),
    "reembolso": (
        ["Estado de mi reembolso", "Devolución pedido #{order}", "¿Cuándo me devuelven mi dinero?"],
        [
            "Devolví el pedido #{order} hace {days} días y aún no veo el reembolso.",
            "Hola, quiero saber el estado de la devolución del pedido #{order}. Ya entregué el producto en tienda.",
            "Ya pasaron más de {days} días hábiles y no me reembolsan. Si no me responden voy a presentar un reclamo formal.",
        ],
        {"Baja": 0.10, "Media": 0.50, "Alta": 0.30, "Urgente": 0.10}, True),
    "cambio_talla": (
        ["Cambio de talla", "Quiero cambiar la talla del pedido #{order}"],
        [
            "Hola, compré una talla que me queda chica. ¿Cómo hago el cambio?",
            "Quisiera cambiar el producto del pedido #{order} por una talla más grande, ¿puedo hacerlo en tienda?",
        ],
        {"Baja": 0.75, "Media": 0.25}, True),
    "consulta_producto": (
        ["Consulta sobre {product}", "¿Tienen stock?", "Pregunta antes de comprar"],
        [
            "¿El {product} tiene garantía? ¿Cuánto tiempo?",
            "Quería saber si el {product} es compatible con lo que ya tengo, y si hay stock en {city}.",
            "Hola, ¿hacen envíos a {city}? ¿Cuánto demora?",
        ],
        {"Baja": 0.85, "Media": 0.15}, False),
    "cuenta_acceso": (
        ["No puedo ingresar a mi cuenta", "Problema con mi contraseña", "Cambiar mi correo"],
        [
            "No me llega el correo para recuperar la contraseña.",
            "Quiero actualizar mi correo electrónico de la cuenta, el anterior ya no lo uso.",
            "La app me bota cada vez que inicio sesión.",
        ],
        {"Baja": 0.55, "Media": 0.40, "Alta": 0.05}, False),
    "cargo_no_reconocido": (
        ["Cargo no reconocido", "Posible fraude en mi cuenta", "No hice esta compra"],
        [
            "Me llegó un cargo de Andina Market que yo no hice. Creo que robaron mis datos.",
            "Alguien hizo una compra con mi cuenta, no reconozco el pedido. Bloqueen mi cuenta por favor, es urgente.",
        ],
        {"Alta": 0.30, "Urgente": 0.70}, False),
}

# Peso relativo de cada tipo cuando el ticket nace de un pedido "normal".
ORDER_TICKET_MIX = {"entrega_retrasada": 0.42, "producto_defectuoso": 0.25, "pago_rechazado": 0.08,
                    "cambio_talla": 0.10, "reembolso": 0.15}
STANDALONE_TICKET_MIX = {"consulta_producto": 0.55, "cuenta_acceso": 0.37, "cargo_no_reconocido": 0.08}

PRIORITY_RANK = ["Baja", "Media", "Alta", "Urgente"]
URGENCY_KEYWORDS = ("urgente", " ya.", "hoy", "fraude", "quemado", "robaron", "bloqueen", "miedo")

WEEKDAYS =["lunes", "martes", "miércoles", "jueves", "viernes", "sábado"]
TICKET_CHANNELS = {"email": 0.35, "chat": 0.35, "telefono": 0.10, "app": 0.20}


def _pick(rng, weights: dict):
    keys = list(weights)
    probs = np.array(list(weights.values()), dtype=float)
    return keys[rng.choice(len(keys), p=probs / probs.sum())]


def make_ticket(rng, ttype, order_id=None, product=None, city=None, amount=None):
    subjects, bodies, prio, _ = TICKET_TYPES[ttype]
    ctx = {"order": order_id or "", "product": product or "producto", "city": city or "mi ciudad",
           "days": int(rng.integers(3, 25)), "weekday": rng.choice(WEEKDAYS),
           "amount": f"{amount:.2f}" if amount else "0.00"}
    subject = rng.choice(subjects).format(**ctx)
    body = rng.choice(bodies).format(**ctx)
    # Variaciones de estilo real: todo en mayúsculas, sin tildes, texto vacío.
    r = rng.random()
    if r < 0.04:
        body = body.upper()
    elif r < 0.10:
        body = body.translate(str.maketrans("áéíóúÁÉÍÓÚ", "aeiouAEIOU"))
    elif r < 0.12:
        body = ""  # el cliente solo llenó el asunto
    priority = _pick(rng, prio)
    # El agente escala cuando el texto transmite urgencia o riesgo.
    if any(k in body.lower() for k in URGENCY_KEYWORDS):
        escalated = _pick(rng, {"Alta": 0.35, "Urgente": 0.65})
        priority = max(priority, escalated, key=PRIORITY_RANK.index)
    if rng.random() < 0.08:  # ruido del etiquetado humano
        priority = rng.choice(["Baja", "Media", "Alta", "Urgente"])
    return subject[:200], body, priority
