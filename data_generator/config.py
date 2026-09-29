"""Parámetros del dataset sintético de Andina Market.

Todo lo que define volumetría, geografía y comportamiento vive aquí para que
el dataset sea reproducible (semilla fija) y fácil de ajustar.
"""
from datetime import datetime

SEED = 42

# Ventana histórica simulada (UTC). Dos años permiten estacionalidad
# interanual, clientes que abandonan y features con ventanas de 30/90/365 días.
START_DATE = datetime(2024, 10, 1)
END_DATE = datetime(2026, 9, 28, 23, 59, 59)

N_CUSTOMERS = 6000
N_PRODUCTS = 320

# País -> (peso, ciudades con peso, prefijo telefónico, locale de Faker)
COUNTRIES = {
    "PE": (0.35, {"Lima": 0.60, "Arequipa": 0.12, "Trujillo": 0.10, "Cusco": 0.09, "Piura": 0.09}, "+51 9", "es_CO"),
    "CO": (0.25, {"Bogotá": 0.45, "Medellín": 0.25, "Cali": 0.18, "Barranquilla": 0.12}, "+57 3", "es_CO"),
    "CL": (0.15, {"Santiago": 0.65, "Valparaíso": 0.18, "Concepción": 0.17}, "+56 9", "es_CL"),
    "MX": (0.15, {"Ciudad de México": 0.50, "Guadalajara": 0.20, "Monterrey": 0.18, "Puebla": 0.12}, "+52 55", "es_MX"),
    "EC": (0.10, {"Quito": 0.45, "Guayaquil": 0.40, "Cuenca": 0.15}, "+593 9", "es_CO"),
}

# Estacionalidad mensual (1.0 = mes promedio): Cyber/Black Friday en
# noviembre, Navidad en diciembre, Fiestas Patrias/vacaciones en julio.
MONTH_WEIGHT = {1: 0.80, 2: 0.78, 3: 0.92, 4: 0.95, 5: 1.05, 6: 1.00,
                7: 1.20, 8: 0.95, 9: 0.95, 10: 1.00, 11: 1.60, 12: 1.50}
WEEKDAY_WEIGHT = {0: 0.95, 1: 0.92, 2: 0.95, 3: 1.00, 4: 1.10, 5: 1.12, 6: 0.96}

# Hora del día (UTC-5 ~ local) -> peso. Picos al almuerzo y en la noche.
HOUR_WEIGHT = [0.2, 0.1, 0.05, 0.05, 0.05, 0.1, 0.3, 0.5, 0.8, 1.0, 1.1, 1.2,
               1.5, 1.4, 1.1, 1.0, 1.0, 1.1, 1.3, 1.6, 1.8, 1.7, 1.2, 0.6]

# Mezcla de canales: al inicio domina web; el app gana participación con el tiempo.
CHANNEL_MIX_START = {"web": 0.50, "app": 0.25, "tienda": 0.25}
CHANNEL_MIX_END = {"web": 0.38, "app": 0.42, "tienda": 0.20}

PAYMENT_METHODS = {
    "web": {"tarjeta": 0.62, "billetera": 0.18, "transferencia": 0.20},
    "app": {"tarjeta": 0.55, "billetera": 0.35, "transferencia": 0.10},
    "tienda": {"tarjeta": 0.55, "efectivo": 0.35, "billetera": 0.10},
}
# Probabilidad de que el primer intento de pago sea rechazado, por método.
REJECTION_RATE = {"tarjeta": 0.09, "billetera": 0.05, "transferencia": 0.03, "efectivo": 0.0}

ANNUAL_PRICE_INFLATION = 0.04  # deriva de precios de catálogo por año

# Reglas de segmento (las aplica el "CRM" de la fuente; ver README):
SEGMENT_RULES = {
    "vip_spend_365d": 1500.0,
    "vip_orders_365d": 12,
    "frecuente_orders_365d": 4,
    "nuevo_days": 90,
}
