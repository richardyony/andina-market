"""Catálogo de productos de Andina Market.

Las marcas son ficticias. Cada tipo de producto trae un rango de precio y una
plantilla de descripción con atributos concretos (garantía, material,
capacidad...), porque esas descripciones alimentan luego los manuales de
producto del nivel RAG.
"""
import math
from datetime import timedelta

import numpy as np

from .config import ANNUAL_PRICE_INFLATION

BRANDS = {
    "Electrónica": ["Inti Tech", "Kallpa", "Andina Basics", "Nazca Audio"],
    "Hogar": ["Andina Home", "Mistura", "Kallpa", "Tumi Casa"],
    "Moda": ["Qhapaq", "Andina Basics", "Vicuña Wear", "Paracas"],
    "Deportes": ["Qhapaq Sport", "Cóndor", "Andina Basics"],
    "Belleza": ["Pachamama Beauty", "Killa", "Andina Basics"],
    "Mascotas": ["Andina Pets", "Huellitas", "Kallpa"],
}

# (categoria, subcategoria, nombre base, precio_min, precio_max, variantes, atributos)
PRODUCT_TYPES = [
    ("Electrónica", "Audio", "Audífonos inalámbricos", 25, 180, ["X1", "X2 Pro", "Sport", "Lite"],
     "Bluetooth 5.3, hasta {h} horas de batería, cancelación de ruido {anc}. Garantía de {g} meses."),
    ("Electrónica", "Audio", "Parlante bluetooth", 30, 250, ["Mini", "Go", "Max", "Party"],
     "Resistente al agua IPX{ip}, {h} horas de reproducción, potencia de {w} W. Garantía de {g} meses."),
    ("Electrónica", "Celulares y accesorios", "Cargador rápido USB-C", 12, 45, ["20W", "33W", "65W"],
     "Carga rápida compatible con PD 3.0, incluye cable de 1 m. Garantía de {g} meses."),
    ("Electrónica", "Celulares y accesorios", "Power bank", 20, 70, ["10000 mAh", "20000 mAh"],
     "Doble salida USB y USB-C, indicador LED de carga. Apto para equipaje de mano. Garantía de {g} meses."),
    ("Electrónica", "Computación", "Mouse inalámbrico", 10, 60, ["Office", "Ergo", "Gamer"],
     "Sensor óptico de {dpi} DPI, receptor USB nano, pilas incluidas. Garantía de {g} meses."),
    ("Electrónica", "Computación", "Teclado mecánico", 40, 150, ["TKL", "Full", "Compacto 60%"],
     "Switches {sw}, retroiluminación RGB, distribución en español latino. Garantía de {g} meses."),
    ("Electrónica", "Computación", "Laptop", 450, 1500, ["14\" i5 8GB", "15.6\" i7 16GB", "14\" Ryzen 5 16GB"],
     "SSD de {ssd} GB, pantalla Full HD, Windows 11. Garantía de {g} meses con servicio técnico autorizado."),
    ("Electrónica", "Computación", "Monitor", 120, 420, ["24\" FHD", "27\" QHD", "27\" 165Hz"],
     "Panel IPS, entradas HDMI y DisplayPort, soporte VESA. Garantía de {g} meses."),
    ("Electrónica", "Smart home", "Foco inteligente WiFi", 8, 25, ["Blanco", "RGB"],
     "Control por app y asistentes de voz, {lm} lúmenes, rosca E27. Garantía de {g} meses."),
    ("Electrónica", "Smart home", "Cámara de seguridad WiFi", 30, 120, ["Interior", "Exterior", "360°"],
     "Resolución {res}, visión nocturna, detección de movimiento, almacenamiento en microSD. Garantía de {g} meses."),
    ("Hogar", "Cocina", "Olla arrocera", 25, 90, ["1 L", "1.8 L", "2.8 L"],
     "Recubrimiento antiadherente, función mantener caliente, incluye vaporera. Garantía de {g} meses."),
    ("Hogar", "Cocina", "Licuadora", 35, 160, ["600W", "1000W", "1200W Pro"],
     "Vaso de vidrio de {cap} L, {vel} velocidades y función pulso. Garantía de {g} meses."),
    ("Hogar", "Cocina", "Sartén antiadherente", 12, 55, ["20 cm", "24 cm", "28 cm"],
     "Apta para inducción, libre de PFOA, mango ergonómico. Garantía de {g} meses."),
    ("Hogar", "Cocina", "Cafetera", 30, 220, ["Goteo", "Espresso", "Prensa francesa"],
     "Capacidad de {taz} tazas, filtro permanente lavable. Garantía de {g} meses."),
    ("Hogar", "Dormitorio", "Juego de sábanas", 20, 80, ["1.5 plazas", "2 plazas", "Queen", "King"],
     "Algodón de {hilos} hilos, incluye fundas de almohada. Lavable a máquina."),
    ("Hogar", "Dormitorio", "Almohada viscoelástica", 18, 60, ["Estándar", "Cervical"],
     "Espuma con memoria, funda removible e hipoalergénica."),
    ("Hogar", "Organización", "Caja organizadora", 6, 25, ["S", "M", "L"],
     "Plástico reforzado con tapa hermética, apilable."),
    ("Moda", "Ropa", "Polo básico", 9, 25, ["S", "M", "L", "XL"],
     "100% algodón pima peruano, corte regular. Cambio de talla dentro de 30 días."),
    ("Moda", "Ropa", "Casaca impermeable", 45, 140, ["S", "M", "L", "XL"],
     "Capucha desmontable, costuras selladas, bolsillos con cierre. Cambio de talla dentro de 30 días."),
    ("Moda", "Calzado", "Zapatillas urbanas", 40, 130, ["38", "40", "42", "44"],
     "Suela de caucho antideslizante, plantilla acolchada. Cambio de talla dentro de 30 días."),
    ("Moda", "Accesorios", "Mochila urbana", 25, 90, ["20 L", "28 L"],
     "Compartimento acolchado para laptop de 15.6\", tela repelente al agua."),
    ("Deportes", "Fitness", "Mat de yoga", 12, 45, ["4 mm", "6 mm"],
     "TPE antideslizante, incluye correa de transporte."),
    ("Deportes", "Fitness", "Set de mancuernas", 25, 120, ["10 kg", "20 kg"],
     "Discos intercambiables, barras con agarre antideslizante."),
    ("Deportes", "Ciclismo", "Bicicleta montañera", 280, 900, ["Aro 27.5", "Aro 29"],
     "Cuadro de aluminio, {cambios} cambios, frenos de disco. Garantía de {g} meses en cuadro."),
    ("Deportes", "Outdoor", "Botella térmica", 10, 35, ["500 ml", "750 ml", "1 L"],
     "Acero inoxidable, mantiene frío {hf} h y caliente {hc} h, libre de BPA."),
    ("Deportes", "Outdoor", "Carpa para camping", 60, 260, ["2 personas", "4 personas"],
     "Doble techo, columna de agua de {mm} mm, armado en 10 minutos."),
    ("Belleza", "Cuidado capilar", "Shampoo sin sulfatos", 6, 18, ["300 ml", "500 ml"],
     "Con extracto de quinua y aceite de sacha inchi. No testeado en animales."),
    ("Belleza", "Cuidado de la piel", "Crema hidratante facial", 10, 40, ["50 ml"],
     "Con ácido hialurónico y protector solar FPS {fps}. Apta para piel sensible."),
    ("Belleza", "Cuidado capilar", "Secadora de cabello", 25, 110, ["1800W", "2200W Iónica"],
     "{vel} velocidades, aire frío, boquilla concentradora. Garantía de {g} meses."),
    ("Mascotas", "Alimento", "Alimento para perro adulto", 15, 70, ["3 kg", "8 kg", "15 kg"],
     "Proteína de pollo, sin colorantes artificiales, con omega 3."),
    ("Mascotas", "Accesorios", "Cama para mascota", 18, 65, ["S", "M", "L"],
     "Relleno de fibra siliconada, base antideslizante, funda lavable."),
]

# Categorías de consumo recurrente: se compran en mayor cantidad.
BULK_CATEGORIES = {"Belleza", "Mascotas"}


def _attrs(rng):
    return {
        "h": rng.choice([12, 20, 30, 40]), "anc": rng.choice(["activa", "pasiva"]),
        "g": rng.choice([6, 12, 24]), "ip": rng.choice([5, 7]), "w": rng.choice([10, 20, 40]),
        "dpi": rng.choice([1600, 3200, 8000]), "sw": rng.choice(["rojos", "azules", "marrones"]),
        "ssd": rng.choice([256, 512, 1024]), "lm": rng.choice([806, 1055]),
        "res": rng.choice(["1080p", "2K"]), "cap": rng.choice([1.5, 2]), "vel": rng.choice([3, 5]),
        "taz": rng.choice([4, 8, 12]), "hilos": rng.choice([180, 300, 400]),
        "cambios": rng.choice([21, 24, 27]), "hf": 24, "hc": 12,
        "mm": rng.choice([2000, 3000]), "fps": rng.choice([30, 50]),
    }


def build_products(n_products: int, start, end, rng: np.random.Generator):
    """Devuelve una lista de dicts de producto, incluyendo metadatos internos
    (prefijo _) que el generador usa y que no se cargan a la base."""
    products = []
    sku_counter = {}
    total_days = (end - start).days
    type_idx = 0
    while len(products) < n_products:
        cat, sub, base, pmin, pmax, variants, desc = PRODUCT_TYPES[type_idx % len(PRODUCT_TYPES)]
        type_idx += 1
        brand = rng.choice(BRANDS[cat])
        variant = rng.choice(variants)
        name = f"{base} {brand} {variant}"
        if any(p["Name"] == name for p in products):
            name = f"{name} ({rng.choice(['Edición 2025', 'Negro', 'Gris', 'Azul', 'Blanco'])})"
            if any(p["Name"] == name for p in products):
                continue
        prefix = cat[:3].upper().replace("É", "E")
        sku_counter[prefix] = sku_counter.get(prefix, 0) + 1
        sku = f"{prefix}-{sku_counter[prefix]:05d}"
        base_price = float(np.round(rng.uniform(pmin, pmax), 0)) - 0.10  # precios terminados en .90

        # 80% ya existía al inicio; 20% se lanza durante el período.
        if rng.random() < 0.80:
            launch_days = -int(rng.integers(30, 720))
        else:
            launch_days = int(rng.integers(0, total_days - 30))
        launch = start + timedelta(days=launch_days, hours=int(rng.integers(9, 18)))

        # 6% se descontinúa en algún punto del período.
        discontinued_at = None
        if rng.random() < 0.06 and max(launch_days, 0) + 60 < total_days - 30:
            discontinued_at = start + timedelta(
                days=int(rng.integers(max(launch_days, 0) + 60, total_days)), hours=10)

        description = f"{name}. " + desc.format(**_attrs(rng))
        products.append({
            "SKU": sku, "Name": name, "Category": cat, "Subcategory": sub, "Brand": brand,
            "Description": description,
            "_base_price": base_price, "_launch": launch, "_discontinued_at": discontinued_at,
            # Cola larga (pocos productos venden mucho) y lo caro se vende menos.
            "_popularity": float((rng.pareto(1.3) + 0.2) * (40 / base_price) ** 0.7),
        })
    return products


def price_at(base_price: float, when, start, promo: bool) -> float:
    """Precio vigente en una fecha: deriva inflacionaria + descuento en campañas.
    Se ajusta a terminación .90, como publica precios el retail."""
    years = max((when - start).days, 0) / 365.0
    price = base_price * (1 + ANNUAL_PRICE_INFLATION) ** years
    if promo:
        price *= 0.85
    return round(math.floor(price) + 0.90, 2)
