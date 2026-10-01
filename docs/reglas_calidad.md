# Reglas de limpieza, transformación y cuarentena

Catálogo completo de lo que el pipeline hace con cada dato entre bronze y gold. Cada regla dice qué detecta, qué acción toma y dónde queda la evidencia. El principio que las ordena: **bronze guarda lo recibido sin tocarlo, silver corrige con trazabilidad y nada se descarta en silencio** (D-10, D-14).

## 1. Acciones posibles

| Acción | Qué pasa con el dato | Dónde queda la evidencia |
|---|---|---|
| **fail** | El pipeline se detiene; no se publica nada de ese lote | Event log del pipeline |
| **reject** | No entra al estado actual de silver | `silver.rejected_order_items` (cada cambio rechazado) y `silver.data_quality_issues` |
| **quarantine** | Se conserva, se aparta en una tabla para corregir y en gold apunta a un miembro "desconocido" | `silver.quarantine_order_items` y `silver.data_quality_issues` |
| **warn** | Se conserva tal cual (o corregido, guardando el original) y se marca | Métricas de la expectation en el event log y `silver.data_quality_issues` |
| **info** | Normalización esperada; se registra para que se pueda medir | `silver.data_quality_issues` |

Las métricas de cada expectation se ven en la interfaz del pipeline (pestaña **Data quality** de cada tabla). `silver.data_quality_issues` dice además cuál registro y por qué, sin copiar datos personales.

## 2. Limpieza y normalización (vistas `*_changes`)

| Entidad | Campo | Regla | Ejemplo |
|---|---|---|---|
| Todas | Nombres de columna | `PascalCase` de la fuente → `snake_case` de negocio | `CustomerId` → `customer_id` |
| Todas | Textos | `trim` de espacios en nombres, SKU, asunto | `" Ana "` → `"Ana"` |
| Clientes | `email_norm` | `lower(trim(email))`; vacío → NULL. El original se conserva en `email` | `" Ana@Mail.COM "` → `ana@mail.com` |
| Clientes | `email_valido` | Regex `usuario@dominio.ext` | `ana@@mail.com` → `false` |
| Clientes | `country_iso` | Mayúsculas, sin espacios ni tildes, y mapa a ISO-2. Lo no reconocido queda NULL. El original se conserva en `country_raw` | `" pe"`, `Perú`, `PERU` → `PE`; `MEX`, `México` → `MX` |
| Clientes | `city` | NULL → `"Desconocida"` | |
| Pedidos | `channel` | `lower(trim)` | `"Web "` → `web` |
| Pedidos | `order_date` | Si `OrderDate` supera en más de un día a `CreatedAt` (año mal digitado), se usa `CreatedAt`. El original queda en `order_date_raw` y `order_date_corrected = true` | `2027-03-10` → `2026-03-10` |
| Pedidos | `coupon_code` | Columna que llegó con un cambio de esquema; NULL donde aún no existe | |
| Líneas | `line_amount` | `quantity × unit_price` en `decimal(14,2)` | |
| Pagos | `method` | `lower(trim)` | |
| Tickets | `body` | `''` o solo espacios → NULL | |
| Productos | `is_active` | `status = 'Active'` | |

## 3. Validaciones (expectations)

| Entidad | Regla | Condición | Acción |
|---|---|---|---|
| Todas | `pk_presente` | La clave primaria no es nula | **fail** |
| Clientes | `email_formato_valido` | Email nulo o con formato válido | warn |
| Clientes | `pais_reconocido` | País nulo o mapeado a ISO-2 | warn |
| Clientes | `segmento_conocido` | Nuevo, Regular, Frecuente o VIP | warn |
| Productos | `precio_positivo` | `price > 0` | warn |
| Productos | `estado_conocido` | Active o Discontinued | warn |
| Pedidos | `fecha_no_futura` | El año no venía mal digitado | warn (se corrige) |
| Pedidos | `canal_conocido` | web, app o tienda | warn |
| Pedidos | `total_no_negativo` | `total_amount >= 0` | warn |
| Líneas | `cantidad_positiva` | `quantity > 0` | warn + **reject** del estado actual |
| Líneas | `precio_no_negativo` | `unit_price >= 0` | warn |
| Pagos | `monto_positivo` | `amount > 0` | warn |
| Pagos | `estado_conocido` | Pendiente, Aprobado, Rechazado o Reembolsado | warn |
| Tickets | `prioridad_conocida` | Baja, Media, Alta o Urgente | warn |
| Tickets | `cuerpo_presente` | El cuerpo no es nulo después de normalizar | warn |
| Gold (hechos) | `cliente_asignado`, `fecha_asignada`, `producto_asignado` | La fila apunta a sus dimensiones | warn; la tarea de validación del job **falla** si alguna no es 0 |
| Gold (líneas) | `importe_no_negativo` | `line_amount >= 0` | warn |

**Por qué casi todo es `warn`:** un email mal escrito o un país sucio no invalidan una venta. Descartar el registro haría que los totales dejaran de cuadrar con la fuente. Solo se detiene el pipeline si el dato rompe la estructura (sin clave no se puede aplicar ningún cambio), y solo se rechaza lo que no tiene sentido de negocio (una línea con cantidad 0 no es una venta).

## 4. Reglas de integridad entre entidades (`silver.data_quality_issues`)

| Regla | Qué detecta | Acción | Causa que se informa |
|---|---|---|---|
| `producto_inexistente` | Línea con `ProductId` que no existe en el catálogo | quarantine | `product_id` faltante |
| `pedido_sin_lineas` | Pedido sin ninguna línea en silver | warn | "sin líneas en la fuente" o "solo tenía líneas con cantidad 0" |
| `total_no_cuadra_con_lineas` | Total del pedido ≠ suma de sus líneas (tolerancia 0,01) | warn | "descuento en la cabecera" o "línea con cantidad 0 excluida" |
| `cantidad_no_positiva` | Línea cuya última versión tiene cantidad ≤ 0 | reject | pedido y cantidad |
| `cliente_duplicado` | Cuenta que parece la misma persona que otra | warn | cuenta principal y regla que la detectó |
| `doble_cobro` | Pago aprobado duplicado | warn | pago original, segundos de diferencia, estado actual |
| `email_formato_invalido` | Email con formato inválido | warn | tipo de error ("sin @", "@ repetida"), no el email |
| `pais_no_reconocido` / `pais_normalizado` | País que no se pudo mapear / que sí se normalizó | warn / info | valor original → ISO-2 |
| `fecha_pedido_futura` | Pedido con el año mal digitado | warn | fecha original y fecha usada |
| `ticket_sin_cuerpo` | Ticket sin texto | info | canal |
| `columna_no_mapeada` | Columna que existe en bronze y ninguna vista de silver lleva | warn | tabla y columna |

## 5. Reglas de detección

**Clientes duplicados** (`silver.customer_duplicate_groups`, `silver.customer_duplicates`). Dos reglas independientes:
1. **Email canónico:** email válido, en minúsculas, sin espacios y sin la etiqueta `+...` (`ana+tienda@mail.com` = `ana@mail.com`). Los valores de relleno como `sin-correo` no cuentan.
2. **Nombre y teléfono:** mismo nombre completo y mismo teléfono (solo dígitos). Cubre las cuentas de tienda sin email.

La cuenta principal es la más antigua. Las cuentas **se marcan y no se fusionan**: puede haber homónimos y fusionar es una decisión del negocio. Resultado: las 57 del catálogo; 47 las detectan ambas reglas.

**Doble cobro** (`silver.payment_double_charges`): dos pagos que estuvieron **Aprobados** (según el historial SCD2, aunque hoy estén Reembolsados) para el mismo pedido y monto, con menos de 60 segundos entre su creación. El segundo es el duplicado. `duplicate_current_status` indica si ya se devolvió.

## 6. Cambios en el tiempo (AUTO CDC)

| Regla | Detalle |
|---|---|
| Orden de los cambios | `_ct_version` de Change Tracking: el orden real de la fuente, no el reloj de la aplicación |
| DELETE | `_ct_operation = 'D'` borra la fila del estado actual; en las líneas también una cantidad ≤ 0 |
| Estado actual (SCD1) | Las 6 entidades |
| Historial (SCD2) | Solo `segment` del cliente y `status` del pago abren versiones nuevas |
| Vigencia | `__START_AT` / `__END_AT` con la versión y `updated_at`; en `gold.dim_customer`, `valid_from` / `valid_to`, y la primera versión se abre en 1900-01-01 |
| Recarga completa | Toda clave vigente en bronze que no aparece en el snapshot nuevo genera un DELETE sintético (ver D-09) |

## 7. Transformación a gold

| Regla | Detalle |
|---|---|
| Point-in-time | Cada pedido toma la versión del cliente vigente en `order_date` |
| Miembro desconocido | Líneas en cuarentena → `product_key = -1` "Producto desconocido": las ventas cuadran con la fuente |
| Claves sustitutas | `customer_key = xxhash64(customer_id, versión)`: deterministas, idénticas en cada recálculo |
| Calendario | Desde el 1 de enero del primer año con datos hasta el 31 de diciembre del año siguiente al último |
| Datos personales | Gold no tiene nombre, email ni teléfono; solo `has_email`, `email_valido`, ciudad y país |
| Flags en hechos | `order_date_corrected`, `has_no_lines`, `total_difference`, `has_double_charge`, `is_double_charge` |

## 8. Validación de punta a punta (última tarea del job)

`src/validacion/03_validar_lakehouse.py` falla el job si algo no cuadra y deja el resultado en `ops.validation_log`:

- estado actual reconstruido desde bronze = silver, por tabla;
- una sola fila por clave en silver;
- gold = silver en ventas, líneas, pedidos y pagos;
- ningún hecho sin cliente, fecha o producto, y toda fecha dentro del calendario;
- una sola versión vigente por cliente en `dim_customer`.
