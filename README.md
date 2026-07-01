# Taller 3 Kafka

Integrantes:

- Daniel Esteban Rodriguez Suarez

- Hildebrando Peña Quezada

Proyecto Spring Boot para el laboratorio de Apache Kafka y arquitecturas orientadas por eventos.

## Como ejecutar

1. Levanta Kafka y Kafka UI con Docker Compose.
2. Ejecuta la aplicacion Spring Boot en Java 21.
3. Invoca `POST http://localhost:8081/orders` con un JSON como:

```json
{
  "customerId": "CUS-01",
  "total": 120000
}
```

## Actividad 1. Analisis de comunicacion

| Proceso | Tipo | Justificacion |
| --- | --- | --- |
| Consultar catalogo | Sincrono | El usuario espera respuesta inmediata para navegar y comprar. |
| Crear pedido | Hibrido | Responde rapido con el pedido creado, pero el procesamiento posterior puede seguir asíncrono. |
| Validar pago | Hibrido | La confirmacion inicial puede ser inmediata, pero el procesamiento interno puede continuar por eventos. |
| Enviar notificacion | Asincrono | No bloquea la experiencia del usuario y puede reintentarse sin afectar el flujo principal. |
| Actualizar analitica | Asincrono | Es un proceso posterior, independiente del tiempo de respuesta del usuario. |
| Registrar auditoria | Asincrono | Debe desacoplarse del flujo transaccional para no aumentar latencia. |
| Consultar estado del pedido | Sincrono | El usuario necesita ver el estado actual al momento de consultar. |
| Actualizar inventario | Asincrono | Puede reaccionar al evento de pedido sin bloquear la creacion del pedido. |

## Actividad 2. Decisiones de configuracion

### Riesgos de una configuracion debil

| Configuracion | Problema | Impacto |
| --- | --- | --- |
| Topic unico `events` | Mezcla dominios | Baja mantenibilidad y trazabilidad |
| 1 particion | Sin paralelismo real | Menor escalabilidad |
| RF=1 | Un solo broker como copia | Menor disponibilidad |
| Sin clave | Orden inconsistente por entidad | Riesgo de reordenamiento |
| Retencion 12-24h | Poco historial | Dificulta reprocesamiento y auditoria |
| Sin `eventId` | Duplicados difíciles de detectar | Falta de idempotencia |
| Sin `correlationId` | Trazabilidad incompleta | Difícil seguir el flujo extremo a extremo |
| Un solo Consumer Group | No hay fan-out real | Se rompe el desacoplamiento |
| Sin DLT | Eventos fallidos se pierden o quedan bloqueados | Recuperacion deficiente |
| Sin lag monitoring | Sin observabilidad operativa | Fallos invisibles |

### Mejoras prioritarias

1. Separar topics por dominio o proceso.
2. Definir claves por entidad, normalmente `orderId`.
3. Aumentar particiones segun volumen esperado.
4. Usar `eventId` y `correlationId`.
5. Implementar DLT y reintentos controlados.
6. Agregar monitoreo de lag, errores y tiempos de proceso.
7. Elevar el factor de replicacion en produccion.

## Actividad 3. Implementacion base y trazabilidad

### Estructura implementada

- `src/main/java/edu/eci/arsw/kafka/config/KafkaTopicConfig.java`
- `src/main/java/edu/eci/arsw/kafka/controller/OrderController.java`
- `src/main/java/edu/eci/arsw/kafka/dto/CreateOrderRequest.java`
- `src/main/java/edu/eci/arsw/kafka/dto/OrderCreatedEvent.java`
- `src/main/java/edu/eci/arsw/kafka/producer/OrderEventProducer.java`
- `src/main/java/edu/eci/arsw/kafka/consumer/PaymentEventConsumer.java`
- `src/main/java/edu/eci/arsw/kafka/consumer/OrderEventConsumer.java`

### Flujo funcional

1. El cliente invoca `POST /orders` con `customerId` y `total`.
2. `OrderController` crea un `OrderCreatedEvent` con `eventId`, `correlationId`, `orderId`, `status` y `occurredAt`.
3. `OrderEventProducer` publica el evento en el topic `orders` usando `orderId` como clave.
4. Kafka asigna la particion segun la clave y el numero de particiones del topic.
5. `PaymentEventConsumer` consume el evento en el grupo `payment-service` y publica `PaymentProcessedEvent` en `payments`.
6. `OrderEventConsumer` consume el evento en el grupo `inventory-service` y publica `InventoryProcessedEvent` en `inventory`.

### Trazabilidad del evento

- HTTP: `POST /orders`
- Producer: `OrderEventProducer`
- Topic: `orders`
- Clave: `orderId`
- Particion: determinada por Kafka con base en la clave; se conserva el orden dentro de la misma particion.
- Consumer Group 1: `payment-service`
- Consumidor 1: `PaymentEventConsumer`
- Topic derivado: `payments`
- Consumer Group 2: `inventory-service`
- Consumidor 2: `OrderEventConsumer`
- Topic derivado: `inventory`

## Actividad 4. Extension con pagos e inventario

### Reglas de negocio

- Pago aprobado si `total <= 250000`.
- Inventario reservado si `total <= 300000`.
- En ambos casos se reutiliza `orderId` como clave de particionamiento para conservar orden por pedido.

### Consumer groups separados

- `payment-service` recibe todos los pedidos y produce eventos de pago.
- `inventory-service` recibe todos los pedidos y produce eventos de inventario.
- Son grupos distintos porque ambos deben recibir el mismo evento original.

## Actividad 5. Manejo de errores e idempotencia

### Estrategia aplicada para inventory-service

- `DefaultErrorHandler` con `DeadLetterPublishingRecoverer`.
- Reintentos: 3.
- Espera entre reintentos: 2000 ms.
- Topic de destino tras agotar reintentos: `orders.DLT`.

### Idempotencia recomendada

El consumidor debe guardar `eventId` en una tabla o repositorio de eventos procesados. Antes de procesar un evento:

1. Verificar si `eventId` ya fue procesado.
2. Si ya existe, ignorar el evento.
3. Si no existe, procesarlo y registrar `eventId` como completado.

Esto evita efectos duplicados si Kafka reintenta o si el consumidor reinicia luego de haber confirmado parcialmente.

## Actividad 6. Diagnostico de una configuracion mala

### Problemas identificados

- Topic unico `events` mezcla dominios distintos y degrada la claridad del modelo.
- Una sola particion elimina paralelismo real.
- `RF=1` reduce disponibilidad y tolerancia a fallos.
- Mensajes sin clave rompen el orden por entidad.
- Retencion corta limita reprocesamiento y auditoria.
- Sin `eventId` no hay idempotencia confiable.
- Sin `correlationId` la trazabilidad end to end queda incompleta.
- Un solo Consumer Group impide fan out real.
- Sin DLT se pierden eventos fallidos.
- Sin monitoreo de lag no hay observabilidad operativa.

### Atributos afectados

- Escalabilidad: baja por una sola particion y un unico topic.
- Disponibilidad: baja por factor de replicacion 1.
- Trazabilidad: baja por ausencia de `eventId` y `correlationId`.
- Mantenibilidad: baja por mezcla de dominios.
- Observabilidad: baja sin lag ni DLT.

### Mejoras prioritarias

1. Separar topics por dominio o proceso.
2. Definir claves por entidad, normalmente `orderId`.
3. Aumentar particiones segun volumen esperado.
4. Usar `eventId` y `correlationId`.
5. Implementar DLT y reintentos controlados.
6. Agregar monitoreo de lag, errores y tiempos de proceso.
7. Elevar el factor de replicacion en produccion.

## Actividad 7. Arquitectura completa del reto final

### Servicios

| Servicio | Responsabilidad |
| --- | --- |
| `order-service` | Crear pedidos y publicar `order-created`. |
| `payment-service` | Procesar pagos y publicar `payment-approved` o `payment-rejected`. |
| `inventory-service` | Validar disponibilidad y publicar `inventory-reserved` o `inventory-rejected`. |
| `invoice-service` | Generar facturas cuando el pago sea aprobado. |
| `notification-service` | Enviar notificaciones al cliente. |
| `analytics-service` | Consumir eventos para analitica. |
| `audit-service` | Registrar trazabilidad y auditoria. |

### Eventos

| Evento | Dominio |
| --- | --- |
| `order-created` | Pedido creado. |
| `payment-approved` | Pago aprobado. |
| `payment-rejected` | Pago rechazado. |
| `inventory-reserved` | Inventario reservado. |
| `inventory-rejected` | Inventario rechazado. |
| `invoice-generated` | Factura generada. |
| `notification-sent` | Notificacion enviada. |
| `audit-record-created` | Registro de auditoria creado. |

### Topics y claves

| Topic | Eventos principales | Clave sugerida |
| --- | --- | --- |
| `orders` | `order-created`, `order-cancelled` | `orderId` |
| `payments` | `payment-approved`, `payment-rejected` | `orderId` |
| `inventory` | `inventory-reserved`, `inventory-rejected` | `orderId` |
| `invoices` | `invoice-generated`, `invoice-failed` | `orderId` |
| `notifications` | `notification-sent`, `notification-failed` | `orderId` |
| `audit` | `audit-record-created` | `correlationId` |

### Consumer groups

| Servicio consumidor | Consumer Group |
| --- | --- |
| `payment-service` | `payment-service` |
| `inventory-service` | `inventory-service` |
| `invoice-service` | `invoice-service` |
| `notification-service` | `notification-service` |
| `analytics-service` | `analytics-service` |
| `audit-service` | `audit-service` |

### Retencion, replicacion y observabilidad

- En laboratorio: `RF=1` y 3 particiones por topic son suficientes para validar el flujo.
- En produccion: usar `RF > 1` para tolerancia a fallos.
- Retencion: definirla segun auditoria, reprocesamiento y costo de almacenamiento.
- Observabilidad: monitorear lag, fallos, latencia de consumo y eventos enviados a DLT.

## Actividad 8. Por que no conviene un topic global `events`

- Mezcla dominios y contratos distintos en un solo canal.
- Complica el versionado de eventos.
- Hace mas dificil aplicar claves y particiones correctas.
- Aumenta el acoplamiento entre consumidores.
- Dificulta la seguridad, gobernanza y monitoreo por dominio.
- Reduce claridad operativa al diagnosticar fallos o lag.

## Actividad 9. Entregable final

La solucion separa el flujo de pedidos en topics por dominio, conserva orden por `orderId`, permite fan out con consumer groups distintos y agrega DLT para manejo de errores. El diseño propuesto favorece desacoplamiento, observabilidad y consistencia eventual.