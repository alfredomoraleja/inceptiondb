# Análisis de cuellos de botella en `insert`

## Resumen del flujo
1. El handler HTTP `insert` habilita el modo full-duplex, busca/crea la colección y recorre los documentos del `Body` con un `json.Decoder`, insertando uno a uno y escribiendo la respuesta con `fmt.Fprintln` sobre el `http.ResponseWriter`.【F:api/apicollectionv1/insert.go†L15-L113】
2. Cada llamada a `collection.Insert` agrega valores por defecto, serializa el item, actualiza los índices in-memory (`indexInsert`) y persiste el comando en el archivo de la colección mediante `json.NewEncoder(c.file).Encode`.【F:collection/collection.go†L18-L27】【F:collection/collection.go†L162-L220】【F:collection/collection.go†L361-L388】
3. Los índices `map`/`sync.Map` deserializan de nuevo el payload JSON de la fila para inspeccionar campos clave en cada inserción.【F:collection/indexmap.go†L58-L110】【F:collection/indexsyncmap.go†L56-L99】

## Posibles cuellos de botella y bloqueos

### Capa HTTP
- **Decodificación y asignaciones repetidas.** El handler crea un mapa nuevo y decodifica JSON para cada documento. El GC debe colectar cada mapa y los valores `interface{}` asociados. Reutilizar mapas o usar tipos concretos reduciría presión de GC.
- **Conversión redundante al responder.** `fmt.Fprintln(w, string(row.Payload))` crea una copia completa del payload antes de enviarlo, duplicando trabajo en respuestas voluminosas. Enviar el `[]byte` directamente (por ejemplo con `w.Write(row.Payload)` o un `json.Encoder` reutilizable) evitaría la conversión.【F:api/apicollectionv1/insert.go†L93-L104】
- **Full-duplex innecesario.** `EnableFullDuplex()` bloquea la cabecera de respuesta y activa mecanismos para streaming aunque el handler termina devolviendo un cuerpo simple (líneas separadas). Sin clientes que lean incrementalmente, este paso solo añade trabajo extra al stack HTTP.【F:api/apicollectionv1/insert.go†L17-L21】

### Capa Service / Collection
- **Serialización doble por inserción.** `collection.Insert` convierte el `map[string]any` a JSON para persistir y, acto seguido, cada índice vuelve a deserializar la misma carga para buscar los campos indexados. En colecciones con muchos índices esto multiplica la CPU consumida por operación.【F:collection/collection.go†L195-L220】【F:collection/indexmap.go†L58-L110】【F:collection/indexsyncmap.go†L56-L99】
- **Acceso a índices sin protección global.** El mapa `Collection.Indexes` no está protegido y se itera directamente en `indexInsert`. Si otra gorutina crea/elimina índices simultáneamente podrían aparecer carreras o bloqueos inesperados; una `RWMutex` alrededor de la tabla de índices mitigaría el riesgo.【F:collection/collection.go†L18-L33】【F:collection/collection.go†L300-L359】【F:collection/collection.go†L361-L388】
- **Falta de sincronización para escritura a disco.** Varias inserciones concurrentes comparten `c.file` pero cada una crea su propio `json.NewEncoder(c.file)` sin ninguna exclusión mutua. El `Encoder` escribe directamente sobre el descriptor, por lo que las inserciones podrían intercalar bytes y provocar contención en el kernel o corrupción lógica. Un `sync.Mutex` dedicado al archivo (o un buffer canalizado) evitaría este punto caliente.【F:collection/collection.go†L215-L217】
- **Ausencia de buffer de escritura.** Hay un `TODO` explícito sobre usar `bufio.Writer` porque mejora 3x el rendimiento. Cada `Encode` provoca syscalls pequeñas; amortizarlas con buffer reduce latencia y carga de CPU.【F:collection/collection.go†L20-L26】【F:collection/collection.go†L215-L217】
- **Bloqueo por `rowsMutex`.** El mutex se usa solo para hacer `append` en la slice de filas, lo que es rápido pero serializa todas las inserciones aunque el resto del trabajo sea thread-safe. Promover una estructura lock-free o fragmentar la slice por shards podría elevar el throughput.【F:collection/collection.go†L142-L159】

### Índices
- **Re-deserialización por índice.** Tanto `IndexMap` como `IndexSyncMap` vuelven a hacer `json.Unmarshal` sobre `row.Payload` en cada inserción para obtener el campo indexado. Con N índices se realizan N deserializaciones, aún cuando el handler ya tenía los datos estructurados. Guardar la representación ya parseada o los campos indexados dentro de `Row` reduciría este costo.【F:collection/indexmap.go†L58-L110】【F:collection/indexsyncmap.go†L56-L99】
- **Bloqueos parciales.** `IndexMap` usa un `RWMutex` solo en la rama de valores escalares; el camino de arreglos (`[]interface{}`) accede a `entries` sin el candado, lo que puede generar carreras y corrupciones con alta concurrencia. Alinear ambos caminos usando el mismo mutex disminuiría errores y reintentos caros.【F:collection/indexmap.go†L94-L104】
- **Coste del `sync.Map`.** `IndexSyncMap` evita un mutex pero paga con boxing y asignaciones adicionales; además, al no existir búsqueda previa en memoria local, cada llamada genera un `interface{}` -> `*Row`. Para índices pequeños un `map` con `RWMutex` puede ser más barato.【F:collection/indexsyncmap.go†L75-L99】

## Recomendaciones
- Reutilizar un `json.Decoder` + estructura prealocada en el handler y escribir la respuesta con `io.Writer` directo para reducir copias.【F:api/apicollectionv1/insert.go†L59-L104】
- Añadir un `mutex`/buffer alrededor de `c.file` y reutilizar un `json.Encoder` por colección para agrupar escrituras.【F:collection/collection.go†L20-L26】【F:collection/collection.go†L215-L217】
- Conservar los campos indexados (o el `map` ya parseado) dentro de `Row` para que los índices no tengan que deserializar repetidamente.【F:collection/indexmap.go†L58-L110】【F:collection/indexsyncmap.go†L56-L99】
- Revisar el uso del `rowsMutex` (sharding, lock-free queue) y garantizar que todas las ramas de `IndexMap` usen el mutex para evitar bloqueos ocultos por reintentos o corrupción.【F:collection/collection.go†L142-L159】【F:collection/indexmap.go†L94-L104】
