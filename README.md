# Proyecto Final Computación Paralela y Distribuida (ICI-4344)
## Sistema de Mensajería Instantánea Multitransparente con Coordinación, Relojes de Lamport y Tolerancia a Fallos

Este repositorio contiene la implementación del **Proyecto Final** de la asignatura **Computación Paralela y Distribuida (ICI-4344)**. El sistema evoluciona la arquitectura cliente-servidor centralizada del Proyecto Parcial a una **topología multinodo completamente distribuida** y coordinada mediante paso de mensajes, relojes lógicos, exclusión mutua distribuida, elección y tolerancia a fallos.

---

## 🚀 Características Principales

### 1. Topología Multinodo (P2P / Multi-Servidor)
*   **Servidores Descentralizados**: El sistema cuenta con 3 nodos de servidor independientes (`NodoServidor 1`, `NodoServidor 2` y `NodoServidor 3`) ejecutándose en procesos/JVMs separados.
*   **Arquitectura de Red**: Cada servidor corre un `ServerSocket` para atender a sus clientes locales (puertos `5001`, `5002` y `5003`) y otro para interconectarse con los otros servidores en un anillo/malla P2P (puertos `6001`, `6002` y `6003`).
*   **Membresía del Clúster**: Los nodos mantienen un registro de estado dinámico (`MembresiaCluster`) sobre los peers conectados o caídos.

### 2. Ordenamiento de Eventos (Relojes de Lamport)
*   **Ausencia de Reloj Global**: La sincronización de mensajes se resuelve lógicamente mediante la implementación de **Relojes Lógicos de Lamport** (`RelojLamport.java`).
*   **Causalidad de Eventos**: Cada envío de mensaje/coordinación realiza un `tick()` local. Al recibir un paquete, se actualiza el reloj del nodo receptor según: $L_{rec} = \max(L_{local}, L_{msg}) + 1$.
*   **Bitácora Cronológica**: Todos los eventos (mensajes, elecciones, pasajes de token) se registran con su marca de tiempo lógica en archivos individuales dentro de la carpeta `logs/` (ej. `nodo_1_events.log`).

### 3. Coordinación Distribuida
*   **Elección de Coordinador (Algoritmo Bully)**: Cuando los nodos detectan la caída del coordinador actual (mediante la pérdida de heartbeats), inician de manera autónoma una elección usando el **Algoritmo del Abusón (Bully)**. El nodo activo con el ID más alto reclama el liderazgo e informa al resto.
*   **Exclusión Mutua (Token Ring)**: El acceso al recurso crítico simulado (`RecursoCritico.java`) se gestiona mediante un **Token Ring**. Un token único circula de forma continua entre los nodos del servidor activos (`1 ➔ 2 ➔ 3 ➔ 1`). Solo el poseedor del token puede procesar operaciones de escritura crítica.

### 4. Tolerancia a Fallos Independientes
*   **Detección por Heartbeats**: Los nodos servidores monitorean la vida del coordinador mediante el envío periódico de latidos (`HeartbeatManager.java`). Si no se recibe respuesta tras un timeout configurado, se asume la caída del nodo.
*   **Recuperación Dinámica**: Tras un crash, el sistema se reorganiza automáticamente, disparando una elección y reconectando las rutas de comunicación activa sin detener el servicio para los clientes.

### 5. Pruebas de Carga y Tránsito (Stress Testing)
*   **Generador de Carga**: Un módulo especializado (`GeneradorCarga.java`) lanza de forma concurrente **50 clientes simulados** distribuidos en round-robin entre los 3 servidores por un periodo de **60 segundos**.
*   **Métricas en Tiempo Real**: Mide el rendimiento a través de tasas de envío, latencia promedio, percentil 95 (P95), tasa de error y calcula el **tiempo de recuperación** exacto tras inducir una falla.

---

## 📁 Estructura del Código

```text
app/src/main/java/org/example/
├── Config.java                   # Centraliza puertos, timeouts y constantes del clúster
├── PaqueteMensaje.java           # Definición del objeto serializado estandarizado
├── NodoServidor.java             # Motor principal del nodo servidor distribuido (reemplaza a App.java)
├── Cliente.java                  # Cliente interactivo humano para chat y comandos
├── NodoBot.java                  # Bot autónomo de comandos (se conecta a cualquier servidor activo)
│
├── distributed/                  # Módulos distribuidos del proyecto final
│   ├── IRelojLamport.java        # Interfaz de reloj lógico
│   ├── RelojLamport.java         # Implementación de Lamport
│   ├── IEventLog.java            # Interfaz de registro de eventos
│   ├── EventLog.java             # Registro en logs/nodo_X_events.log
│   ├── ConexionPeer.java         # Gestión del socket entre servidores
│   ├── MembresiaCluster.java     # Mantenimiento del clúster y estados
│   ├── HeartbeatManager.java     # Envío y recepción de latidos de vida
│   ├── AlgoritmoBully.java       # Implementación del algoritmo abusón
│   ├── TokenRing.java            # Anillo de exclusión mutua
│   ├── RecursoCritico.java       # Recurso compartido distribuido
│   └── TestRelojLamport.java     # Test básico unitario del reloj
│
└── loadtest/                     # Módulo de pruebas de carga
    ├── ClienteCarga.java         # Hilo que simula un cliente activo enviando spam
    ├── RecolectorMetricas.java   # Contadores de rendimiento thread-safe
    └── GeneradorCarga.java       # Orquestador del test de estrés multinodo
```

---

## 🛠️ Instrucciones de Ejecución

### 1. Compilar el Proyecto
Desde la raíz del proyecto, compila las clases de Java ejecutando:
```bash
./gradlew classes
```

### 2. Iniciar el Clúster de Servidores
Para levantar el clúster multinodo de manera local, debes iniciar 3 terminales separadas, una para cada `NodoServidor`:

*   **Terminal 1 (Nodo 1)**:
    ```bash
    java -cp app/build/classes/java/main org.example.NodoServidor 1
    ```
*   **Terminal 2 (Nodo 2)**:
    ```bash
    java -cp app/build/classes/java/main org.example.NodoServidor 2
    ```
*   **Terminal 3 (Nodo 3)**:
    ```bash
    java -cp app/build/classes/java/main org.example.NodoServidor 3
    ```

*El Nodo 3, al tener el ID más alto, se convertirá en el Coordinador inicial.*

### 3. Iniciar el NodoBot
El bot autónomo puede conectarse a cualquiera de los puertos de cliente activos (puertos `5001`, `5002` o `5003`):
```bash
java -cp app/build/classes/java/main org.example.NodoBot localhost 5003
```

### 4. Conectar Clientes Interactivos
Puedes abrir múltiples terminales para simular usuarios humanos chateando desde diferentes servidores. Los mensajes se replicarán a todo el clúster usando la comunicación P2P entre servidores.

*   **Cliente en Servidor 1**:
    ```bash
    java -cp app/build/classes/java/main org.example.Cliente localhost 5001
    ```
*   **Cliente en Servidor 2**:
    ```bash
    java -cp app/build/classes/java/main org.example.Cliente localhost 5002
    ```

---

## 📊 Demostración de Falla Inducida e Informe de Carga

El sistema incluye una simulación guiada para probar la resiliencia bajo estrés.

### Ejecutar el Test de Estrés
1. Asegúrate de tener iniciados los 3 servidores (`NodoServidor 1, 2, 3`) y el `NodoBot`.
2. Ejecuta el generador de carga desde otra terminal:
   ```bash
   java -cp app/build/classes/java/main org.example.loadtest.GeneradorCarga
   ```
3. El programa levantará **50 hilos concurrentes** enviando mensajes en round-robin.
4. **Inducir Caída del Coordinador**: A los **~30 segundos** de la prueba, ve a la terminal del **Nodo 3** y detén el proceso forzosamente (`Ctrl+C`).
5. Vuelve rápidamente a la terminal del `GeneradorCarga` y presiona **`ENTER`** para marcar el instante exacto de la falla.
6. El clúster detectará la desconexión del Nodo 3 mediante la pérdida de heartbeats, disparará el algoritmo Bully en los Nodos 1 y 2, elegirá al Nodo 2 como nuevo coordinador, reactivará el Token Ring y continuará operando.
7. Al expirar los 60 segundos, verás el reporte de métricas impreso en consola.

---

## 📂 Archivos de Logs Generados
Durante la ejecución, cada servidor genera una bitácora detallada con marcas de tiempo lógicas. Puedes encontrarlos en:
*   `logs/nodo_1_events.log`
*   `logs/nodo_2_events.log`
*   `logs/nodo_3_events.log`

Ejemplo de línea de Log con Reloj de Lamport:
```text
[Reloj Lamport: 142] - [Fallas] - [HEARTBEAT] - Enviando heartbeat a coordinador
[Reloj Lamport: 145] - [TOKEN] - [TOKEN_PASS] - Token recibido. Ejecutando cola...
```
