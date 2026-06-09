# Proyecto Final: Computación Paralela y Distribuida (ICI-4344)
## Sistema de Mensajería Instantánea Distribuido, Coordinado y Tolerante a Fallos con Relojes de Lamport

Este proyecto consiste en la evolución de una arquitectura cliente-servidor centralizada tradicional de mensajería instantánea hacia una **topología multinodo completamente distribuida (peer-to-peer/multi-servidor)**. El sistema está coordinado mediante paso de mensajes serializados, relojes lógicos de Lamport para el ordenamiento parcial de eventos, exclusión mutua distribuida basada en un Token Ring con mitigación de duplicación por épocas, algoritmos de elección de líder (Bully) y mecanismos robustos de tolerancia a fallos y sincronización de estado.

---

## 🚀 Características Principales del Sistema

### 1. Topología Multinodo Descentralizada (P2P)
*   **Múltiples Servidores**: Consiste en 3 nodos servidores independientes ejecutándose en procesos/JVMs separados ([NodoServidor](app/src/main/java/org/example/NodoServidor.java)).
*   **Dualidad de Sockets**: Cada nodo mantiene un `ServerSocket` para atender a sus clientes de chat locales (puertos `5001`, `5002` y `5003`) y un socket de interconexión para participar en el anillo P2P del clúster (puertos `6001`, `6002` y `6003`).
*   **Membresía Dinámica**: A través de [MembresiaCluster.java](app/src/main/java/org/example/distributed/MembresiaCluster.java), cada nodo registra el estado conocido de sus peers (`ACTIVO`, `CAIDO` o `DESCONOCIDO`).

### 2. Ordenamiento de Eventos y Consistencia (Relojes de Lamport)
*   **Ausencia de Reloj Físico Global**: La ordenación lógica de los mensajes de chat y eventos del sistema se resuelve mediante **Relojes Lógicos de Lamport** ([RelojLamport.java](app/src/main/java/org/example/distributed/RelojLamport.java)), implementados con un enfoque de concurrencia optimista sin bloqueo (`AtomicInteger` y operaciones CAS).
*   **Regla de Actualización Lógica**: Cada acción local ejecuta un `tick()`. Al recibir un mensaje, el nodo actualiza su reloj local aplicando: 
    $$L_{local} = \max(L_{local}, L_{mensaje}) + 1$$
*   **Bitácora Cronológica de Eventos**: Cada nodo escribe sus trazas lógicas de forma atómica en un archivo individual ([EventLog.java](app/src/main/java/org/example/distributed/EventLog.java)) bajo la ruta `logs/nodo_X.log` para permitir la auditoría de eventos.

### 3. Exclusión Mutua Distribuida y Consistencia de Estado
*   **Token Ring con Épocas (Term Epochs)**: El acceso exclusivo al recurso crítico ([RecursoCritico.java](app/src/main/java/org/example/distributed/RecursoCritico.java)) se coordina haciendo circular un token lógico en anillo (`1 ➔ 2 ➔ 3 ➔ 1`) gestionado por [TokenRing.java](app/src/main/java/org/example/distributed/TokenRing.java).
    *   *Resolución de Duplicados*: Para evitar la presencia de tokens duplicados tras la sanación de particiones de red o elecciones recurrentes, se introdujo una variable `epoch` (época del coordinador). Todo token lleva estampada la época actual; si un token de una época inferior (`token.epoch < local.epoch`) llega a un nodo, se descarta y destruye automáticamente de forma segura.
*   **Sincronización en Caliente (State Transfer)**: Cuando un servidor que estuvo caído se levanta de nuevo o se reincorpora al clúster, envía un mensaje `SYNC_REQUEST` a un peer activo. El peer responde con un `SYNC_RESPONSE` que transfiere la bitácora serializada del diccionario de comandos aprendidos de [RecursoCritico.java](app/src/main/java/org/example/distributed/RecursoCritico.java), logrando consistencia eventual instantánea.
*   **Sintaxis Global de Aprendizaje**: Si un usuario intenta usar la función `/aprender` con argumentos incorrectos (ej. `/aprender hola`), el servidor detecta el error sintáctico y difunde un mensaje de advertencia (`WARNING`) a todos los clientes del chat global, en lugar de responder de forma privada únicamente al autor del comando.

### 4. Tolerancia a Fallos y Elección de Líder
*   **Monitoreo por Heartbeats**: Cada servidor corre un hilo secundario coordinado por [HeartbeatManager.java](app/src/main/java/org/example/distributed/HeartbeatManager.java) que envía latidos periódicos al coordinador activo. Si expira el tiempo de espera, se declara su caída.
*   **Algoritmo Bully (Abusón)**: Ante la caída confirmada del líder, el nodo iniciador convoca un proceso de elección a través de [AlgoritmoBully.java](app/src/main/java/org/example/distributed/AlgoritmoBully.java). El nodo activo de mayor ID es elegido, incrementa la época del coordinador (`epoch`) y difunde el nuevo liderazgo.
*   **Apagado Administrativo Remoto**: Los servidores soportan un tipo de mensaje administrativo `SHUTDOWN` firmado por un token compartido ([Config.java](app/src/main/java/org/example/Config.java)). Esto permite forzar la caída controlada de un servidor para evaluar la resiliencia del sistema.

### 5. Balanceador de Carga del Cliente (Client-Side Load Balancing)
*   **Asignación de Puerto Aleatoria**: Si un cliente ([Cliente.java](app/src/main/java/org/example/Cliente.java)) o un bot ([NodoBot.java](app/src/main/java/org/example/NodoBot.java)) se inicia sin parámetros de puerto específicos, su balanceador interno escoge un puerto aleatorio de cliente activo del clúster (5001, 5002 o 5003).
*   **Reconexión Circular**: Si el servidor al que está conectado un cliente se cae, el cliente intercepta el error e intenta reconectarse de manera transparente a los servidores restantes en orden circular, garantizando que el usuario no pierda el servicio de chat.

---

## 📁 Estructura del Código

El proyecto está estructurado dentro del módulo de Gradle `app`:

*   [Config.java](app/src/main/java/org/example/Config.java): Almacena puertos de red, parámetros de timeouts de latidos, límite de historial de chat, tokens de seguridad de shutdown y constantes del clúster.
*   [PaqueteMensaje.java](app/src/main/java/org/example/PaqueteMensaje.java): Estructura de datos serializable utilizada para todo tipo de comunicación (mensajes del chat, elecciones, tokens, sincronización de estado y apagado). Contiene la época (`epoch`) de control.
*   [NodoServidor.java](app/src/main/java/org/example/NodoServidor.java): Orquestador y motor principal del nodo de mensajería distribuida. Maneja las conexiones P2P, los clientes locales, el procesamiento de comandos del chat (`/usuarios`, `/historial`, `/aprender`), la detección de errores de sintaxis y los mensajes `SHUTDOWN` y `SYNC`.
*   [Cliente.java](app/src/main/java/org/example/Cliente.java): Interfaz CLI de chat interactivo que implementa balanceo de carga automático y reconexión circular tolerante a fallos.
*   [NodoBot.java](app/src/main/java/org/example/NodoBot.java): Agente de chat inteligente autónomo. Automatiza la consulta de definiciones agregadas en el recurso crítico mediante `/aprender` y cuenta con balanceador y reconexión circular.
*   [ClusterLauncher.java](app/src/main/java/org/example/ClusterLauncher.java): Utilidad para iniciar automáticamente Nodos 1, 2 y 3 en hilos en segundo plano, redireccionando su output a archivos de log independientes. Soporta apagado masivo ordenado con `Ctrl+C`.
*   [ClusterControl.java](app/src/main/java/org/example/ClusterControl.java): Utilidad de apagado controlado que envía una señal TCP `SHUTDOWN` autenticada a un nodo específico.
*   **Paquete `distributed`** (Lógica de Sistemas Distribuidos):
    *   [IRelojLamport.java](app/src/main/java/org/example/distributed/IRelojLamport.java) & [RelojLamport.java](app/src/main/java/org/example/distributed/RelojLamport.java): Implementación del reloj lógico.
    *   [IEventLog.java](app/src/main/java/org/example/distributed/IEventLog.java) & [EventLog.java](app/src/main/java/org/example/distributed/EventLog.java): Persistencia atómica de eventos ordenados por Lamport.
    *   [ConexionPeer.java](app/src/main/java/org/example/distributed/ConexionPeer.java): Abstracción de un canal de comunicación de sockets bidireccional entre servidores vecinos en la malla P2P.
    *   [MembresiaCluster.java](app/src/main/java/org/example/distributed/MembresiaCluster.java): Registro local del estado y conectividad de los nodos del clúster.
    *   [HeartbeatManager.java](app/src/main/java/org/example/distributed/HeartbeatManager.java): Responsable de enviar señales de vida periódicas al coordinador actual y de alertar si este se desconecta.
    *   [AlgoritmoBully.java](app/src/main/java/org/example/distributed/AlgoritmoBully.java): Orquesta la elección de un nuevo líder tras la caída del coordinador utilizando el algoritmo Bully.
    *   [TokenRing.java](app/src/main/java/org/example/distributed/TokenRing.java): Define el flujo circular del token de exclusión mutua en la red y las reglas de validación por épocas.
    *   [RecursoCritico.java](app/src/main/java/org/example/distributed/RecursoCritico.java): Diccionario en memoria con soporte de serialización y deserialización para la sincronización de estado entre nodos.
    *   [TestRelojLamport.java](app/src/main/java/org/example/distributed/TestRelojLamport.java): Tests unitarios de verificación para el incremento causal de marcas de tiempo de Lamport.
*   **Paquete `loadtest`** (Mecanismos de Pruebas de Carga):
    *   [ClienteCarga.java](app/src/main/java/org/example/loadtest/ClienteCarga.java): Hilo cliente de simulación de estrés que registra tiempos de respuesta, errores y reconexiones.
    *   [RecolectorMetricas.java](app/src/main/java/org/example/loadtest/RecolectorMetricas.java): Consolida los reportes del test y calcula las conexiones activas al finalizar el experimento.
    *   [GeneradorCarga.java](app/src/main/java/org/example/loadtest/GeneradorCarga.java): Orquestador principal de la simulación. Lanza 50 clientes paralelos, induce la caída de Nodo 3 a los 30 segundos usando el mensaje administrativo `SHUTDOWN`, evalúa la reconexión y genera reportes detallados.

---

## 🛠️ Instrucciones de Compilación y Ejecución

### 1. Compilación del Código
Asegúrate de estar en el directorio raíz de la carpeta `Proyecto_Paralela` y ejecuta el comando de Gradle para compilar el proyecto:
```bash
./gradlew build -x test
```

---

### 2. Levantar el Clúster de Servidores
Tienes dos opciones para levantar el clúster de servidores (Nodos 1, 2 y 3):

#### Opción A: Levantamiento Automático (Recomendado)
Puedes iniciar todos los servidores de manera coordinada en una sola terminal ejecutando el lanzador automático:
```bash
java -cp app/build/classes/java/main org.example.ClusterLauncher
```
*   **Cómo funciona**: Levanta en segundo plano los 3 procesos Java de `NodoServidor` correspondientes a los IDs 1, 2 y 3.
*   **Logs**: La salida estándar y de error de cada nodo se redirige automáticamente a `logs/nodo_1.log`, `logs/nodo_2.log` y `logs/nodo_3.log`.
*   **Finalización**: Presionando `Ctrl+C` en esta terminal detendrás de manera limpia y segura todos los servidores del clúster.

#### Opción B: Levantamiento Manual (Tres terminales por separado)
Si deseas observar la salida de consola de cada servidor en tiempo real, puedes abrir tres terminales diferentes y ejecutar:
*   **Terminal del Nodo 1**:
    ```bash
    java -cp app/build/classes/java/main org.example.NodoServidor 1
    ```
*   **Terminal del Nodo 2**:
    ```bash
    java -cp app/build/classes/java/main org.example.NodoServidor 2
    ```
*   **Terminal del Nodo 3**:
    ```bash
    java -cp app/build/classes/java/main org.example.NodoServidor 3
    ```

---

### 3. Conectar Clientes Interactivos de Chat
Para chatear en la red interactiva de mensajería distribuida, abre una o más terminales y ejecuta:
```bash
java -cp app/build/classes/java/main org.example.Cliente
```
*   **Balanceo de Carga**: El cliente seleccionará un servidor activo aleatoriamente (puertos `5001`, `5002` o `5003`).
*   **Conexión Manual**: Si deseas conectarte específicamente a un puerto determinado, pásalo como parámetro:
    ```bash
    java -cp app/build/classes/java/main org.example.Cliente localhost 5001
    ```
*   **Comandos en el Chat**:
    *   `/usuarios` : Muestra los usuarios autenticados en el nodo local actual.
    *   `/historial` : Solicita la bitácora de mensajes de chat recientes guardados en memoria.
    *   `/aprender <termino> <definicion>` : Guarda un par clave-valor en el recurso crítico distribuido requiriendo la adquisición del Token Ring.
    *   Cualquier otra cadena enviada se difundirá como mensaje de chat convencional a todos los nodos.

---

### 4. Iniciar el Bot Autónomo
Puedes iniciar el agente inteligente autónomo para comprobar el procesamiento automatizado del clúster:
```bash
java -cp app/build/classes/java/main org.example.NodoBot
```
*   Igual que el cliente, el bot cuenta con balanceador de carga automático e intentará conectarse a cualquiera de los nodos disponibles y cambiará de nodo en caliente si este es dado de baja.

---

## 📊 Pruebas de Carga y Resiliencia Automatizadas

El proyecto incluye un arnés de simulación de estrés diseñado para evaluar de forma autónoma el rendimiento y la tolerancia a fallos del clúster bajo condiciones intensivas:

1.  **Iniciar servidores**: Levanta el clúster con la Opción A (`ClusterLauncher`).
2.  **Iniciar la Prueba**: En otra terminal, ejecuta:
    ```bash
    java -cp app/build/classes/java/main org.example.loadtest.GeneradorCarga
    ```
3.  **Ciclo de Vida de la Simulación (60 segundos)**:
    *   El generador crea **50 clientes de carga concurrentes** ([ClienteCarga.java](app/src/main/java/org/example/loadtest/ClienteCarga.java)).
    *   Estos clientes bombardean con mensajes y consultas a los tres servidores activos distribuyendo la carga de manera aleatoria.
    *   A los **30 segundos de ejecución**, el generador envía automáticamente un comando administrativo de `SHUTDOWN` al coordinador (Nodo 3).
    *   El clúster detecta la desconexión del coordinador, convoca elecciones de inmediato con el algoritmo Bully, asigna un nuevo coordinador, incrementa la época del token a `epoch = 4` y descarta los tokens antiguos.
    *   Los clientes que estaban conectados a Nodo 3 interceptan la falla del socket, buscan un nuevo nodo vivo (Nodo 1 o 2) y se reconectan de manera transparente.
    *   A los **55 segundos**, el generador toma un snapshot del estado de la red para contar cuántos clientes siguen conectados.
    *   A los **60 segundos**, detiene los hilos y genera un resumen de métricas en la consola y en el archivo `logs/reporte_carga.txt`.

El reporte final mostrará:
*   **Throughput (resp/s)** y **Latencia promedio (ms)**.
*   **Tiempo de Recuperación del Clúster (ms)** tras la caída del coordinador.
*   **Conexiones de Clientes Esperadas vs Reales** para validar que la reconexión distribuida haya funcionado al 100%.

---

## 🔧 Simulación y Pruebas de Falla Manuales

Si prefieres realizar una auditoría de fallas y recuperaciones de forma interactiva y paso a paso:

### 1. Provocar la caída de un Nodo Servidor
Usa la herramienta [ClusterControl.java](app/src/main/java/org/example/ClusterControl.java) indicando el identificador del nodo que quieres dar de baja (por ejemplo, el Nodo 3):
```bash
java -cp app/build/classes/java/main org.example.ClusterControl 3
```
*   Esto enviará el paquete `SHUTDOWN` autenticado al puerto de clientes de dicho nodo, provocando un cierre seguro de sockets y la finalización controlada de su hilo de ejecución.

### 2. Comprobar la resiliencia en Caliente
Observa cómo los clientes que estaban en la Terminal de chat conectados al Nodo 3 inician su algoritmo de reintentos circulares y se mudan automáticamente al Nodo 1 o Nodo 2, imprimiendo logs de reconexión.
Al mismo tiempo, los nodos restantes iniciarán elecciones Bully para designar el nuevo coordinador y reestablecer el paso del token.

### 3. Recuperar el Nodo Caído (Sincronización de Estado)
Vuelve a levantar el Nodo 3 en otra terminal:
```bash
java -cp app/build/classes/java/main org.example.NodoServidor 3
```
*   **Sincronización Automática**: Durante su inicio, Nodo 3 se conectará con los nodos activos, enviará un mensaje `SYNC_REQUEST` y recibirá un `SYNC_RESPONSE` con todas las definiciones del diccionario aprendidas mientras estuvo fuera de línea.
*   **Reingreso al Anillo**: El nodo recuperado es reinsertado dinámicamente al flujo circular del token de exclusión mutua.

---

## 📝 Formato de Logs del Clúster

Los logs en `logs/nodo_X.log` siguen la convención cronológica ordenada lógicamente por Lamport:

```text
[2026-06-09 13:30:10.150][L:12][NODO:3][SISTEMA] NodoServidor 3 iniciado en puerto clientes 5003
[2026-06-09 13:30:40.220][L:82][NODO:1][BULLY] Elección iniciada localmente
[2026-06-09 13:30:44.935][L:95][NODO:2][TOKEN] Token OBSOLETO descartado (token epoch: 0, local: 4)
[2026-06-09 13:30:44.936][L:96][NODO:2][TOKEN] Token inicial creado en Nodo 2 (coordinador, época: 4)
```

Las trazas indican:
*   `[2026-06-09 13:30:10.150]`: Marca de tiempo real del host.
*   `[L:12]`: Marca de tiempo lógica del **Reloj de Lamport** del nodo en ese instante de evento.
*   `[NODO:X]`: Identificador del servidor que registra el evento.
*   `[SISTEMA / BULLY / TOKEN / CONEXION]`: Etiqueta del módulo del sistema distribuido.
