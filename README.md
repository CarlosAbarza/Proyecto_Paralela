# Documentación Técnica: Arquitectura del Sistema Distribuido de Mensajería

## 1. Visión General de la Arquitectura

El sistema implementa una arquitectura distribuida basada en el modelo Cliente-Servidor puro mediante el uso de Sockets TCP. Está compuesto por procesos independientes que se comunican a través de una red para lograr un objetivo común: la mensajería asíncrona multipunto y la ejecución de comandos automatizados.

El ecosistema está conformado por tres componentes físicos (nodos) distintos:

1. **Servidor Central (`App.java`):** Actúa como el enrutador principal (broker) y gestor del estado global.
    
2. **Nodos Cliente (`Cliente.java`):** Interfaces de usuario para la interacción humana.
    
3. **Nodo Autónomo (`NodoBot.java`):** Un proceso independiente que actúa como motor concurrente de comandos.
    

## 2. Análisis de Componentes y Código Fuente

### 2.1. El Protocolo de Comunicación: `PaqueteMensaje.java`

Esta clase define el estándar de comunicación entre todos los nodos del sistema.

- **Marshalling y Serialización:** Implementa la interfaz `Serializable`, lo que permite a la Máquina Virtual de Java (JVM) transformar el estado complejo del objeto en una secuencia de bytes para su transmisión por la red y su posterior reconstrucción (unmarshalling) en el nodo receptor.
    
- **Ausencia de Reloj Global:** Al instanciarse, la clase captura la marca de tiempo local (`LocalDateTime.now()`). Esto es fundamental en sistemas distribuidos para proveer un ordenamiento lógico de los eventos, mitigando la falta de un reloj físico sincronizado entre máquinas distintas.
    
- **Enrutamiento Lógico:** Utiliza un `enum Tipo { TEXTO, COMANDO }` para que los nodos puedan discriminar rápidamente la naturaleza del paquete sin tener que analizar (parsear) su contenido textual.
    

### 2.2. El Núcleo de Enrutamiento: `App.java` (Servidor)

Este archivo contiene la lógica del nodo central. Es el responsable de la concurrencia máxima y de garantizar la resiliencia de la red.

- **Gestión de Concurrencia:** Delega cada conexión entrante (`servidor.accept()`) a un nuevo hilo de ejecución (`ManejadorCliente`). Esto permite que el servidor atienda a múltiples nodos simultáneamente sin bloquearse.
    
- **Protección de Regiones Críticas:** La lista de flujos de salida (`List<ObjectOutputStream> clientes`) es un recurso compartido. Toda operación de lectura o escritura sobre esta lista está estrictamente protegida por bloques `synchronized (clientes)` para prevenir condiciones de carrera durante la difusión de mensajes.
    
- **Sincronización de Estado (Historial):** Utiliza una `ConcurrentLinkedQueue` (estructura nativa _thread-safe_ de Java) para almacenar los últimos 15 mensajes. Cuando un nodo nuevo se conecta, el servidor le transfiere este estado inicial antes de agregarlo a la red activa, asegurando consistencia en el contexto.
    
- **Tolerancia a Fallos (Crash/Omisión):** Si un cliente se desconecta abruptamente (fallo de _Crash_), el hilo dedicado captura la `IOException`, reporta la pérdida de conexión del nodo específico y, mediante el bloque `finally`, elimina de forma segura el socket de la lista compartida. Esto evita la propagación del fallo y mantiene el servidor principal operativo.
    

### 2.3. Interfaz de Usuario: `Cliente.java`

Representa el nodo humano. Su diseño se basa en la separación de responsabilidades de Entrada/Salida (I/O).

- **Asincronía de I/O:** El hilo principal del programa (`main`) se dedica exclusivamente a escuchar pasivamente los objetos que llegan desde el servidor mediante `ObjectInputStream.readObject()`. Paralelamente, se levanta un `HiloEnvio` que bloquea la consola esperando el _input_ del usuario.
    
- **Transparencia de Acceso:** El cliente empaqueta su mensaje como `Tipo.TEXTO` o `Tipo.COMANDO` basándose en el prefijo (ej. `/`, `!`). El usuario no necesita saber si un comando será procesado por el servidor o por un bot externo; simplemente lo envía a la red.
    
- **Gestión de Consola:** Utiliza secuencias de escape ANSI (`\033[1A\033[2K`) para limpiar la entrada estándar y evitar la duplicidad visual de los mensajes en terminales físicas.
    

### 2.4. Motor de Procesamiento: `NodoBot.java`

Este archivo representa la segunda función de gran escala del sistema. Es un nodo independiente que demuestra aislamiento de fallos y concurrencia avanzada.

- **Aislamiento de Fallos:** Al ejecutarse como un proceso independiente, cualquier error crítico de lógica dentro del bot provocará únicamente el cierre de su propio proceso. El `App.java` registrará su desconexión, pero el resto de los clientes humanos no sufrirán ninguna interrupción.
    
- **Pool de Hilos (ExecutorService):** A diferencia de un bot tradicional de un solo hilo, este nodo utiliza `Executors.newFixedThreadPool(10)`. Cuando intercepta un comando válido, no detiene su bucle de escucha; delega el procesamiento a uno de los hilos del _pool_. Esto le permite procesar múltiples comandos pesados (como `/pesado`) en paralelo sin generar cuellos de botella.
    
- **Memoria Dinámica Concurrente:** Implementa un sistema de aprendizaje en tiempo real mediante `ConcurrentHashMap`. Esta estructura permite que múltiples hilos del bot lean y escriban nuevos comandos de forma simultánea y segura, bloqueando únicamente los segmentos de memoria estrictamente necesarios en lugar de toda la tabla de datos.
    
