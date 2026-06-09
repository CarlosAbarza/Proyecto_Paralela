package org.example;

import org.example.distributed.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Nodo servidor distribuido.
 * Reemplaza a App.java para el Proyecto Final.
 *
 * Cada instancia es un nodo del cluster con:
 * - Un ServerSocket para clientes (puerto 500X)
 * - Un ServerSocket para peers (puerto 600X)
 * - Un RelojLamport propio
 * - Un EventLog propio
 * - Conexiones peer-to-peer con los otros servidores
 *
 * Uso: java NodoServidor <nodoId>
 *   donde nodoId = 1, 2 o 3
 */
public class NodoServidor {

    private final int nodoId;
    private final int puertoClientes;
    private final int puertoPeers;
    private final String host;

    // Componentes distribuidos
    private final RelojLamport reloj;
    private final EventLog log;
    private final MembresiaCluster membresia;

    // Conexiones de clientes locales
    private final List<ClienteInfo> clientesLocales = Collections.synchronizedList(new ArrayList<>());

    // Conexiones peer-to-peer con otros servidores
    private final Map<Integer, ConexionPeer> peers = new ConcurrentHashMap<>();

    // Historial de mensajes (protegido por synchronized en actualizarHistorial)
    private final Queue<PaqueteMensaje> historial = new LinkedList<>();

    // Módulos de coordinación (Fases 3, 4, 5)
    private AlgoritmoBully algoritmoBully;
    private HeartbeatManager heartbeatManager;
    private TokenRing tokenRing;
    private RecursoCritico recursoCritico;

    // ID del coordinador actual (Fases 3 y 5)
    private volatile int coordinadorId;
    private final AtomicInteger epochCoordinador = new AtomicInteger(1);

    // Callback para mensajes peer de coordinación (Fases 3, 4, 5)
    private volatile Consumer<PaqueteMensaje> onMensajePeer;

    // --- Inner class para info de clientes conectados ---
    static class ClienteInfo {
        final ObjectOutputStream out;
        final String nombre;
        ClienteInfo(ObjectOutputStream out, String nombre) {
            this.out = out;
            this.nombre = nombre;
        }
    }

    // ==================== CONSTRUCTOR ====================

    public NodoServidor(int nodoId) {
        this.nodoId = nodoId;
        this.host = Config.HOST_DEFAULT;
        this.puertoClientes = Config.getPuertoClientes(nodoId);
        this.puertoPeers = Config.getPuertoPeers(nodoId);
        this.reloj = new RelojLamport();
        this.log = new EventLog(nodoId);
        this.membresia = new MembresiaCluster(nodoId);
        this.coordinadorId = Config.NUM_NODOS; // El de mayor ID es coordinador inicial
    }

    // ==================== INICIO ====================

    public void iniciar() {
        log.registrar("INICIO", "Nodo servidor " + nodoId + " iniciando...", reloj.tick());

        // --- Paso 1: Inicializar módulos de coordinación ANTES de aceptar conexiones ---
        // (evita que mensajes peer lleguen antes de tener el dispatcher registrado)
        algoritmoBully = new AlgoritmoBully(this);
        heartbeatManager = new HeartbeatManager(this);
        tokenRing = new TokenRing(this);
        recursoCritico = new RecursoCritico();

        // --- Paso 2: Registrar el dispatcher de mensajes peer ---
        setOnMensajePeer(this::dispatchMensajePeer);

        // --- Paso 3: Conectar callbacks ---
        heartbeatManager.setOnCoordinadorCaido(() -> {
            algoritmoBully.iniciarEleccion();
        });

        // --- Paso 4: Aceptar conexiones (ahora el dispatcher ya está listo) ---
        new Thread(this::aceptarClientes, "AceptarClientes-" + nodoId).start();
        new Thread(this::aceptarPeers, "AceptarPeers-" + nodoId).start();
        new Thread(this::conectarAPeers, "ConectarPeers-" + nodoId).start();

        System.out.println("[NODO " + nodoId + "] Servidor iniciado.");
        System.out.println("[NODO " + nodoId + "] Puerto clientes: " + puertoClientes);
        System.out.println("[NODO " + nodoId + "] Puerto peers: " + puertoPeers);

        // --- Paso 5: Esperar a que las conexiones peer se establezcan ---
        try { Thread.sleep(5000); } catch (InterruptedException e) { /* ok */ }

        // --- Paso 6: Iniciar módulos (después de que los peers se conectaron) ---
        heartbeatManager.iniciar();
        tokenRing.iniciar();

        log.registrar("INICIO",
            "Todos los módulos iniciados. Coordinador inicial: Nodo " + coordinadorId,
            reloj.tick());
        System.out.println("[NODO " + nodoId + "] Coordinador: Nodo " + coordinadorId);
        System.out.println("[NODO " + nodoId + "] Todos los módulos activos. Listo.");
        
        // Iniciar elección al arrancar para descubrir/reclamar el rol de coordinador
        algoritmoBully.iniciarEleccion();
    }

    // ==================== CLIENTES ====================

    private void aceptarClientes() {
        try (ServerSocket serverSocket = new ServerSocket(puertoClientes)) {
            log.registrar("INICIO", "Escuchando clientes en puerto " + puertoClientes, reloj.tick());

            /*
             * CICLO DE ACEPTACIÓN DE CLIENTES:
             * - Qué hace: Escucha de forma continua conexiones entrantes de clientes (usuarios del chat) en el puerto de clientes local.
             * - Con quién se comunica: Con los clientes de chat (instancias de Cliente) que se conectan a este nodo.
             * - De qué depende: Depende de la disponibilidad del puerto del sistema y de que el hilo principal inicie este servicio.
             * - Manejo de errores: Si falla el socket al aceptar una conexión específica, se captura la excepción. Si hay un
             *   fallo catastrófico en el ServerSocket externo, finaliza la aceptación de nuevos clientes pero no tumba al servidor completo.
             */
            while (true) {
                Socket socket = serverSocket.accept();
                log.registrar("CONEXION", "Nueva conexión de cliente desde " + socket.getInetAddress(), reloj.tick());

                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();

                Thread hilo = new Thread(new ManejadorCliente(socket, out), "Cliente-" + socket.getPort());
                hilo.start();
            }
        } catch (IOException e) {
            log.registrar("ERROR", "Error en ServerSocket de clientes: " + e.getMessage(), reloj.tick());
        }
    }

    // --- ManejadorCliente: hilo por cada cliente conectado ---
    class ManejadorCliente implements Runnable {
        private final Socket socket;
        private final ObjectOutputStream out;
        private String nombreAsignado = "Desconocido";

        public ManejadorCliente(Socket socket, ObjectOutputStream out) {
            this.socket = socket;
            this.out = out;
        }

        @Override
        public void run() {
            try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

                if (!autenticar(in)) return;

                /*
                 * CICLO DE LECTURA DE MENSAJES DEL CLIENTE:
                 * - Qué hace: Lee de forma contínua y bloqueante paquetes serializados enviados por el cliente.
                 * - Con quién se comunica: Con el cliente TCP remoto asignado a este manejador.
                 * - De qué depende: De la validez de la sesión autenticada y de la estabilidad de la conexión TCP.
                 * - Manejo de errores/Reconexión: Captura IOException o ClassNotFoundException si el cliente se desconecta
                 *   o envía datos corruptos, finalizando el ciclo. En el bloque 'finally' se liberan los recursos y se
                 *   limpia al cliente de la lista activa del nodo. La reconexión es responsabilidad del Cliente.
                 */
                while (true) {
                    Object recibido = in.readObject();
                    if (!(recibido instanceof PaqueteMensaje)) {
                        enviarDirecto(new PaqueteMensaje("Servidor", "Paquete inválido.", PaqueteMensaje.Tipo.SISTEMA));
                        continue;
                    }

                    PaqueteMensaje mensajeRecibido = (PaqueteMensaje) recibido;
                    procesarMensaje(mensajeRecibido);
                }

            } catch (IOException | ClassNotFoundException e) {
                log.registrar("DESCONEXION", "Conexión perdida con [" + nombreAsignado + "]", reloj.tick());
            } finally {
                limpiarRecursos();
            }
        }

        private boolean autenticar(ObjectInputStream in) throws IOException, ClassNotFoundException {
            // Paso 1: AUTH
            Object authObj = in.readObject();
            if (!(authObj instanceof PaqueteMensaje)) return false;

            PaqueteMensaje auth = (PaqueteMensaje) authObj;
            if (auth.getTipo() != PaqueteMensaje.Tipo.AUTH ||
                    !Config.TOKEN_VALIDO.equals(auth.getContenido())) {
                log.registrar("AUTH", "Token inválido desde " + socket.getInetAddress(), reloj.tick());
                enviarDirecto(new PaqueteMensaje("Servidor", "Token inválido.", PaqueteMensaje.Tipo.SISTEMA));
                return false;
            }

            // Paso 2: LOGIN
            Object loginObj = in.readObject();
            if (!(loginObj instanceof PaqueteMensaje)) return false;

            PaqueteMensaje login = (PaqueteMensaje) loginObj;
            String nombre = login.getRemitente();

            if (login.getTipo() != PaqueteMensaje.Tipo.LOGIN || !nombreValido(nombre)) {
                enviarDirecto(new PaqueteMensaje("Servidor", "Nombre inválido.", PaqueteMensaje.Tipo.SISTEMA));
                return false;
            }

            /*
             * AUDITORÍA / DETECCIÓN DE COMPORTAMIENTO:
             * - No se realiza validación de unicidad de nombre de usuario (username uniqueness)
             *   ni a nivel local del nodo ni a nivel global de clúster.
             * - Si un usuario inicia sesión con un nombre idéntico al de otro usuario conectado,
             *   ambas conexiones coexistirán de manera silenciosa. Esto puede provocar ambigüedades en la
             *   identificación de remitentes e inconsistencias en la notificación de salida.
             * - MEJORA: Validar localmente contra la lista 'clientesLocales' y replicar la consulta en el clúster.
             */

            nombreAsignado = nombre;
            log.registrar("AUTH", "Usuario autenticado: " + nombreAsignado, reloj.tick());

            enviarDirecto(new PaqueteMensaje("Servidor",
                    "Bienvenido, " + nombreAsignado + ". Conectado a Nodo " + nodoId + ".",
                    PaqueteMensaje.Tipo.SISTEMA));

            // IMPORTANTE: Registrar en la lista ANTES de enviar historial
            // para evitar ventana de race donde broadcasts se pierden.
            clientesLocales.add(new ClienteInfo(out, nombreAsignado));

            enviarHistorial();

            PaqueteMensaje anuncio = new PaqueteMensaje("Servidor",
                    nombreAsignado + " se ha unido al chat (Nodo " + nodoId + ").",
                    PaqueteMensaje.Tipo.SISTEMA);
            int marca = reloj.tick();
            anuncio.setLamportTimestamp(marca);
            anuncio.setNodoOrigenId(nodoId);

            difundirLocal(anuncio);
            replicarAPeers(anuncio);

            return true;
        }

        private void procesarMensaje(PaqueteMensaje mensajeRecibido) {
            // Manejar apagado administrativo remoto
            if (mensajeRecibido.getTipo() == PaqueteMensaje.Tipo.SHUTDOWN) {
                if (Config.TOKEN_VALIDO.equals(mensajeRecibido.getContenido())) {
                    log.registrar("SISTEMA", "Recibida señal de APAGADO administrativo local. Apagando...", reloj.tick());
                    log.flush();
                    System.out.println("[NODO " + nodoId + "] Recibida señal de APAGADO administrativo. Deteniendo...");
                    System.exit(0);
                }
            }

            String contenido = mensajeRecibido.getContenido();

            if (contenido == null || contenido.trim().isEmpty()) {
                enviarDirecto(new PaqueteMensaje("Servidor", "No se permiten mensajes vacíos.", PaqueteMensaje.Tipo.SISTEMA));
                return;
            }

            if (contenido.length() > Config.MAX_MENSAJE) {
                enviarDirecto(new PaqueteMensaje("Servidor",
                        "Mensaje excede " + Config.MAX_MENSAJE + " caracteres.", PaqueteMensaje.Tipo.SISTEMA));
                return;
            }

            // Comandos locales del servidor
            if (contenido.equalsIgnoreCase("/usuarios")) {
                enviarListaUsuarios();
                return;
            }
            if (contenido.equalsIgnoreCase("/historial")) {
                enviarHistorial();
                return;
            }

            // Comando /aprender → requiere exclusión mutua (token ring)
            if (contenido.startsWith("/aprender") && tokenRing != null) {
                String[] partes = contenido.split(" ", 3);
                if (contenido.startsWith("/aprender ") && partes.length >= 3) {
                    String nuevoCmd = partes[1].toLowerCase();
                    String respuestaCmd = partes[2];

                    boolean exito = tokenRing.ejecutarEnSeccionCritica(() -> {
                        recursoCritico.escribirComando(nuevoCmd, respuestaCmd);
                        log.registrar("SECCION_CRITICA",
                            "Comando '" + nuevoCmd + "' aprendido en sección crítica",
                            reloj.tick());
                        
                        // Replicar comando a todos los peers
                        PaqueteMensaje replica = new PaqueteMensaje(
                            "Sistema",
                            nuevoCmd + "|" + respuestaCmd,
                            PaqueteMensaje.Tipo.REPLICA
                        );
                        enviarATodosPeers(replica);
                    }, Config.TOKEN_TIMEOUT_MS);

                    if (exito) {
                        PaqueteMensaje resp = new PaqueteMensaje("Servidor",
                            "Comando '" + nuevoCmd + "' aprendido exitosamente.",
                            PaqueteMensaje.Tipo.SISTEMA);
                        resp.setLamportTimestamp(reloj.tick());
                        resp.setNodoOrigenId(nodoId);
                        difundirLocal(resp);
                        replicarAPeers(resp);
                    } else {
                        enviarDirecto(new PaqueteMensaje("Servidor",
                            "Error: no se pudo obtener acceso exclusivo para aprender.",
                            PaqueteMensaje.Tipo.SISTEMA));
                    }
                    return;
                } else {
                    // Difundir error global de sintaxis (según feedback L54 del plan de implementación)
                    PaqueteMensaje errGlobal = new PaqueteMensaje("Servidor",
                        "Uso incorrecto del comando de aprendizaje. Formato: /aprender /comando respuesta",
                        PaqueteMensaje.Tipo.SISTEMA);
                    errGlobal.setLamportTimestamp(reloj.tick());
                    errGlobal.setNodoOrigenId(nodoId);
                    difundirLocal(errGlobal);
                    replicarAPeers(errGlobal);
                    return;
                }
            }

            // Si es un comando aprendido, buscarlo en el recurso crítico
            if (contenido.startsWith("/") && recursoCritico != null) {
                String cmd = contenido.trim().split(" ")[0].toLowerCase();
                String respuestaAprendida = recursoCritico.leerComando(cmd);
                if (respuestaAprendida != null) {
                    PaqueteMensaje resp = new PaqueteMensaje("Bot",
                        respuestaAprendida, PaqueteMensaje.Tipo.TEXTO);
                    resp.setLamportTimestamp(reloj.tick());
                    resp.setNodoOrigenId(nodoId);
                    difundirLocal(resp);
                    replicarAPeers(resp);
                    return;
                }
            }

            // Mensaje normal o comando: asignar marca Lamport y difundir
            PaqueteMensaje mensajeSeguro = new PaqueteMensaje(
                    nombreAsignado, contenido, mensajeRecibido.getTipo());

            int marca = reloj.tick();
            mensajeSeguro.setLamportTimestamp(marca);
            mensajeSeguro.setNodoOrigenId(nodoId);

            log.registrar("MENSAJE", "[" + mensajeRecibido.getTipo() + "] " + nombreAsignado + ": " + contenido, marca);

            actualizarHistorial(mensajeSeguro);
            difundirLocal(mensajeSeguro);
            replicarAPeers(mensajeSeguro);  // <-- CLAVE: replicar a los otros servidores
        }

        private boolean nombreValido(String nombre) {
            return nombre != null && nombre.matches("[a-zA-Z0-9_]{3,20}");
        }

        private void enviarDirecto(PaqueteMensaje mensaje) {
            try {
                synchronized (out) {
                    out.writeObject(mensaje);
                    out.flush();
                }
            } catch (IOException e) {
                log.registrar("ERROR", "No se pudo enviar a " + nombreAsignado, reloj.getValor());
            }
        }

        /*
         * AUDITORÍA / DETECCIÓN DE COMPORTAMIENTO:
         * - La lista de usuarios devuelta es LOCAL a este servidor (NodoServidor).
         * - Los usuarios conectados a otros nodos del clúster no se listan aquí.
         * - MEJORA: Para un sistema totalmente distribuido, se debería implementar una consulta P2P
         *   o mantener un estado global compartido de membresía de usuarios conectados en todo el clúster.
         */
        private void enviarListaUsuarios() {
            List<String> nombres = new ArrayList<>();
            for (ClienteInfo info : clientesLocales) {
                if (info.nombre != null) nombres.add(info.nombre);
            }
            enviarDirecto(new PaqueteMensaje("Servidor",
                    "Usuarios en Nodo " + nodoId + ": " + nombres, PaqueteMensaje.Tipo.SISTEMA));
        }

        /*
         * AUDITORÍA / DETECCIÓN DE COMPORTAMIENTO:
         * - El historial devuelto es LOCAL a la cola 'historial' de esta instancia de servidor.
         * - Aunque los mensajes se replican activamente entre nodos, un nodo recién ingresado o recuperado
         *   no tendrá los mensajes antiguos dado que no se realiza sincronización del historial al iniciar.
         * - MEJORA: Solicitar el historial reciente a un peer activo durante la inicialización.
         */
        private void enviarHistorial() {
            List<PaqueteMensaje> copia = new ArrayList<>(historial);
            if (copia.isEmpty()) {
                enviarDirecto(new PaqueteMensaje("Servidor", "Historial vacío.", PaqueteMensaje.Tipo.SISTEMA));
                return;
            }
            enviarDirecto(new PaqueteMensaje("Servidor", "=== HISTORIAL RECIENTE ===", PaqueteMensaje.Tipo.SISTEMA));
            for (PaqueteMensaje m : copia) {
                enviarDirecto(m);
            }
        }

        private void limpiarRecursos() {
            clientesLocales.removeIf(info -> info.out == this.out);
            try { socket.close(); } catch (IOException e) { /* ignorar */ }

            if (!"Desconocido".equals(nombreAsignado)) {
                PaqueteMensaje salida = new PaqueteMensaje("Servidor",
                        nombreAsignado + " salió del chat.", PaqueteMensaje.Tipo.SISTEMA);
                salida.setLamportTimestamp(reloj.tick());
                salida.setNodoOrigenId(nodoId);
                difundirLocal(salida);
                replicarAPeers(salida);
            }
        }
    }

    // ==================== PEERS ====================

    /**
     * Acepta conexiones entrantes de peers con ID mayor.
     */
    private void aceptarPeers() {
        try (ServerSocket serverSocket = new ServerSocket(puertoPeers)) {
            log.registrar("INICIO", "Escuchando peers en puerto " + puertoPeers, reloj.tick());

            /*
             * CICLO DE ACEPTACIÓN DE CONEXIONES PEER:
             * - Qué hace: Escucha y acepta continuamente solicitudes de conexión TCP de otros nodos servidores (peers) del cluster.
             * - Con quién se comunica: Con otros nodos de mayor ID que inician una conexión TCP hacia nosotros.
             * - De qué depende: Del ServerSocket en 'puertoPeers' y de un handshake inicial correcto de identificación.
             * - Manejo de errores: Si ocurre un fallo durante el handshake o accept, se registra el error y la conexión
             *   se aborta de forma segura sin interrumpir la escucha de futuras conexiones peer.
             */
            while (true) {
                Socket socket = serverSocket.accept();
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                // El peer envía un MEMBERSHIP_UPDATE con su nodoId como contenido
                PaqueteMensaje identificacion = (PaqueteMensaje) in.readObject();
                int peerNodoId = Integer.parseInt(identificacion.getContenido());

                // Enviar nuestra identificación de vuelta
                PaqueteMensaje miId = new PaqueteMensaje("Nodo" + nodoId,
                        String.valueOf(nodoId), PaqueteMensaje.Tipo.MEMBERSHIP_UPDATE);
                miId.setLamportTimestamp(reloj.tick());
                miId.setNodoOrigenId(nodoId);
                out.writeObject(miId);
                out.flush();

                // Crear ConexionPeer REUTILIZANDO los streams del handshake
                // (evita crear un segundo header de ObjectOutputStream → StreamCorruptedException)
                ConexionPeer conexion = new ConexionPeer(peerNodoId, socket, out, in);
                peers.put(peerNodoId, conexion);
                membresia.marcarActivo(peerNodoId);

                log.registrar("PEER", "Conexión peer aceptada de Nodo " + peerNodoId, reloj.tick());

                // Solicitar sincronización de estado (comandos aprendidos) al nuevo peer
                PaqueteMensaje syncReq = new PaqueteMensaje(
                    "Nodo" + nodoId,
                    "",
                    PaqueteMensaje.Tipo.SYNC_REQUEST
                );
                enviarAPeer(peerNodoId, syncReq);

                // Iniciar hilo listener para este peer
                new Thread(() -> escucharPeer(peerNodoId, conexion), "PeerListener-" + peerNodoId).start();
            }
        } catch (IOException | ClassNotFoundException e) {
            log.registrar("ERROR", "Error en ServerSocket de peers: " + e.getMessage(), reloj.tick());
        }
    }

    /**
     * Conecta a peers con ID menor que el nuestro.
     * Reintenta cada 3 segundos si falla.
     */
    private void conectarAPeers() {
        for (int targetId = 1; targetId < nodoId; targetId++) {
            final int peerId = targetId;
            new Thread(() -> conectarAPeerConReintentos(peerId), "ConectarPeer-" + peerId).start();
        }
    }

    private void conectarAPeerConReintentos(int peerId) {
        int peerPuerto = Config.getPuertoPeers(peerId);

        /*
         * CICLO DE REINTENTO DE CONEXIÓN CON PEER:
         * - Qué hace: Intenta continuamente establecer conexión saliente con un peer de ID menor.
         * - Con quién se comunica: Con el nodo remoto de ID 'peerId'.
         * - De qué depende: De la disponibilidad de red y de que el nodo destino esté activo y escuchando en 'peerPuerto'.
         * - Manejo de errores y reconexiones: Si falla la conexión o el handshake (IOException/ClassNotFoundException),
         *   se registra el error, se duerme el hilo por 3 segundos y se vuelve a intentar de forma indefinida.
         *   Si se conecta con éxito, llama a escucharPeer (bloqueante). Si escucharPeer finaliza por desconexión,
         *   el ciclo while continuará intentando reconectarse.
         */
        while (true) {
            try {
                Socket socket = new Socket(host, peerPuerto);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                // Enviar nuestra identificación
                PaqueteMensaje miId = new PaqueteMensaje("Nodo" + nodoId,
                        String.valueOf(nodoId), PaqueteMensaje.Tipo.MEMBERSHIP_UPDATE);
                miId.setLamportTimestamp(reloj.tick());
                miId.setNodoOrigenId(nodoId);
                out.writeObject(miId);
                out.flush();

                // Recibir identificación del peer
                PaqueteMensaje peerId_msg = (PaqueteMensaje) in.readObject();

                // Reutilizar streams del handshake (Critical: no crear nuevos sobre el mismo socket)
                ConexionPeer conexion = new ConexionPeer(peerId, socket, out, in);
                peers.put(peerId, conexion);
                membresia.marcarActivo(peerId);

                log.registrar("PEER", "Conectado a Nodo " + peerId + " (puerto " + peerPuerto + ")", reloj.tick());

                // Solicitar sincronización de estado al conectar
                PaqueteMensaje syncReq = new PaqueteMensaje(
                    "Nodo" + nodoId,
                    "",
                    PaqueteMensaje.Tipo.SYNC_REQUEST
                );
                enviarAPeer(peerId, syncReq);

                // Iniciar hilo listener
                escucharPeer(peerId, conexion);
                // Si la conexión se pierde en escucharPeer, el flujo vuelve aquí y se reintentará conectar.

            } catch (IOException | ClassNotFoundException e) {
                log.registrar("PEER", "No se pudo conectar a Nodo " + peerId + ", reintentando en 3s...", reloj.getValor());
                try { Thread.sleep(3000); } catch (InterruptedException ie) { break; }
            }
        }
    }

    /**
     * Loop de lectura de mensajes de un peer específico.
     */
    private void escucharPeer(int peerId, ConexionPeer conexion) {
        try {
            /*
             * CICLO DE LECTURA DE PEER:
             * - Qué hace: Lee continuamente los mensajes serializados provenientes de un peer específico.
             * - Con quién se comunica: Con el peer especificado por 'peerId'.
             * - De qué depende: De la estabilidad del socket de red establecido.
             * - Manejo de errores/Desconexión: Si falla la lectura por corte de red (IOException) o desajuste de clases,
             *   se registra el evento de desconexión, se elimina de la lista de peers y se marca como caído.
             */
            while (conexion.isConectado()) {
                PaqueteMensaje mensaje = conexion.recibir();
                onMensajeDePeer(mensaje, peerId);
            }
        } catch (IOException | ClassNotFoundException e) {
            log.registrar("PEER", "Conexión perdida con Nodo " + peerId, reloj.tick());
            peers.remove(peerId);
            // NO marcar como caído aquí — eso lo hará el HeartbeatManager (Fase 4)
            // Por ahora, marcamos manualmente:
            membresia.marcarCaido(peerId);
        }
    }

    // ==================== MANEJO DE MENSAJES PEER ====================

    /**
     * Dispatcher central de mensajes recibidos de peers.
     * Enruta cada tipo al módulo correspondiente.
     */
    private void onMensajeDePeer(PaqueteMensaje mensaje, int peerOrigenId) {
        // Actualizar reloj de Lamport con la marca del mensaje recibido
        reloj.actualizar(mensaje.getLamportTimestamp());

        switch (mensaje.getTipo()) {
            case REPLICA:
                // Mensaje replicado: difundir a clientes locales, NO re-replicar
                if (mensaje.getRemitente().equals("Sistema") && mensaje.getContenido().contains("|")) {
                    // Actualización de comando aprendido en recurso crítico
                    String[] partes = mensaje.getContenido().split("\\|", 2);
                    if (partes.length == 2 && recursoCritico != null) {
                        recursoCritico.escribirComando(partes[0], partes[1]);
                        log.registrar("SECCION_CRITICA",
                            "Comando '" + partes[0] + "' replicado en recurso crítico",
                            reloj.getValor());
                    }
                    break; // No difundir a clientes locales
                }

                // Reconstruimos el tipo original para que los clientes locales (y el bot) lo reciban correctamente
                PaqueteMensaje.Tipo tipoOriginal = PaqueteMensaje.Tipo.TEXTO;
                if (mensaje.getRemitente().equals("Servidor")) {
                    tipoOriginal = PaqueteMensaje.Tipo.SISTEMA;
                } else if (mensaje.getContenido().startsWith("/")) {
                    tipoOriginal = PaqueteMensaje.Tipo.COMANDO;
                }

                PaqueteMensaje mensajeAClientes = new PaqueteMensaje(
                        mensaje.getRemitente(),
                        mensaje.getContenido(),
                        tipoOriginal
                );
                mensajeAClientes.setLamportTimestamp(mensaje.getLamportTimestamp());
                mensajeAClientes.setNodoOrigenId(mensaje.getNodoOrigenId());

                difundirLocal(mensajeAClientes);
                actualizarHistorial(mensajeAClientes);
                log.registrar("REPLICA", "Replica de Nodo " + peerOrigenId + ": " + mensaje.getContenido(), reloj.getValor());
                break;

            case HEARTBEAT:
            case ELECTION:
            case ELECTION_OK:
            case COORDINATOR:
            case TOKEN_PASS:
            case TOKEN_REQUEST:
            case TOKEN_RELEASE:
            case MEMBERSHIP_UPDATE:
                // Delegar a módulos de coordinación (Fases 3, 4, 5)
                if (onMensajePeer != null) {
                    onMensajePeer.accept(mensaje);
                } else {
                    log.registrar("WARN", "Mensaje " + mensaje.getTipo() + " recibido pero no hay handler registrado", reloj.getValor());
                }
                break;

            case SHUTDOWN:
                if (Config.TOKEN_VALIDO.equals(mensaje.getContenido())) {
                    log.registrar("SISTEMA", "Recibida señal de APAGADO administrativo de peer. Apagando...", reloj.tick());
                    log.flush();
                    System.out.println("[NODO " + nodoId + "] Recibida señal de APAGADO administrativo de peer. Deteniendo...");
                    System.exit(0);
                }
                break;

            case SYNC_REQUEST:
                // Responder al solicitante con el contenido de la memoria serializado
                PaqueteMensaje syncResp = new PaqueteMensaje(
                    "Nodo" + nodoId,
                    recursoCritico.serializarComandos(),
                    PaqueteMensaje.Tipo.SYNC_RESPONSE
                );
                enviarAPeer(peerOrigenId, syncResp);
                log.registrar("SYNC", "Enviada copia de comandos a Nodo " + peerOrigenId, reloj.getValor());
                break;

            case SYNC_RESPONSE:
                // Cargar comandos recibidos en el recurso crítico
                recursoCritico.deserializarComandos(mensaje.getContenido());
                log.registrar("SYNC", "Comandos recibidos y sincronizados desde Nodo " + peerOrigenId, reloj.getValor());
                break;

            default:
                log.registrar("WARN", "Tipo de mensaje peer no reconocido: " + mensaje.getTipo(), reloj.getValor());
        }
    }

    // ==================== BROADCAST Y REPLICACIÓN ====================

    /**
     * Difunde un mensaje a todos los clientes conectados a ESTE servidor.
     * NO envía a peers — para eso usar replicarAPeers().
     * Usa snapshot de la lista para evitar deadlock con locks anidados.
     */
    public void difundirLocal(PaqueteMensaje mensaje) {
        // Tomar snapshot para evitar locks anidados (clientesLocales + info.out)
        List<ClienteInfo> snapshot;
        synchronized (clientesLocales) {
            snapshot = new ArrayList<>(clientesLocales);
        }

        List<ClienteInfo> desconectados = new ArrayList<>();
        for (ClienteInfo info : snapshot) {
            try {
                synchronized (info.out) {
                    info.out.writeObject(mensaje);
                    info.out.flush();
                }
            } catch (IOException e) {
                log.registrar("CLIENTE", "Cliente eliminado por fallo: " + info.nombre, reloj.getValor());
                desconectados.add(info);
            }
        }

        // Limpiar clientes desconectados fuera del loop de envío
        if (!desconectados.isEmpty()) {
            synchronized (clientesLocales) {
                clientesLocales.removeAll(desconectados);
            }
        }
    }

    /**
     * Envía una copia REPLICA del mensaje a todos los peers conectados.
     * Los peers difundirán el mensaje a sus clientes locales.
     */
    public void replicarAPeers(PaqueteMensaje mensajeOriginal) {
        // Crear versión REPLICA del mensaje
        PaqueteMensaje replica = new PaqueteMensaje(
                mensajeOriginal.getRemitente(),
                mensajeOriginal.getContenido(),
                PaqueteMensaje.Tipo.REPLICA
        );
        replica.setLamportTimestamp(mensajeOriginal.getLamportTimestamp());
        replica.setNodoOrigenId(nodoId);

        for (Map.Entry<Integer, ConexionPeer> entry : peers.entrySet()) {
            ConexionPeer peer = entry.getValue();
            if (peer.isConectado()) {
                peer.enviar(replica);
            }
        }
    }

    // ==================== HISTORIAL ====================

    /**
     * Agrega un mensaje al historial con límite de tamaño.
     * Synchronized para garantizar atomicidad entre verificación de tamaño y modificación.
     */
    private synchronized void actualizarHistorial(PaqueteMensaje mensaje) {
        while (historial.size() >= Config.MAX_HISTORIAL) {
            historial.poll();
        }
        historial.offer(mensaje);
    }

    // ==================== API PÚBLICA PARA MÓDULOS DE COORDINACIÓN ====================

    /**
     * Envía un mensaje a un peer específico con marca Lamport.
     * Usado por: AlgoritmoBully, HeartbeatManager, TokenRing
     */
    public void enviarAPeer(int targetNodoId, PaqueteMensaje msg) {
        ConexionPeer peer = peers.get(targetNodoId);
        if (peer != null && peer.isConectado()) {
            int marca = reloj.tick();
            msg.setLamportTimestamp(marca);
            msg.setNodoOrigenId(nodoId);
            peer.enviar(msg);
        }
    }

    /**
     * Envía un mensaje a TODOS los peers conectados con marca Lamport.
     * Crea una copia independiente para cada peer para evitar race conditions
     * al modificar la marca Lamport del mismo objeto desde múltiples envíos.
     */
    public void enviarATodosPeers(PaqueteMensaje msg) {
        for (int id : peers.keySet()) {
            // Crear copia independiente para cada peer (cada envío asigna su propia marca Lamport)
            PaqueteMensaje copia = new PaqueteMensaje(
                msg.getRemitente(), msg.getContenido(), msg.getTipo());
            enviarAPeer(id, copia);
        }
    }

    // ==================== GETTERS / SETTERS ====================

    public int getNodoId() { return nodoId; }
    public int getCoordinadorId() { return coordinadorId; }
    public void setCoordinadorId(int id) {
        this.coordinadorId = id;
        if (id == nodoId) {
            epochCoordinador.incrementAndGet();
        }
        log.registrar("COORDINADOR", "Coordinador actualizado a Nodo " + id + " (época " + epochCoordinador.get() + ")", reloj.tick());
    }
    public int getEpochCoordinador() { return epochCoordinador.get(); }
    public void setEpochCoordinador(int epoch) { this.epochCoordinador.set(epoch); }
    public RelojLamport getReloj() { return reloj; }
    public EventLog getLog() { return log; }
    public MembresiaCluster getMembresia() { return membresia; }
    public Map<Integer, ConexionPeer> getPeers() { return peers; }

    // Setter para el callback de mensajes peer (usado por Fases 3, 4, 5)
    public void setOnMensajePeer(Consumer<PaqueteMensaje> callback) {
        this.onMensajePeer = callback;
    }

    /**
     * Dispatcher central: enruta mensajes de peers al módulo correspondiente.
     * Es llamado automáticamente por onMensajeDePeer() para tipos de coordinación.
     */
    private void dispatchMensajePeer(PaqueteMensaje mensaje) {
        switch (mensaje.getTipo()) {
            // --- Heartbeats (Fase 4) ---
            case HEARTBEAT:
                heartbeatManager.onHeartbeatRecibido(mensaje);
                break;

            // --- Elección Bully (Fase 3) ---
            case ELECTION:
            case ELECTION_OK:
            case COORDINATOR:
                int nuevaEpoca = mensaje.getEpoch();
                algoritmoBully.procesarMensaje(mensaje);
                // Si recibimos COORDINATOR, verificar si debemos regenerar o invalidar token
                if (mensaje.getTipo() == PaqueteMensaje.Tipo.COORDINATOR) {
                    int nuevoCoord = Integer.parseInt(mensaje.getContenido());
                    if (nuevoCoord != nodoId) {
                        setEpochCoordinador(nuevaEpoca);
                    }
                    if (nuevoCoord == nodoId) {
                        // Somos el nuevo coordinador → regenerar token
                        tokenRing.regenerarToken();
                    } else {
                        // Otro es el nuevo coordinador → invalidamos el nuestro para evitar duplicados
                        tokenRing.invalidarToken();
                    }
                }
                break;

            // --- Token Ring (Fase 5) ---
            case TOKEN_PASS:
            case TOKEN_REQUEST:
            case TOKEN_RELEASE:
                tokenRing.procesarMensaje(mensaje);
                break;

            // --- Membresía ---
            case MEMBERSHIP_UPDATE:
                log.registrar("MEMBRESIA",
                    "Actualización de membresía de Nodo " + mensaje.getNodoOrigenId(),
                    reloj.getValor());
                break;

            default:
                log.registrar("WARN",
                    "Tipo de mensaje peer no manejado en dispatcher: " + mensaje.getTipo(),
                    reloj.getValor());
        }
    }

    // ==================== MAIN ====================

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Uso: java NodoServidor <nodoId>");
            System.err.println("  nodoId: 1, 2 o 3");
            System.exit(1);
        }

        int nodoId;
        try {
            nodoId = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.err.println("Error: nodoId debe ser un número entero. Recibido: '" + args[0] + "'");
            System.exit(1);
            return;
        }

        if (nodoId < 1 || nodoId > Config.NUM_NODOS) {
            System.err.println("nodoId debe estar entre 1 y " + Config.NUM_NODOS);
            System.exit(1);
        }

        NodoServidor servidor = new NodoServidor(nodoId);
        servidor.iniciar();
    }
}
