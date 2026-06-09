package org.example.distributed;

import org.example.Config;
import org.example.PaqueteMensaje;
import org.example.NodoServidor;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Gestor de heartbeats para detección de fallos.
 *
 * Envía heartbeats periódicos a todos los peers y verifica
 * que los peers respondan dentro del timeout configurado.
 *
 * Uso:
 *   HeartbeatManager hb = new HeartbeatManager(servidor);
 *   hb.setOnCoordinadorCaido(() -> bully.iniciarEleccion());
 *   hb.iniciar();
 *   // ...
 *   hb.onHeartbeatRecibido(mensaje);  // cuando llega un HEARTBEAT
 */
public class HeartbeatManager {

    private final NodoServidor servidor;
    private final int nodoId;
    private final EventLog log;
    private final RelojLamport reloj;
    private final MembresiaCluster membresia;

    // Última vez que recibimos heartbeat de cada peer (millis)
    private final Map<Integer, Long> ultimoHeartbeat = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private volatile boolean activo = false;

    // Callback para cuando el coordinador cae
    private volatile Runnable onCoordinadorCaido;

    public HeartbeatManager(NodoServidor servidor) {
        this.servidor = servidor;
        this.nodoId = servidor.getNodoId();
        this.log = servidor.getLog();
        this.reloj = servidor.getReloj();
        this.membresia = servidor.getMembresia();

        // Inicializar timestamps: dar gracia inicial
        long ahora = System.currentTimeMillis();
        /*
         * CICLO DE INICIALIZACIÓN DE TIMESTAMPS:
         * - Qué hace: Establece el timestamp inicial de latido para todos los demás nodos del clúster
         *   como el tiempo actual del sistema para dar una gracia inicial y evitar falsos positivos al arrancar.
         * - De qué depende: Del número total de nodos configurados.
         */
        for (int i = 1; i <= Config.NUM_NODOS; i++) {
            if (i != nodoId) {
                ultimoHeartbeat.put(i, ahora);
            }
        }
    }

    /**
     * Inicia el envío periódico de heartbeats y la verificación de timeouts.
     */
    public void iniciar() {
        activo = true;

        // Tarea 1: Enviar heartbeats cada HEARTBEAT_INTERVAL_MS
        scheduler.scheduleAtFixedRate(this::enviarHeartbeats,
            1000, Config.HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);

        // Tarea 2: Verificar timeouts cada HEARTBEAT_INTERVAL_MS
        scheduler.scheduleAtFixedRate(this::verificarTimeouts,
            Config.HEARTBEAT_TIMEOUT_MS, Config.HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);

        log.registrar("HEARTBEAT",
            "HeartbeatManager iniciado (intervalo=" + Config.HEARTBEAT_INTERVAL_MS
            + "ms, timeout=" + Config.HEARTBEAT_TIMEOUT_MS + "ms)", reloj.tick());
    }

    /**
     * Detiene el HeartbeatManager.
     */
    public void detener() {
        activo = false;
        scheduler.shutdownNow();
    }

    /**
     * Envía un heartbeat a todos los peers conectados.
     */
    private void enviarHeartbeats() {
        if (!activo) return;

        /*
         * CICLO DE EMISIÓN DE LATIDOS:
         * - Qué hace: Envía un mensaje de tipo HEARTBEAT a todos los servidores (peers) que tenemos registrados en el mapa.
         * - Con quién se comunica: Con todos los peers activos o con socket de red conectado en el clúster.
         * - De qué depende: De las conexiones TCP registradas en el mapa de peers.
         */
        for (int id : servidor.getPeers().keySet()) {
            PaqueteMensaje hb = new PaqueteMensaje(
                "Nodo" + nodoId,
                String.valueOf(nodoId),
                PaqueteMensaje.Tipo.HEARTBEAT
            );
            servidor.enviarAPeer(id, hb);
        }
    }

    /**
     * Verifica si algún peer ha excedido el timeout de heartbeat.
     * Synchronized para evitar carreras con onHeartbeatRecibido().
     */
    private synchronized void verificarTimeouts() {
        if (!activo) return;

        long ahora = System.currentTimeMillis();

        /*
         * CICLO DE AUDITORÍA DE TIMEOUT DE LATIDOS:
         * - Qué hace: Recorre todos los nodos del clúster (excepto el propio) y verifica si el tiempo transcurrido desde su
         *   último latido supera el límite tolerado ('Config.HEARTBEAT_TIMEOUT_MS').
         * - Con quién se comunica: Lee datos locales de 'ultimoHeartbeat' y de 'membresia'.
         * - De qué depende: Del número de nodos y de los valores registrados en 'ultimoHeartbeat'.
         * - Manejo de errores/Tolerancia a fallos: Si se detecta un timeout, marca al nodo como CAIDO en el registro de membresía.
         *   Si el nodo caído era el coordinador del clúster, invoca el callback 'onCoordinadorCaido' (iniciando una nueva elección Bully).
         */
        for (int id = 1; id <= Config.NUM_NODOS; id++) {
            if (id == nodoId) continue;
            if (!membresia.isActivo(id)) continue; // ya está marcado como caído

            Long ultimo = ultimoHeartbeat.get(id);
            if (ultimo != null && (ahora - ultimo) > Config.HEARTBEAT_TIMEOUT_MS) {
                // ¡Timeout! Marcar como caído
                membresia.marcarCaido(id);
                log.registrar("FALLO",
                    "Nodo " + id + " detectado como CAIDO (timeout heartbeat: "
                    + (ahora - ultimo) + "ms)", reloj.tick());

                // Si el coordinador cayó, disparar elección
                if (id == servidor.getCoordinadorId()) {
                    log.registrar("FALLO",
                        "El coordinador (Nodo " + id + ") cayó. Disparando elección.", reloj.getValor());
                    if (onCoordinadorCaido != null) {
                        onCoordinadorCaido.run();
                    }
                }
            }
        }
    }

    /**
     * Procesa un heartbeat recibido de un peer.
     * Actualiza el timestamp y reintegra al nodo si estaba caído.
     * Synchronized para evitar carreras con verificarTimeouts().
     */
    public synchronized void onHeartbeatRecibido(PaqueteMensaje mensaje) {
        int origenId = mensaje.getNodoOrigenId();
        if (origenId < 1 || origenId > Config.NUM_NODOS || origenId == nodoId) return;

        ultimoHeartbeat.put(origenId, System.currentTimeMillis());

        // Si estaba marcado como caído, reintegrar
        if (!membresia.isActivo(origenId)) {
            membresia.marcarActivo(origenId);
            log.registrar("RECUPERACION",
                "Nodo " + origenId + " reintegrado al cluster (heartbeat recibido)", reloj.tick());
        }
    }

    /**
     * Registra el callback que se ejecuta cuando el coordinador cae.
     * Típicamente: () -> bully.iniciarEleccion()
     */
    public void setOnCoordinadorCaido(Runnable callback) {
        this.onCoordinadorCaido = callback;
    }

    public boolean isActivo() { return activo; }
}
