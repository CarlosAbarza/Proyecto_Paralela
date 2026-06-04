package org.example.distributed;

import org.example.Config;
import org.example.PaqueteMensaje;
import org.example.NodoServidor;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementación de exclusión mutua distribuida por anillo de token.
 *
 * El token circula: 1 → 2 → 3 → 1 (solo entre nodos activos).
 * Solo el nodo con el token puede ejecutar operaciones de escritura
 * sobre el RecursoCritico.
 *
 * Uso:
 *   TokenRing tokenRing = new TokenRing(servidor);
 *   tokenRing.iniciar();  // el coordinador crea el token
 *
 *   // Cuando se necesita escribir:
 *   boolean ok = tokenRing.ejecutarEnSeccionCritica(() -> {
 *       recursoCritico.escribirComando("/saludo", "Hola!");
 *   }, Config.TOKEN_TIMEOUT_MS);
 *
 *   // Cuando llega un mensaje TOKEN_PASS de un peer:
 *   tokenRing.procesarMensaje(mensaje);
 */
public class TokenRing {

    private final NodoServidor servidor;
    private final int nodoId;
    private final EventLog log;
    private final RelojLamport reloj;

    // ¿Tenemos el token?
    private volatile boolean tengoToken = false;

    // Cola de operaciones pendientes esperando el token
    private final BlockingQueue<Runnable> operacionesPendientes = new LinkedBlockingQueue<>();

    // Contador de mensajes de token (para métricas de la prueba de carga)
    private final AtomicInteger contadorMensajesToken = new AtomicInteger(0);

    // Hilo del ciclo del token
    private volatile Thread hiloToken;
    private volatile boolean activo = false;

    public TokenRing(NodoServidor servidor) {
        this.servidor = servidor;
        this.nodoId = servidor.getNodoId();
        this.log = servidor.getLog();
        this.reloj = servidor.getReloj();
    }

    /**
     * Inicializa el token ring.
     * Solo el coordinador crea el token inicial.
     */
    public void iniciar() {
        activo = true;

        if (nodoId == servidor.getCoordinadorId()) {
            tengoToken = true;
            log.registrar("TOKEN", "Token inicial creado en Nodo " + nodoId + " (coordinador)", reloj.tick());
            iniciarCicloToken();
        } else {
            log.registrar("TOKEN", "TokenRing iniciado, esperando token del coordinador", reloj.tick());
        }
    }

    /**
     * Inicia el hilo que gestiona el ciclo del token.
     */
    private void iniciarCicloToken() {
        if (hiloToken != null && hiloToken.isAlive()) return;

        hiloToken = new Thread(this::cicloToken, "TokenRing-" + nodoId);
        hiloToken.setDaemon(true);
        hiloToken.start();
    }

    /**
     * Ciclo principal del token ring.
     * Synchronized con ejecutarEnSeccionCritica() para evitar TOCTOU:
     * no se puede pasar el token mientras se verifica/ejecuta una sección crítica.
     */
    private void cicloToken() {
        while (activo && tengoToken) {
            try {
                // Intentar ejecutar operaciones pendientes (esperar hasta 500ms)
                Runnable op = operacionesPendientes.poll(500, TimeUnit.MILLISECONDS);

                if (op != null) {
                    synchronized (this) {
                        log.registrar("TOKEN", "Ejecutando operación en sección crítica", reloj.tick());
                        op.run();
                        log.registrar("TOKEN", "Operación en sección crítica completada", reloj.tick());
                    }
                } else {
                    // No hay operaciones pendientes → pasar el token
                    synchronized (this) {
                        pasarToken();
                    }
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * Pasa el token al siguiente nodo activo en el anillo.
     * Anillo lógico: 1 → 2 → 3 → 1
     * Si el envío falla, reintenta con el siguiente nodo activo.
     * Solo libera el token cuando el envío es exitoso (evita pérdida del token).
     */
    private void pasarToken() {
        int siguiente = obtenerSiguienteNodo();
        if (siguiente == nodoId) {
            // Soy el único nodo activo, me quedo con el token
            try { Thread.sleep(1000); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return;
        }

        PaqueteMensaje tokenMsg = new PaqueteMensaje(
            "Nodo" + nodoId,
            "TOKEN",
            PaqueteMensaje.Tipo.TOKEN_PASS
        );

        // Intentar enviar al siguiente; si falla, probar con otros nodos activos
        int intentos = 0;
        int nodoDestino = siguiente;
        while (intentos < Config.NUM_NODOS) {
            servidor.enviarAPeer(nodoDestino, tokenMsg);
            // Verificar si el peer sigue conectado después del envío
            var peer = servidor.getPeers().get(nodoDestino);
            if (peer != null && peer.isConectado()) {
                // Envío exitoso → ahora sí liberar el token
                tengoToken = false;
                contadorMensajesToken.incrementAndGet();
                log.registrar("TOKEN", "Token pasado a Nodo " + nodoDestino, reloj.getValor());
                return;
            }
            // Falló → intentar con el siguiente nodo en el anillo
            log.registrar("TOKEN", "Fallo al pasar token a Nodo " + nodoDestino + ", reintentando", reloj.getValor());
            nodoDestino = (nodoDestino % Config.NUM_NODOS) + 1;
            if (nodoDestino == nodoId) nodoDestino = (nodoDestino % Config.NUM_NODOS) + 1;
            intentos++;
        }

        // No se pudo enviar a nadie → conservar el token
        log.registrar("TOKEN", "No se pudo pasar el token, conservándolo", reloj.getValor());
    }

    /**
     * Obtiene el siguiente nodo activo en el anillo lógico.
     * Recorre circularmente: si estamos en 2, busca 3, luego 1, etc.
     */
    private int obtenerSiguienteNodo() {
        int siguiente = nodoId;
        for (int i = 0; i < Config.NUM_NODOS; i++) {
            siguiente = (siguiente % Config.NUM_NODOS) + 1;
            if (siguiente != nodoId && servidor.getMembresia().isActivo(siguiente)) {
                return siguiente;
            }
        }
        return nodoId; // Solo este nodo está activo
    }

    /**
     * Procesa un mensaje de token recibido de otro nodo.
     */
    public void procesarMensaje(PaqueteMensaje mensaje) {
        switch (mensaje.getTipo()) {
            case TOKEN_PASS:
                tengoToken = true;
                contadorMensajesToken.incrementAndGet();
                log.registrar("TOKEN",
                    "Token recibido de Nodo " + mensaje.getNodoOrigenId(), reloj.getValor());

                // Iniciar ciclo del token
                iniciarCicloToken();
                break;

            case TOKEN_REQUEST:
                log.registrar("TOKEN",
                    "Solicitud de token desde Nodo " + mensaje.getNodoOrigenId(), reloj.getValor());
                // Podría acelerar el paso del token
                break;

            default:
                break;
        }
    }

    /**
     * Ejecuta una operación en la sección crítica.
     * Si ya tenemos el token, se ejecuta inmediatamente bajo lock.
     * Si no, se encola y espera a que llegue el token.
     *
     * @param operacion la operación a ejecutar con exclusión mutua
     * @param timeoutMs tiempo máximo de espera para obtener el token
     * @return true si se ejecutó exitosamente, false si timeout
     */
    public boolean ejecutarEnSeccionCritica(Runnable operacion, long timeoutMs) {
        // Intentar ejecución inmediata bajo lock (evita TOCTOU entre check y uso del token)
        synchronized (this) {
            if (tengoToken) {
                log.registrar("TOKEN",
                    "Ejecución inmediata en sección crítica (ya tenemos token)", reloj.tick());
                operacion.run();
                return true;
            }
        }

        // No tenemos el token → encolar y esperar
        CompletableFuture<Void> futuro = new CompletableFuture<>();
        operacionesPendientes.offer(() -> {
            operacion.run();
            futuro.complete(null);
        });

        try {
            futuro.get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
            log.registrar("TOKEN", "Timeout esperando token para sección crítica", reloj.getValor());
            return false;
        }
    }

    /**
     * Regenera el token. Solo el coordinador puede hacerlo.
     * Se llama cuando se detecta que el portador del token cayó.
     */
    public void regenerarToken() {
        if (nodoId == servidor.getCoordinadorId()) {
            tengoToken = true;
            log.registrar("TOKEN",
                "Token REGENERADO por coordinador (Nodo " + nodoId + ")", reloj.tick());
            iniciarCicloToken();
        }
    }

    /**
     * Invalida el token local si ya no somos el poseedor legítimo
     * (por ejemplo, si se eligió un nuevo coordinador que regeneró el token).
     */
    public synchronized void invalidarToken() {
        if (tengoToken) {
            tengoToken = false;
            log.registrar("TOKEN", "Token invalidado localmente por cambio de coordinador", reloj.getValor());
        }
    }

    // --- Getters para métricas ---

    public boolean tieneToken() { return tengoToken; }
    public int getContadorMensajes() { return contadorMensajesToken.get(); }
}
