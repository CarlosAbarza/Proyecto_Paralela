package org.example.distributed;

import org.example.Config;
import org.example.PaqueteMensaje;
import org.example.NodoServidor;
import java.util.concurrent.*;

/**
 * Implementación del algoritmo de elección Bully.
 *
 * Uso:
 *   AlgoritmoBully bully = new AlgoritmoBully(nodoServidor);
 *   bully.iniciarEleccion();          // cuando el coordinador cayó
 *   bully.procesarMensaje(mensaje);   // cuando llega ELECTION/OK/COORDINATOR
 */
public class AlgoritmoBully {

    private final NodoServidor servidor;
    private final int nodoId;
    private final EventLog log;
    private final RelojLamport reloj;

    // Estado de la elección
    private volatile boolean eleccionEnCurso = false;
    private volatile boolean recibiOk = false;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private ScheduledFuture<?> timeoutFuture;

    public AlgoritmoBully(NodoServidor servidor) {
        this.servidor = servidor;
        this.nodoId = servidor.getNodoId();
        this.log = servidor.getLog();
        this.reloj = servidor.getReloj();
    }

    /**
     * Inicia una elección Bully.
     * Se llama cuando se detecta que el coordinador actual cayó.
     */
    public synchronized void iniciarEleccion() {
        if (eleccionEnCurso) return; // evitar elecciones duplicadas
        eleccionEnCurso = true;
        recibiOk = false;

        log.registrar("ELECCION", "Iniciando elección Bully desde Nodo " + nodoId, reloj.tick());

        // Enviar ELECTION a todos los nodos con ID mayor
        boolean envioAlguno = false;
        for (int id = nodoId + 1; id <= Config.NUM_NODOS; id++) {
            if (servidor.getMembresia().isActivo(id) || 
                servidor.getMembresia().getEstado(id) == MembresiaCluster.EstadoNodo.DESCONOCIDO) {
                PaqueteMensaje election = new PaqueteMensaje(
                    "Nodo" + nodoId,
                    String.valueOf(nodoId),
                    PaqueteMensaje.Tipo.ELECTION
                );
                servidor.enviarAPeer(id, election);
                envioAlguno = true;
                log.registrar("ELECCION", "Enviado ELECTION a Nodo " + id, reloj.getValor());
            }
        }

        if (!envioAlguno) {
            // No hay nadie con ID mayor → yo soy el coordinador
            declararCoordinador();
            return;
        }

        // Esperar respuestas OK con timeout
        timeoutFuture = scheduler.schedule(() -> {
            synchronized (this) {
                if (!recibiOk && eleccionEnCurso) {
                    // Nadie respondió OK → yo soy el coordinador
                    declararCoordinador();
                }
            }
        }, Config.ELECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Procesa un mensaje de elección recibido de otro nodo.
     * Debe llamarse desde el dispatcher de mensajes peer del NodoServidor.
     */
    public synchronized void procesarMensaje(PaqueteMensaje mensaje) {
        int origenId = mensaje.getNodoOrigenId();

        switch (mensaje.getTipo()) {
            case ELECTION:
                log.registrar("ELECCION", "Recibido ELECTION de Nodo " + origenId, reloj.getValor());

                if (nodoId > origenId) {
                    // Respondemos OK ("yo estoy vivo y tengo mayor ID")
                    PaqueteMensaje ok = new PaqueteMensaje(
                        "Nodo" + nodoId,
                        String.valueOf(nodoId),
                        PaqueteMensaje.Tipo.ELECTION_OK
                    );
                    servidor.enviarAPeer(origenId, ok);
                    log.registrar("ELECCION", "Enviado OK a Nodo " + origenId, reloj.getValor());

                    // Cancelar timeout anterior antes de reiniciar elección
                    // (evita que el timeout previo se dispare y declare coordinador erróneamente)
                    if (timeoutFuture != null) {
                        timeoutFuture.cancel(false);
                    }
                    eleccionEnCurso = false; // reset para permitir nueva elección
                    iniciarEleccion();
                }
                break;

            case ELECTION_OK:
                log.registrar("ELECCION", "Recibido OK de Nodo " + origenId, reloj.getValor());
                recibiOk = true;

                // Alguien con mayor ID está vivo, cancelar mi timeout
                if (timeoutFuture != null) {
                    timeoutFuture.cancel(false);
                }

                // Nuevo timeout esperando COORDINATOR
                timeoutFuture = scheduler.schedule(() -> {
                    synchronized (this) {
                        if (eleccionEnCurso) {
                            log.registrar("ELECCION",
                                "Timeout esperando COORDINATOR, reiniciando elección", reloj.tick());
                            eleccionEnCurso = false;
                            iniciarEleccion();
                        }
                    }
                }, Config.ELECTION_TIMEOUT_MS * 2, TimeUnit.MILLISECONDS);
                break;

            case COORDINATOR:
                int nuevoCoordinador = Integer.parseInt(mensaje.getContenido());
                log.registrar("ELECCION",
                    "Nuevo coordinador: Nodo " + nuevoCoordinador + " (anunciado por Nodo " + origenId + ")",
                    reloj.getValor());
                servidor.setCoordinadorId(nuevoCoordinador);
                eleccionEnCurso = false;
                recibiOk = false;
                if (timeoutFuture != null) {
                    timeoutFuture.cancel(false);
                }
                break;

            default:
                break;
        }
    }

    /**
     * Se declara a sí mismo como coordinador y anuncia a todos.
     */
    private void declararCoordinador() {
        servidor.setCoordinadorId(nodoId);
        eleccionEnCurso = false;
        recibiOk = false;

        log.registrar("ELECCION",
            "¡Este nodo (" + nodoId + ") es el nuevo COORDINADOR!", reloj.tick());

        // Anunciar a todos los peers
        PaqueteMensaje coordinator = new PaqueteMensaje(
            "Nodo" + nodoId,
            String.valueOf(nodoId),
            PaqueteMensaje.Tipo.COORDINATOR
        );
        servidor.enviarATodosPeers(coordinator);
    }

    public boolean isEleccionEnCurso() { return eleccionEnCurso; }
}
