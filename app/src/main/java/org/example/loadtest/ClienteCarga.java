package org.example.loadtest;

import org.example.Config;
import org.example.PaqueteMensaje;
import java.io.*;
import java.net.Socket;

/**
 * Cliente simplificado para prueba de carga.
 * Se conecta a un servidor, se autentica, y envía mensajes/comandos
 * durante el tiempo especificado mientras mide latencia de envío.
 *
 * Nota sobre latencia: se mide el tiempo de escritura en el buffer del socket,
 * no el round-trip completo. Esto es aceptable para el modelo de chat donde
 * los mensajes se difunden asíncronamente a todos los clientes.
 *
 * Cada instancia corre en su propio hilo.
 */
public class ClienteCarga implements Runnable {

    private final String host;
    private final int puerto;
    private final String nombre;
    private final int duracionSegundos;
    private final RecolectorMetricas metricas;
    private volatile boolean activo = true;

    public ClienteCarga(String host, int puerto, String nombre,
                        int duracionSegundos, RecolectorMetricas metricas) {
        this.host = host;
        this.puerto = puerto;
        this.nombre = nombre;
        this.duracionSegundos = duracionSegundos;
        this.metricas = metricas;
    }

    @Override
    public void run() {
        Socket socket = null;
        boolean conectado = false;
        try {
            // 1. Conectar
            socket = new Socket(host, puerto);
            socket.setSoTimeout(5000); // timeout de lectura 5s
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            // 2. Autenticar
            out.writeObject(new PaqueteMensaje("auth", Config.TOKEN_VALIDO,
                    PaqueteMensaje.Tipo.AUTH));
            out.flush();
            out.writeObject(new PaqueteMensaje(nombre, "LOGIN",
                    PaqueteMensaje.Tipo.LOGIN));
            out.flush();

            conectado = true;
            metricas.registrarConexion();

            // 3. Consumir mensaje de bienvenida + historial
            consumirMensajesIniciales(in);

            // 4. Hilo receptor: consume respuestas y cuenta
            Thread receptor = new Thread(() -> recibirRespuestas(in), "Receptor-" + nombre);
            receptor.setDaemon(true);
            receptor.start();

            // 5. Enviar mensajes durante duracionSegundos
            long fin = System.currentTimeMillis() + (duracionSegundos * 1000L);
            int contador = 0;

            /*
             * CICLO DE ENVÍO DE MENSAJES DE PRUEBA:
             * - Qué hace: Envía ráfagas de mensajes de prueba de manera constante durante el tiempo de duración definido.
             * - Con quién se comunica: Con el nodo servidor de clúster al que está conectado.
             * - De qué depende: De la bandera 'activo' y del temporizador del sistema para no sobrepasar la duración de la prueba.
             * - Manejo de errores: Si falla el envío (IOException), registra el error en las métricas colectivas y aborta el ciclo.
             */
            while (System.currentTimeMillis() < fin && activo) {
                long t0 = System.nanoTime();

                PaqueteMensaje msg = crearMensaje(contador);

                try {
                    synchronized (out) {
                        out.writeObject(msg);
                        out.flush();
                        out.reset(); // evitar memory leak
                    }

                    long t1 = System.nanoTime();
                    // Nota: mide latencia de escritura en buffer del socket (envío),
                    // no round-trip completo. La respuesta se recibe asíncronamente
                    // en el hilo receptor. Es una aproximación aceptable para la
                    // prueba de carga dado el modelo asíncrono del chat.
                    metricas.registrarLatencia((t1 - t0) / 1_000_000.0);
                    metricas.registrarEnvio();
                } catch (IOException e) {
                    metricas.registrarError();
                    // Intentar reconectar o abandonar
                    break;
                }

                contador++;

                // ~10 mensajes/segundo por cliente → 500 msg/s total con 50 clientes
                Thread.sleep(100);
            }

        } catch (Exception e) {
            metricas.registrarError();
        } finally {
            activo = false;
            if (conectado) {
                metricas.registrarDesconexion();
            }
            if (socket != null) {
                try { socket.close(); } catch (IOException e) { /* ignorar */ }
            }
        }
    }

    /**
     * Crea el mensaje a enviar según el contador.
     * Alterna entre texto normal, /ping y /aprender para ejercitar todo.
     */
    private PaqueteMensaje crearMensaje(int contador) {
        if (contador % 20 == 0) {
            // Cada 20 mensajes: /ping (comando del bot)
            return new PaqueteMensaje(nombre, "/ping", PaqueteMensaje.Tipo.COMANDO);
        } else if (contador % 50 == 25) {
            // Cada 50 mensajes: /aprender (exclusión mutua)
            return new PaqueteMensaje(nombre,
                    "/aprender /auto" + nombre.hashCode() + contador + " resp" + contador,
                    PaqueteMensaje.Tipo.COMANDO);
        } else {
            // Mensaje de texto normal
            return new PaqueteMensaje(nombre,
                    "Carga-" + nombre + "-" + contador,
                    PaqueteMensaje.Tipo.TEXTO);
        }
    }

    /**
     * Consume los mensajes iniciales (bienvenida, historial, unión al chat).
     */
    private void consumirMensajesIniciales(ObjectInputStream in) {
        try {
            // Consumir hasta 10 mensajes iniciales (bienvenida + historial)
            in.readObject(); // bienvenida
            // Los demás se consumirán en el hilo receptor
        } catch (Exception e) {
            // Ignorar — el hilo receptor se encargará
        }
    }

    /**
     * Loop de recepción de respuestas. Corre en hilo daemon.
     */
    private void recibirRespuestas(ObjectInputStream in) {
        /*
         * CICLO DE ESCUCHA DE RESPUESTAS EN PRUEBA DE CARGA:
         * - Qué hace: Lee en segundo plano todos los mensajes provenientes del servidor para medir las respuestas.
         * - Con quién se comunica: Con el NodoServidor al que está conectado.
         * - De qué depende: De la validez de la conexión y de que 'activo' sea verdadero.
         * - Manejo de errores: Si ocurre SocketTimeoutException, continúa escuchando. Si ocurre otra excepción,
         *   registra el error y rompe el ciclo finalizando el hilo.
         */
        while (activo) {
            try {
                Object obj = in.readObject();
                if (obj instanceof PaqueteMensaje) {
                    metricas.registrarRespuesta();
                }
            } catch (java.net.SocketTimeoutException e) {
                // Timeout de lectura — no es un error, seguir intentando
                continue;
            } catch (Exception e) {
                if (activo) {
                    metricas.registrarError();
                }
                break;
            }
        }
    }

    public void detener() {
        activo = false;
    }
}
