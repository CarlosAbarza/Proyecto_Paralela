package org.example;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NodoBot {
    private static final String NOMBRE_BOT = "BotCentral";

    private static final ExecutorService poolComandos = Executors.newFixedThreadPool(10);
    private static final ConcurrentHashMap<String, String> memoriaComandos = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int puertoInicial = args.length > 1 ? Integer.parseInt(args[1]) : -1;
        if (args.length > 2) {
            try {
                int totalNodos = Integer.parseInt(args[2]);
                if (totalNodos < 1 || totalNodos >= 1000) {
                    System.err.println("Error: El número total de nodos debe estar en el rango [1, 999]. Recibido: " + totalNodos);
                    System.exit(1);
                }
                Config.NUM_NODOS = totalNodos;
            } catch (NumberFormatException e) {
                System.err.println("Error: totalNodos debe ser un número entero. Recibido: '" + args[2] + "'");
                System.exit(1);
            }
        }

        int nodoActual = -1;
        if (puertoInicial != -1) {
            for (int i = 1; i <= Config.NUM_NODOS; i++) {
                if (Config.getPuertoClientes(i) == puertoInicial) {
                    nodoActual = i;
                    break;
                }
            }
        } else {
            // Balanceador de carga: elegir un nodo inicial al azar
            nodoActual = new java.util.Random().nextInt(Config.NUM_NODOS) + 1;
            System.out.println("[BALANCEADOR] Seleccionando de forma aleatoria el Nodo " + nodoActual + " para el bot.");
        }

        boolean primerIntento = true;
        int intentosFallidosConsecutivos = 0;
        int maxIntentos = 2 * Config.NUM_NODOS;

        /*
         * CICLO PRINCIPAL DE CONEXIÓN Y REINTENTOS DEL BOT:
         * - Qué hace: Intenta establecer conexión con uno de los servidores del clúster de forma circular.
         * - Con quién se comunica: Con el NodoServidor en el puerto de clientes asignado.
         * - De qué depende: De la disponibilidad de red, del host y de que al menos un servidor del clúster esté activo.
         * - Manejo de errores y reconexión: Si se pierde la conexión o no se puede conectar (IOException), cierra los
         *   streams y el socket, duerme 3 segundos e intenta con el siguiente nodo del clúster (1 -> 2 -> 3 -> 1).
         *   Si se agotan 'maxIntentos' (2 vueltas completas), el bot se apaga liberando el pool de hilos de comandos.
         */
        while (true) {
            int puerto;
            if (primerIntento && nodoActual == -1) {
                puerto = puertoInicial;
            } else {
                if (nodoActual == -1) {
                    nodoActual = 1;
                }
                puerto = Config.getPuertoClientes(nodoActual);
            }

            Socket socket = null;
            ObjectOutputStream out = null;
            ObjectInputStream in = null;

            try {
                System.out.println("[BOT] Intentando conectar a " + host + ":" + puerto + 
                        (nodoActual != -1 ? " (Nodo " + nodoActual + ")" : "") + "...");
                socket = new Socket(host, puerto);
                out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();

                in = new ObjectInputStream(socket.getInputStream());

                enviarAuth(out);
                enviarLogin(out);

                System.out.println(NOMBRE_BOT + " conectado a " + host + ":" + puerto);
                System.out.println("Motor concurrente y memoria dinámica activados.");
                
                primerIntento = false;
                intentosFallidosConsecutivos = 0; // Resetear al conectar exitosamente

                /*
                 * CICLO DE LECTURA Y ENRUTAMIENTO DE MENSAJES DEL BOT:
                 * - Qué hace: Recibe continuamente paquetes del servidor. Si el paquete es un comando (o réplica de comando)
                 *   y no fue enviado por el propio bot, delega su procesamiento asíncrono a un pool de hilos dedicado.
                 *   Esto evita bloquear la lectura principal del canal.
                 * - Con quién se comunica: Con el servidor al que está conectado para recibir mensajes de chat.
                 * - De qué depende: De que la conexión de red permanezca abierta.
                 * - Manejo de errores: Si ocurre un ClassNotFoundException, se reporta y continúa. Si ocurre una IOException,
                 *   el error se propaga hacia el try externo para que el bot inicie su proceso de reconexión.
                 */
                while (true) {
                    try {
                        PaqueteMensaje mensaje = (PaqueteMensaje) in.readObject();

                        if ((mensaje.getTipo() == PaqueteMensaje.Tipo.COMANDO ||
                                (mensaje.getTipo() == PaqueteMensaje.Tipo.REPLICA && mensaje.getContenido().startsWith("/")))
                                && !mensaje.getRemitente().equals(NOMBRE_BOT)) {

                            poolComandos.execute(new ProcesadorComando(mensaje, out));
                        }

                    } catch (ClassNotFoundException e) {
                        System.err.println("[BOT] Error procesando paquete recibido: " + e.getMessage());
                    }
                }

            } catch (IOException e) {
                System.err.println("[BOT] Conexión perdida o fallida con el servidor en " + host + ":" + puerto + ".");
                
                try {
                    if (in != null) in.close();
                    if (out != null) out.close();
                    if (socket != null) socket.close();
                } catch (IOException ex) {
                    // ignorar
                }

                intentosFallidosConsecutivos++;
                if (intentosFallidosConsecutivos >= maxIntentos) {
                    System.err.println("[BOT] Todos los servidores del clúster están caídos tras " + maxIntentos + " intentos (2 vueltas completas). Cerrando bot...");
                    poolComandos.shutdownNow();
                    System.exit(1);
                }

                if (nodoActual != -1) {
                    nodoActual = (nodoActual % Config.NUM_NODOS) + 1;
                } else {
                    nodoActual = 1;
                }
                
                primerIntento = false;

                System.err.println("[BOT] Intentando reconectar al próximo servidor en 3 segundos...");
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private static void enviarAuth(ObjectOutputStream out) throws IOException {
        PaqueteMensaje auth = new PaqueteMensaje(
                "auth",
                Config.TOKEN_VALIDO,
                PaqueteMensaje.Tipo.AUTH
        );

        out.writeObject(auth);
        out.flush();
    }

    private static void enviarLogin(ObjectOutputStream out) throws IOException {
        PaqueteMensaje login = new PaqueteMensaje(
                NOMBRE_BOT,
                "LOGIN",
                PaqueteMensaje.Tipo.LOGIN
        );

        out.writeObject(login);
        out.flush();
    }

    static class ProcesadorComando implements Runnable {
        private final PaqueteMensaje mensajeOriginal;
        private final ObjectOutputStream out;

        public ProcesadorComando(PaqueteMensaje mensajeOriginal, ObjectOutputStream out) {
            this.mensajeOriginal = mensajeOriginal;
            this.out = out;
        }

        @Override
        public void run() {
            String respuesta = generarRespuesta(
                    mensajeOriginal.getContenido(),
                    mensajeOriginal.getRemitente()
            );

            if (respuesta != null) {
                PaqueteMensaje mensajeRespuesta = new PaqueteMensaje(
                        NOMBRE_BOT,
                        respuesta,
                        PaqueteMensaje.Tipo.TEXTO
                );

                synchronized (out) {
                    try {
                        out.writeObject(mensajeRespuesta);
                        out.flush();
                    } catch (IOException e) {
                        System.err.println("[BOT] Error al enviar respuesta.");
                    }
                }
            }
        }

        private String generarRespuesta(String comandoCompleto, String usuario) {
            if (comandoCompleto == null || comandoCompleto.trim().isEmpty()) {
                return null;
            }

            String[] partes = comandoCompleto.trim().split(" ", 3);
            String instruccion = partes[0].toLowerCase();

            if (instruccion.equals("/aprender")) {
                return procesarAprendizaje(partes, usuario);
            }

            if (memoriaComandos.containsKey(instruccion)) {
                return null; // El servidor ya responde en su lugar usando el recurso crítico
            }

            switch (instruccion) {
                case "/ping":
                    return "Pong! Atendido por el hilo: " + Thread.currentThread().getName();

                case "/hora":
                    return "La hora actual del nodo bot es: "
                            + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));

                case "/pesado":
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return "El proceso pesado fue interrumpido.";
                    }

                    return "Proceso pesado de 5 segundos terminado para " + usuario + ".";

                case "/ayuda":
                    return "\n\nComandos del bot:\n/salir\n/ping\n/hora\n/pesado\n/aprender /comando respuesta\n/ayuda\n\nComandos del servidor:\n/usuarios\n/historial\n";

                default:
                    return "Comando desconocido ('" + instruccion + "'). Escribe /ayuda o enséñame algo con /aprender.";
            }
        }

        private String procesarAprendizaje(String[] partes, String usuario) {
            if (partes.length < 3) {
                return "Uso incorrecto. Formato: /aprender /comando Mensaje de respuesta";
            }

            String nuevoComando = partes[1].toLowerCase();
            String respuestaDinamica = partes[2];

            if (!nuevoComando.startsWith("/")) {
                return "La clave del comando debe comenzar con '/'. Ejemplo: /aprender /saludo Hola!";
            }

            if (esComandoReservado(nuevoComando)) {
                return "No se puede sobrescribir un comando reservado del sistema.";
            }

            if (nuevoComando.length() > 30) {
                return "El nombre del comando aprendido no puede superar los 30 caracteres.";
            }

            if (respuestaDinamica.length() > Config.MAX_MENSAJE) {
                return "La respuesta aprendida supera el límite de " + Config.MAX_MENSAJE + " caracteres.";
            }

            memoriaComandos.put(nuevoComando, respuestaDinamica);

            return "Comando '" + nuevoComando + "' aprendido exitosamente gracias a " + usuario + ".";
        }

        private boolean esComandoReservado(String comando) {
            return comando.equals("/ping")
                    || comando.equals("/hora")
                    || comando.equals("/pesado")
                    || comando.equals("/ayuda")
                    || comando.equals("/aprender")
                    || comando.equals("/usuarios")
                    || comando.equals("/historial")
                    || comando.equals("/salir");
        }
    }
}
