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
        int puerto = args.length > 1 ? Integer.parseInt(args[1]) : Config.PUERTO_CLIENTES_NODO_1;

        try {
            Socket socket = new Socket(host, puerto);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();

            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            enviarAuth(out);
            enviarLogin(out);

            System.out.println(NOMBRE_BOT + " conectado a " + host + ":" + puerto);
            System.out.println("Motor concurrente y memoria dinámica activados.");

            while (true) {
                try {
                    PaqueteMensaje mensaje = (PaqueteMensaje) in.readObject();

                    if ((mensaje.getTipo() == PaqueteMensaje.Tipo.COMANDO ||
                            (mensaje.getTipo() == PaqueteMensaje.Tipo.REPLICA && mensaje.getContenido().startsWith("/")))
                            && !mensaje.getRemitente().equals(NOMBRE_BOT)) {

                        poolComandos.execute(new ProcesadorComando(mensaje, out));
                    }

                } catch (ClassNotFoundException e) {
                    System.err.println("[BOT] Error procesando paquete recibido.");
                }
            }

        } catch (IOException e) {
            System.err.println("[BOT] El bot perdió conexión con el servidor principal. Cerrando proceso...");
            poolComandos.shutdownNow();
            System.exit(0);
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
                return memoriaComandos.get(instruccion);
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
