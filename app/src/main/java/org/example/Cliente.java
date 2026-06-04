package org.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class Cliente {
    private static String miNombre;

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int puerto = args.length > 1 ? Integer.parseInt(args[1]) : Config.PUERTO_CLIENTES_NODO_1;

        try {
            Socket socket = new Socket(host, puerto);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();

            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            BufferedReader teclado = new BufferedReader(new InputStreamReader(System.in));

            enviarAuth(out);

            miNombre = pedirNombre(teclado);
            enviarLogin(out, miNombre);

            Thread hiloEnvio = new Thread(new HiloEnvio(out, miNombre, teclado));
            hiloEnvio.start();

            System.out.println("Conectado al servidor " + host + ":" + puerto);
            System.out.println("Comandos disponibles: /usuarios, /historial, /ping, /hora, /pesado, /ayuda");
            System.out.println("Escribe /salir para cerrar el cliente.");

            while (true) {
                try {
                    PaqueteMensaje mensaje = (PaqueteMensaje) in.readObject();

                    String nombreAMostrar = mensaje.getRemitente().equals(miNombre)
                            ? "Tú"
                            : mensaje.getRemitente();

                    System.out.println("[" + mensaje.getFechaFormateada() + "][L:" + mensaje.getLamportTimestamp() + "] "
                            + nombreAMostrar + ": " + mensaje.getContenido());

                } catch (ClassNotFoundException e) {
                    System.err.println("[ERROR] Error al deserializar el paquete recibido.");
                }
            }

        } catch (IOException e) {
            System.err.println("[CONEXIÓN] Conexión perdida o no se pudo conectar con el servidor.");
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

    private static void enviarLogin(ObjectOutputStream out, String nombre) throws IOException {
        PaqueteMensaje login = new PaqueteMensaje(
                nombre,
                "LOGIN",
                PaqueteMensaje.Tipo.LOGIN
        );

        out.writeObject(login);
        out.flush();
    }

    private static String pedirNombre(BufferedReader teclado) throws IOException {
        while (true) {
            System.out.print("Nombre de usuario: ");
            String nombre = teclado.readLine();

            if (nombre != null && nombre.matches("[a-zA-Z0-9_]{3,20}")) {
                return nombre;
            }

            System.out.println("[ERROR] Nombre inválido. Usa entre 3 y 20 caracteres: letras, números o guion bajo.");
        }
    }

    static class HiloEnvio implements Runnable {
        private final ObjectOutputStream out;
        private final String nombre;
        private final BufferedReader teclado;

        public HiloEnvio(ObjectOutputStream out, String nombre, BufferedReader teclado) {
            this.out = out;
            this.nombre = nombre;
            this.teclado = teclado;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    String texto = teclado.readLine();

                    if (texto == null) {
                        continue;
                    }

                    texto = texto.trim();

                    if (texto.isEmpty()) {
                        continue;
                    }

                    if (texto.equalsIgnoreCase("/salir")) {
                        System.out.println("Cerrando cliente...");
                        System.exit(0);
                    }

                    if (texto.length() > Config.MAX_MENSAJE) {
                        System.out.println("[ERROR] Mensaje excede el límite de " + Config.MAX_MENSAJE + " caracteres.");
                        continue;
                    }

                    System.out.print("\033[1A\033[2K");

                    PaqueteMensaje.Tipo tipo = texto.startsWith("/")
                            ? PaqueteMensaje.Tipo.COMANDO
                            : PaqueteMensaje.Tipo.TEXTO;

                    PaqueteMensaje mensaje = new PaqueteMensaje(nombre, texto, tipo);

                    synchronized (out) {
                        out.writeObject(mensaje);
                        out.flush();
                    }
                }

            } catch (IOException e) {
                System.err.println("[ERROR] Error en el flujo de envío.");
            }
        }
    }
}
