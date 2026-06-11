package org.example;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class App {

    private static class ClienteInfo {
        private final ObjectOutputStream out;
        private final String nombre;

        ClienteInfo(ObjectOutputStream out, String nombre) {
            this.out = out;
            this.nombre = nombre;
        }
    }

    private static final List<ClienteInfo> clientes = new ArrayList<>();
    private static final Queue<PaqueteMensaje> historial = new ConcurrentLinkedQueue<>();

    public static void main(String[] args) {
        int puerto = args.length > 0 ? Integer.parseInt(args[0]) : Config.getPuertoClientes(1);

        try (ServerSocket servidor = new ServerSocket(puerto)) {
            System.out.println("Servidor de Telegram iniciado en puerto " + puerto + "...");

            while (true) {
                Socket socket = servidor.accept();
                System.out.println("[CONEXIÓN] Nodo conectado desde: " + socket.getInetAddress());

                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();

                Thread hilo = new Thread(new ManejadorCliente(socket, out));
                hilo.start();
            }

        } catch (IOException e) {
            System.err.println("[FALLO CRÍTICO] No se pudo iniciar o mantener el servidor: " + e.getMessage());
        }
    }

    static class ManejadorCliente implements Runnable {
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

                if (!autenticar(in)) {
                    return;
                }

                while (true) {
                    Object recibido = in.readObject();

                    if (!(recibido instanceof PaqueteMensaje)) {
                        enviarDirecto(new PaqueteMensaje(
                                "Servidor",
                                "Paquete inválido recibido. Solo se aceptan objetos PaqueteMensaje.",
                                PaqueteMensaje.Tipo.SISTEMA
                        ));
                        continue;
                    }

                    PaqueteMensaje mensajeRecibido = (PaqueteMensaje) recibido;
                    procesarMensaje(mensajeRecibido);
                }

            } catch (IOException | ClassNotFoundException e) {
                System.out.println("[CONEXIÓN] Se perdió la conexión con el nodo [" + nombreAsignado + "].");
            } finally {
                limpiarRecursos();
            }
        }

        private boolean autenticar(ObjectInputStream in) throws IOException, ClassNotFoundException {
            Object authObj = in.readObject();

            if (!(authObj instanceof PaqueteMensaje)) {
                System.out.println("[AUTH] Primer paquete inválido desde " + socket.getInetAddress());
                return false;
            }

            PaqueteMensaje auth = (PaqueteMensaje) authObj;

            if (auth.getTipo() != PaqueteMensaje.Tipo.AUTH ||
                    !Config.TOKEN_VALIDO.equals(auth.getContenido())) {

                System.out.println("[AUTH] Token inválido desde " + socket.getInetAddress());

                enviarDirecto(new PaqueteMensaje(
                        "Servidor",
                        "Token inválido. Conexión rechazada.",
                        PaqueteMensaje.Tipo.SISTEMA
                ));

                return false;
            }

            Object loginObj = in.readObject();

            if (!(loginObj instanceof PaqueteMensaje)) {
                System.out.println("[AUTH] Paquete LOGIN inválido desde " + socket.getInetAddress());
                return false;
            }

            PaqueteMensaje login = (PaqueteMensaje) loginObj;
            String nombre = login.getRemitente();

            if (login.getTipo() != PaqueteMensaje.Tipo.LOGIN || !nombreValido(nombre)) {
                System.out.println("[AUTH] Nombre inválido desde " + socket.getInetAddress() + ": " + nombre);

                enviarDirecto(new PaqueteMensaje(
                        "Servidor",
                        "Nombre inválido. Usa entre 3 y 20 caracteres: letras, números o guion bajo.",
                        PaqueteMensaje.Tipo.SISTEMA
                ));

                return false;
            }

            nombreAsignado = nombre;

            System.out.println("[AUTH] Usuario autenticado: " + nombreAsignado);

            enviarDirecto(new PaqueteMensaje(
                    "Servidor",
                    "Autenticación exitosa. Bienvenido, " + nombreAsignado + ".",
                    PaqueteMensaje.Tipo.SISTEMA
            ));

            enviarHistorial();

            synchronized (clientes) {
                clientes.add(new ClienteInfo(out, nombreAsignado));
            }

            difundir(new PaqueteMensaje(
                    "Servidor",
                    nombreAsignado + " se ha unido al chat.",
                    PaqueteMensaje.Tipo.SISTEMA
            ));

            return true;
        }

        private void procesarMensaje(PaqueteMensaje mensajeRecibido) {
            String contenido = mensajeRecibido.getContenido();

            if (contenido == null || contenido.trim().isEmpty()) {
                enviarDirecto(new PaqueteMensaje(
                        "Servidor",
                        "No se permiten mensajes vacíos.",
                        PaqueteMensaje.Tipo.SISTEMA
                ));
                return;
            }

            if (contenido.length() > Config.MAX_MENSAJE) {
                enviarDirecto(new PaqueteMensaje(
                        "Servidor",
                        "Mensaje rechazado: supera el límite de " + Config.MAX_MENSAJE + " caracteres.",
                        PaqueteMensaje.Tipo.SISTEMA
                ));
                return;
            }

            if (contenido.equalsIgnoreCase("/usuarios")) {
                enviarListaUsuarios();
                return;
            }

            if (contenido.equalsIgnoreCase("/historial")) {
                enviarHistorial();
                return;
            }

            PaqueteMensaje mensajeSeguro = new PaqueteMensaje(
                    nombreAsignado,
                    contenido,
                    mensajeRecibido.getTipo()
            );

            actualizarHistorial(mensajeSeguro);
            difundir(mensajeSeguro);
        }

        private boolean nombreValido(String nombre) {
            return nombre != null && nombre.matches("[a-zA-Z0-9_]{3,20}");
        }

        private void actualizarHistorial(PaqueteMensaje mensaje) {
            synchronized (historial) {
                while (historial.size() >= Config.MAX_HISTORIAL) {
                    historial.poll();
                }
                historial.offer(mensaje);
            }
        }

        private void difundir(PaqueteMensaje mensaje) {
            synchronized (clientes) {
                clientes.removeIf(info -> {
                    try {
                        synchronized (info.out) {
                            info.out.writeObject(mensaje);
                            info.out.flush();
                        }
                        return false;
                    } catch (IOException e) {
                        System.out.println("[CLIENTE] Cliente eliminado por fallo de envío: " + info.nombre);
                        return true;
                    }
                });
            }
        }

        private void enviarDirecto(PaqueteMensaje mensaje) {
            try {
                synchronized (out) {
                    out.writeObject(mensaje);
                    out.flush();
                }
            } catch (IOException e) {
                System.err.println("[ERROR] No se pudo enviar mensaje directo a " + nombreAsignado);
            }
        }

        private void enviarListaUsuarios() {
            List<String> nombres = new ArrayList<>();

            synchronized (clientes) {
                for (ClienteInfo info : clientes) {
                    if (info.nombre != null) {
                        nombres.add(info.nombre);
                    }
                }
            }

            enviarDirecto(new PaqueteMensaje(
                    "Servidor",
                    "Usuarios conectados: " + nombres,
                    PaqueteMensaje.Tipo.SISTEMA
            ));
        }

        private void enviarHistorial() {
            List<PaqueteMensaje> copiaHistorial = new ArrayList<>();

            synchronized (historial) {
                copiaHistorial.addAll(historial);
            }

            if (copiaHistorial.isEmpty()) {
                enviarDirecto(new PaqueteMensaje(
                        "Servidor",
                        "Historial vacío.",
                        PaqueteMensaje.Tipo.SISTEMA
                ));
                return;
            }

            enviarDirecto(new PaqueteMensaje(
                    "Servidor",
                    "=== HISTORIAL RECIENTE ===",
                    PaqueteMensaje.Tipo.SISTEMA
            ));

            for (PaqueteMensaje mensaje : copiaHistorial) {
                enviarDirecto(mensaje);
            }
        }

        private void limpiarRecursos() {
            synchronized (clientes) {
                clientes.removeIf(info -> info.out == this.out);
            }

            try {
                socket.close();
            } catch (IOException e) {
                System.err.println("[ERROR] No se pudo cerrar el socket de " + nombreAsignado);
            }

            if (!"Desconocido".equals(nombreAsignado)) {
                difundir(new PaqueteMensaje(
                        "Servidor",
                        nombreAsignado + " salió del chat.",
                        PaqueteMensaje.Tipo.SISTEMA
                ));
            }
        }
    }
}
