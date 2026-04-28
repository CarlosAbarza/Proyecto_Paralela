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
    // Lista protegida de flujos de salida para difusión concurrente
    private static final List<ObjectOutputStream> clientes = new ArrayList<>();
    
    // Historial para transferencia de estado inicial (resiliencia y sincronización)
    private static final int MAX_HISTORIAL = 15;
    private static final Queue<PaqueteMensaje> historial = new ConcurrentLinkedQueue<>();

    public static void main(String[] args) {
        try (ServerSocket servidor = new ServerSocket(5000)) {
            System.out.println("Servidor de Telegram iniciado en puerto 5000...");
            
            while (true) {
                Socket socket = servidor.accept();
                System.out.println("Nodo conectado: " + socket.getInetAddress());
                
                // IMPORTANTE: El flujo de salida se crea primero para evitar Deadlocks en ObjectStream
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                
                // Sincronización de estado: Enviamos el historial antes de añadir al cliente a la lista activa
                for (PaqueteMensaje msj : historial) {
                    out.writeObject(msj);
                }
                out.flush();

                synchronized (clientes) {
                    clientes.add(out);
                }
                
                new Thread(new ManejadorCliente(socket, out)).start();
            }
        } catch (IOException e) {
            System.err.println("Fallo crítico en el nodo servidor: " + e.getMessage());
        }
    }

    static class ManejadorCliente implements Runnable {
        private final Socket socket;
        private final ObjectOutputStream out;

        public ManejadorCliente(Socket socket, ObjectOutputStream out) {
            this.socket = socket;
            this.out = out;
        }

        @Override
        public void run() {
            try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
                while (true) {
                    PaqueteMensaje mensaje = (PaqueteMensaje) in.readObject();
                    if (mensaje != null) {
                        actualizarHistorial(mensaje);
                        difundir(mensaje);
                    }
                }
            } catch (IOException | ClassNotFoundException e) {
                System.out.println("Fallo detectado (Crash/Omisión): Un cliente se ha desconectado.");
            } finally {
                limpiarRecursos();
            }
        }

        private void actualizarHistorial(PaqueteMensaje msj) {
            if (historial.size() >= MAX_HISTORIAL) historial.poll();
            historial.offer(msj);
        }

        private void difundir(PaqueteMensaje msj) {
            synchronized (clientes) {
                for (ObjectOutputStream cliente : clientes) {
                    try {
                        cliente.writeObject(msj);
                        cliente.flush();
                    } catch (IOException e) {
                        // El fallo se gestionará individualmente en cada hilo de cliente
                    }
                }
            }
        }

        private void limpiarRecursos() {
            synchronized (clientes) {
                clientes.remove(out);
            }
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}