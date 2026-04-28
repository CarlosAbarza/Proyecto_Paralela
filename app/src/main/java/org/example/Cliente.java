package org.example;

import java.io.*;
import java.net.Socket;

public class Cliente {
    private static String miNombre;

    public static void main(String[] args) {
        try {
            Socket socket = new Socket("localhost", 5000);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            BufferedReader teclado = new BufferedReader(new InputStreamReader(System.in));

            System.out.print("Nombre de usuario: ");
            miNombre = teclado.readLine();

            // Hilo dedicado al envío de mensajes (Input del usuario)
            new Thread(new HiloEnvio(out, miNombre)).start();

            // Hilo principal escucha el broadcast del servidor (Output de red)
            while (true) {
                try {
                    PaqueteMensaje msj = (PaqueteMensaje) in.readObject();
                    String nombreAMostrar = msj.getRemitente().equals(miNombre) ? "Tú" : msj.getRemitente();
                    
                    System.out.println("[" + msj.getFechaFormateada() + "] " 
                            + nombreAMostrar + ": " + msj.getContenido());
                } catch (ClassNotFoundException e) {
                    System.err.println("Error al deserializar el paquete.");
                }
            }
        } catch (IOException e) {
            System.err.println("Conexión perdida con el servidor.");
        }
    }

    static class HiloEnvio implements Runnable {
        private final ObjectOutputStream out;
        private final String nombre;

        public HiloEnvio(ObjectOutputStream out, String nombre) {
            this.out = out;
            this.nombre = nombre;
        }

        @Override
        public void run() {
            BufferedReader teclado = new BufferedReader(new InputStreamReader(System.in));
            try {
                while (true) {
                    String texto = teclado.readLine();
                    if (texto != null && !texto.trim().isEmpty()) {
                        // Limpieza ANSI: Sube el cursor y borra la línea escrita por el usuario
                        System.out.print("\033[1A\033[2K");
                        
                        out.writeObject(new PaqueteMensaje(nombre, texto));
                        out.flush();
                    }
                }
            } catch (IOException e) {
                System.err.println("Error en el flujo de envío.");
            }
        }
    }
}