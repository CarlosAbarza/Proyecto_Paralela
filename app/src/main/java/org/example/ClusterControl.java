package org.example;

import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Utilidad de control administrativo para comandar el apagado
 * remoto de un servidor específico del clúster (falla inducida).
 */
public class ClusterControl {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Uso: java org.example.ClusterControl <nodoId>");
            System.exit(1);
        }

        int nodoId;
        try {
            nodoId = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.err.println("Error: nodoId debe ser un número entero.");
            System.exit(1);
            return;
        }

        int puerto = Config.getPuertoClientes(nodoId);

        try (Socket socket = new Socket("localhost", puerto);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
            
            out.flush();

            // Enviar mensaje de SHUTDOWN con el token de autenticación como contenido
            PaqueteMensaje shutdownMsg = new PaqueteMensaje(
                "Admin",
                Config.TOKEN_VALIDO,
                PaqueteMensaje.Tipo.SHUTDOWN
            );
            out.writeObject(shutdownMsg);
            out.flush();
            System.out.println("✓ Señal de apagado remoto enviada con éxito al Nodo " + nodoId + " (puerto " + puerto + ").");

        } catch (Exception e) {
            System.err.println("❌ Error al enviar señal de apagado al Nodo " + nodoId + ": " + e.getMessage());
        }
    }
}
