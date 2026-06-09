package org.example;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utilidad para iniciar automáticamente todos los servidores del clúster (1, 2 y 3)
 * en procesos separados en segundo plano.
 * Registra un shutdown hook para detenerlos limpiamente si se cancela este proceso.
 */
public class ClusterLauncher {

    private static final List<Process> procesos = new ArrayList<>();

    public static void main(String[] args) {
        System.out.println("=== INICIANDO CLÚSTER DE SERVIDORES ===");

        // Crear directorio de logs si no existe
        File logsDir = new File("logs");
        if (!logsDir.exists()) {
            logsDir.mkdirs();
        }

        String classPath = System.getProperty("java.class.path");

        // Registrar Shutdown Hook para matar los servidores al cerrar el lanzador
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n=== DETENIENDO CLÚSTER DE SERVIDORES ===");
            for (Process p : procesos) {
                if (p.isAlive()) {
                    p.destroyForcibly();
                }
            }
            System.out.println("✓ Todos los procesos de NodoServidor finalizados.");
        }));

        // Iniciar cada uno de los servidores
        for (int i = 1; i <= Config.NUM_NODOS; i++) {
            try {
                int nodoId = i;
                ProcessBuilder pb = new ProcessBuilder(
                    "java",
                    "-cp", classPath,
                    "org.example.NodoServidor",
                    String.valueOf(nodoId)
                );

                // Redirigir salida e historia de logs
                File logFile = new File(logsDir, "nodo_" + nodoId + ".log");
                pb.redirectOutput(ProcessBuilder.Redirect.to(logFile));
                pb.redirectError(ProcessBuilder.Redirect.to(logFile));

                Process proceso = pb.start();
                procesos.add(proceso);
                System.out.println("✓ NodoServidor " + nodoId + " iniciado. Logs en: " + logFile.getAbsolutePath());

            } catch (IOException e) {
                System.err.println("❌ Error al iniciar NodoServidor " + i + ": " + e.getMessage());
            }
        }

        System.out.println("\n✓ Clúster corriendo. Presiona Ctrl+C para finalizar todos los servidores.");
        
        // Mantener el hilo principal vivo esperando a que los procesos terminen o se presione Ctrl+C
        while (true) {
            boolean algunVivo = false;
            for (Process p : procesos) {
                if (p.isAlive()) {
                    algunVivo = true;
                    break;
                }
            }
            if (!algunVivo) {
                System.out.println("Todos los servidores se han cerrado.");
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
