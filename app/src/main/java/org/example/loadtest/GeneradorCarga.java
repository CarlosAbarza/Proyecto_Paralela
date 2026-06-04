package org.example.loadtest;

import org.example.Config;
import java.util.*;
import java.util.concurrent.*;

/**
 * Generador de carga para la prueba de tráfico del Proyecto Final.
 *
 * Lanza 50+ clientes simultáneos distribuidos entre los 3 servidores
 * durante al menos 60 segundos. Genera un reporte con métricas al finalizar.
 *
 * Uso:
 *   java GeneradorCarga [host]
 *
 * Ejemplo:
 *   java -cp app/build/classes/java/main org.example.loadtest.GeneradorCarga
 *   java -cp app/build/classes/java/main org.example.loadtest.GeneradorCarga 192.168.1.10
 */
public class GeneradorCarga {

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : Config.HOST_DEFAULT;
        int numClientes = Config.CARGA_NUM_CLIENTES; // 50
        int duracion = Config.CARGA_DURACION_SEGUNDOS; // 60

        System.out.println("╔══════════════════════════════════════════════════╗");
        System.out.println("║         GENERADOR DE CARGA — PROYECTO FINAL     ║");
        System.out.println("╠══════════════════════════════════════════════════╣");
        System.out.printf("║  Host:                %-26s║%n", host);
        System.out.printf("║  Clientes simultáneos: %-25d║%n", numClientes);
        System.out.printf("║  Duración:            %-23s  ║%n", duracion + " segundos");
        System.out.printf("║  Servidores:          %-25s║%n",
                Config.PUERTO_CLIENTES_NODO_1 + ", " +
                Config.PUERTO_CLIENTES_NODO_2 + ", " +
                Config.PUERTO_CLIENTES_NODO_3);
        System.out.println("╚══════════════════════════════════════════════════╝");
        System.out.println();

        // Puertos de los 3 servidores
        int[] puertos = {
            Config.PUERTO_CLIENTES_NODO_1,
            Config.PUERTO_CLIENTES_NODO_2,
            Config.PUERTO_CLIENTES_NODO_3
        };

        RecolectorMetricas metricas = new RecolectorMetricas();
        ExecutorService pool = Executors.newFixedThreadPool(numClientes);
        List<ClienteCarga> clientes = new ArrayList<>();

        // --- Lanzar clientes ---
        metricas.iniciar();

        for (int i = 0; i < numClientes; i++) {
            int puerto = puertos[i % 3]; // Round-robin entre los 3 servidores
            String nombre = "C" + String.format("%03d", i);
            ClienteCarga cliente = new ClienteCarga(host, puerto, nombre, duracion, metricas);
            clientes.add(cliente);
            pool.submit(cliente);
        }

        System.out.println("✓ " + numClientes + " clientes lanzados (distribuidos entre 3 servidores).");
        System.out.println();

        // --- Instrucciones de falla inducida ---
        System.out.println("┌──────────────────────────────────────────────────┐");
        System.out.println("│  ⚠️  INSTRUCCIONES DE FALLA INDUCIDA:            │");
        System.out.println("│                                                  │");
        System.out.println("│  A los ~30 segundos, derribar el coordinador     │");
        System.out.println("│  (Nodo 3) con Ctrl+C en su terminal.             │");
        System.out.println("│                                                  │");
        System.out.println("│  Luego presionar ENTER aquí para marcar la       │");
        System.out.println("│  falla en las métricas.                          │");
        System.out.println("└──────────────────────────────────────────────────┘");
        System.out.println();

        // --- Hilo para esperar que el usuario marque la falla ---
        Thread hiloFalla = new Thread(() -> {
            try {
                System.in.read(); // Esperar ENTER
                metricas.marcarFalla();
                System.out.println("✓ Falla marcada. Continuando prueba...");
            } catch (Exception e) {
                // Ignorar
            }
        }, "MarcarFalla");
        hiloFalla.setDaemon(true);
        hiloFalla.start();

        // --- Monitoreo en tiempo real ---
        Thread monitor = new Thread(() -> {
            try {
                for (int seg = 1; seg <= duracion; seg++) {
                    Thread.sleep(1000);
                    if (seg % 10 == 0) {
                        System.out.printf("[T=%ds] Envíos: %d | Respuestas: %d | Errores: %d%n",
                                seg,
                                metricas.getTotalEnvios(),
                                metricas.getTotalRespuestas(),
                                metricas.getTotalErrores());
                    }
                }
            } catch (InterruptedException e) {
                // Fin
            }
        }, "Monitor");
        monitor.setDaemon(true);
        monitor.start();

        // --- Esperar a que termine la prueba ---
        pool.shutdown();
        boolean terminado = pool.awaitTermination(duracion + 30, TimeUnit.SECONDS);

        if (!terminado) {
            System.out.println("⚠️  Algunos clientes no terminaron a tiempo. Forzando cierre...");
            pool.shutdownNow();
        }

        metricas.finalizar();

        // --- Resultados ---
        metricas.imprimirResumen();
        metricas.generarReporte("logs/reporte_carga.txt");

        System.out.println();
        System.out.println("✓ Prueba de carga finalizada.");
        System.out.println("✓ Reporte en: logs/reporte_carga.txt");
        System.out.println("✓ Logs de nodos en: logs/nodo_1.log, logs/nodo_2.log, logs/nodo_3.log");
    }
}
