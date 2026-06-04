package org.example.loadtest;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Recolecta y calcula métricas de rendimiento durante la prueba de carga.
 * Thread-safe: puede ser usado por múltiples clientes simultáneamente.
 */
public class RecolectorMetricas {

    private final AtomicLong totalEnvios = new AtomicLong(0);
    private final AtomicLong totalRespuestas = new AtomicLong(0);
    private final AtomicLong totalErrores = new AtomicLong(0);
    private final List<Double> latencias = Collections.synchronizedList(new ArrayList<>());

    private long inicioMs;
    private long finMs;

    // Falla inducida
    private volatile long timestampFalla = 0;
    private volatile long timestampRecuperacion = 0;

    public void iniciar() {
        inicioMs = System.currentTimeMillis();
    }

    public void finalizar() {
        finMs = System.currentTimeMillis();
    }

    // --- Registros thread-safe ---

    public void registrarEnvio() {
        totalEnvios.incrementAndGet();
    }

    public void registrarRespuesta() {
        totalRespuestas.incrementAndGet();
        // Si estamos en fase de recuperación, marcar (CAS manual para thread-safety)
        if (timestampFalla > 0 && timestampRecuperacion == 0) {
            synchronized (this) {
                if (timestampRecuperacion == 0) { // double-check bajo lock
                    timestampRecuperacion = System.currentTimeMillis();
                }
            }
        }
    }

    public void registrarError() {
        totalErrores.incrementAndGet();
    }

    public void registrarLatencia(double ms) {
        latencias.add(ms);
    }

    public void marcarFalla() {
        timestampFalla = System.currentTimeMillis();
        System.out.println("\n⚠️  FALLA INDUCIDA marcada a T=" + timestampFalla);
    }

    // --- Cálculos ---

    public double getThroughput() {
        double duracionSeg = (finMs - inicioMs) / 1000.0;
        return duracionSeg > 0 ? totalRespuestas.get() / duracionSeg : 0;
    }

    public double getLatenciaPromedio() {
        synchronized (latencias) {
            if (latencias.isEmpty()) return 0;
            double suma = 0;
            for (double l : latencias) suma += l;
            return suma / latencias.size();
        }
    }

    public double getLatenciaP95() {
        List<Double> sorted;
        synchronized (latencias) {
            if (latencias.isEmpty()) return 0;
            sorted = new ArrayList<>(latencias);
        }
        Collections.sort(sorted);
        int index = (int) Math.ceil(sorted.size() * 0.95) - 1;
        return sorted.get(Math.max(0, index));
    }

    public double getTasaError() {
        long total = totalEnvios.get();
        if (total == 0) return 0;
        return (totalErrores.get() * 100.0) / total;
    }

    public long getTiempoRecuperacionMs() {
        if (timestampFalla == 0 || timestampRecuperacion == 0) return -1;
        return timestampRecuperacion - timestampFalla;
    }

    // --- Reportes ---

    public void imprimirResumen() {
        System.out.println();
        System.out.println("══════════════════════════════════════════════════");
        System.out.println("         RESUMEN DE PRUEBA DE CARGA");
        System.out.println("══════════════════════════════════════════════════");
        System.out.printf("  Duración:          %.1f segundos%n", (finMs - inicioMs) / 1000.0);
        System.out.printf("  Envíos totales:    %d%n", totalEnvios.get());
        System.out.printf("  Respuestas:        %d%n", totalRespuestas.get());
        System.out.printf("  Errores:           %d%n", totalErrores.get());
        System.out.println("──────────────────────────────────────────────────");
        System.out.printf("  Throughput:        %.2f resp/s%n", getThroughput());
        System.out.printf("  Latencia promedio: %.2f ms%n", getLatenciaPromedio());
        System.out.printf("  Latencia p95:      %.2f ms%n", getLatenciaP95());
        System.out.printf("  Tasa de error:     %.2f%%%n", getTasaError());
        if (timestampFalla > 0) {
            System.out.println("──────────────────────────────────────────────────");
            System.out.printf("  Tiempo recup.:     %d ms%n", getTiempoRecuperacionMs());
        }
        System.out.println("══════════════════════════════════════════════════");
    }

    public void generarReporte(String archivoSalida) {
        try {
            // Asegurar que el directorio de salida existe
            File file = new File(archivoSalida);
            File parent = file.getParentFile();
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }
            
            try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
                pw.println("=== REPORTE DE PRUEBA DE CARGA ===");
                pw.println("Fecha: " + java.time.LocalDateTime.now());
                pw.println("Duración: " + String.format("%.1f", (finMs - inicioMs) / 1000.0) + " segundos");
                pw.println();
                pw.println("--- Volumen ---");
                pw.println("Total envíos:     " + totalEnvios.get());
                pw.println("Total respuestas: " + totalRespuestas.get());
                pw.println("Total errores:    " + totalErrores.get());
                pw.println();
                pw.println("--- Rendimiento ---");
                pw.println("Throughput:        " + String.format("%.2f", getThroughput()) + " resp/s");
                pw.println("Latencia promedio: " + String.format("%.2f", getLatenciaPromedio()) + " ms");
                pw.println("Latencia p95:      " + String.format("%.2f", getLatenciaP95()) + " ms");
                pw.println("Tasa de error:     " + String.format("%.2f", getTasaError()) + "%");
                pw.println();
                if (timestampFalla > 0) {
                    pw.println("--- Falla Inducida ---");
                    pw.println("Timestamp falla:       " + timestampFalla);
                    pw.println("Timestamp recuperación:" + timestampRecuperacion);
                    pw.println("Tiempo recuperación:   " + getTiempoRecuperacionMs() + " ms");
                }
                pw.println();
                pw.println("=================================");

                System.out.println("Reporte guardado en: " + archivoSalida);
            }
        } catch (IOException e) {
            System.err.println("Error al generar reporte: " + e.getMessage());
        }
    }

    // --- Getters para monitoreo en tiempo real ---
    public long getTotalEnvios() { return totalEnvios.get(); }
    public long getTotalRespuestas() { return totalRespuestas.get(); }
    public long getTotalErrores() { return totalErrores.get(); }
}
