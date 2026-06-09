package org.example.distributed;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Sistema de logging persistente para eventos distribuidos.
 * Cada evento se registra con su marca temporal real y lógica (Lamport).
 *
 * Los logs se guardan en: logs/nodo_X.log
 * Formato por línea:
 *   [2026-06-03 14:30:22.123][L:42][NODO:1][MENSAJE] Cliente1 envió "hola"
 */
public class EventLog implements IEventLog {
    private static final DateTimeFormatter FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final int nodoId;
    private final PrintWriter writer;
    private final String archivoLog;
    private final boolean debugConsola;

    public EventLog(int nodoId) {
        this(nodoId, false);
    }

    public EventLog(int nodoId, boolean debugConsola) {
        this.nodoId = nodoId;
        this.debugConsola = debugConsola;
        this.archivoLog = "logs/nodo_" + nodoId + ".log";

        try {
            File dir = new File("logs");
            if (!dir.exists()) dir.mkdirs();

            this.writer = new PrintWriter(
                new BufferedWriter(new FileWriter(archivoLog, true)),
                false  // no autoflush — controlamos nosotros con flush()
            );
        } catch (IOException e) {
            throw new RuntimeException("No se pudo crear archivo de log: " + archivoLog, e);
        }
    }

    @Override
    public synchronized void registrar(String tipoEvento, String descripcion, int lamportTimestamp) {
        String linea = String.format("[%s][L:%d][NODO:%d][%s] %s",
            LocalDateTime.now().format(FORMATTER),
            lamportTimestamp,
            nodoId,
            tipoEvento,
            descripcion
        );
        writer.println(linea);

        // Verificar errores silenciosos de PrintWriter
        if (writer.checkError()) {
            System.err.println("ERROR: Fallo de escritura en log " + archivoLog);
        }

        // Imprimir en consola solo en modo debug (evita contención de System.out bajo carga)
        if (debugConsola) {
            System.out.println("LOG: " + linea);
        }
    }

    @Override
    public synchronized void flush() {
        writer.flush();
    }

    @Override
    public synchronized void cerrar() {
        writer.flush();
        writer.close();
    }
}
