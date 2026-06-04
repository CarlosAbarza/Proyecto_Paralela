package org.example.distributed;

/**
 * Interfaz para el registro persistente de eventos con marcas lógicas.
 * Implementada en Fase 1.
 * Usada por: todos los módulos para registrar eventos.
 * 
 * Cada línea del log tiene formato:
 * [YYYY-MM-DD HH:mm:ss.SSS][L:N][NODO:X][TIPO] descripción
 */
public interface IEventLog {
    /** Registra un evento en el log. */
    void registrar(String tipoEvento, String descripcion, int lamportTimestamp);
    
    /** Fuerza la escritura del buffer al archivo. */
    void flush();
    
    /** Cierra el archivo de log. */
    void cerrar();
}
