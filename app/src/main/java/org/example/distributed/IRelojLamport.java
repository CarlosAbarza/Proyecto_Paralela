package org.example.distributed;

/**
 * Interfaz para el reloj lógico de Lamport.
 * Implementada en Fase 1.
 * Usada por: NodoServidor (Fase 2), todos los módulos de coordinación.
 * 
 * Reglas de Lamport:
 * 1. Antes de cada evento local: tick()
 * 2. Al enviar mensaje: tick(), adjuntar getValor() al mensaje
 * 3. Al recibir mensaje: actualizar(timestamp_recibido)
 */
public interface IRelojLamport {
    /** Incrementa el reloj en 1 y retorna el nuevo valor. Thread-safe. */
    int tick();
    
    /** Actualiza el reloj a max(local, recibido) + 1. Thread-safe. */
    void actualizar(int timestampRecibido);
    
    /** Retorna el valor actual del reloj sin modificarlo. */
    int getValor();
}
