package org.example.distributed;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Recurso crítico compartido: la memoria de comandos aprendidos.
 *
 * - LECTURA: libre, no requiere token (ConcurrentHashMap es thread-safe para lecturas)
 * - ESCRITURA: requiere que el nodo tenga el token del TokenRing
 */
public class RecursoCritico {

    private final ConcurrentHashMap<String, String> memoriaComandos = new ConcurrentHashMap<>();

    /**
     * Lee un comando aprendido. NO requiere token.
     * @return la respuesta del comando, o null si no existe
     */
    public String leerComando(String comando) {
        return memoriaComandos.get(comando);
    }

    /**
     * Verifica si un comando existe. NO requiere token.
     */
    public boolean existeComando(String comando) {
        return memoriaComandos.containsKey(comando);
    }

    /**
     * Escribe un comando aprendido. REQUIERE token del TokenRing.
     * Debe llamarse dentro de tokenRing.ejecutarEnSeccionCritica().
     */
    public void escribirComando(String comando, String respuesta) {
        memoriaComandos.put(comando, respuesta);
    }

    /**
     * Obtiene una copia de todos los comandos (para replicación).
     */
    public Map<String, String> obtenerTodos() {
        return new ConcurrentHashMap<>(memoriaComandos);
    }

    /**
     * Reemplaza todos los comandos (para sincronización al unirse al cluster).
     * Synchronized para garantizar atomicidad: clear() + putAll() deben ser
     * una operación indivisible (lecturas concurrentes no deben ver un mapa vacío).
     */
    public synchronized void sincronizar(Map<String, String> comandos) {
        memoriaComandos.clear();
        memoriaComandos.putAll(comandos);
    }
}
