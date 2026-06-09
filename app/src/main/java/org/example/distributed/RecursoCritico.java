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
     *
     * AUDITORÍA / CRÍTICO:
     * - Este método está correctamente definido pero NUNCA se invoca en el resto de la aplicación.
     * - Como resultado, si un nodo del clúster se cae y se levanta nuevamente, se reiniciará con su
     *   memoria de comandos 'recursoCritico' completamente VACÍA y no recuperará los comandos
     *   aprendidos previamente en los otros nodos, rompiendo la consistencia eventual.
     * - MEJORA: Durante el handshake o al terminar la elección bully, el nuevo nodo debe solicitar
     *   una copia del diccionario de comandos aprendidos de un peer activo y llamar a este método.
     */
    public synchronized void sincronizar(Map<String, String> comandos) {
        memoriaComandos.clear();
        memoriaComandos.putAll(comandos);
    }

    /**
     * Serializa la memoria de comandos en una cadena de texto (formato: cmd1|resp1;cmd2|resp2).
     */
    public String serializarComandos() {
        StringBuilder sb = new StringBuilder();
        for (var entry : memoriaComandos.entrySet()) {
            if (sb.length() > 0) sb.append(";");
            sb.append(entry.getKey()).append("|").append(entry.getValue());
        }
        return sb.toString();
    }

    /**
     * Deserializa una cadena de texto y carga los comandos en memoria.
     */
    public synchronized void deserializarComandos(String str) {
        if (str == null || str.trim().isEmpty()) return;
        String[] entries = str.split(";");
        for (String entry : entries) {
            String[] parts = entry.split("\\|", 2);
            if (parts.length == 2) {
                memoriaComandos.put(parts[0], parts[1]);
            }
        }
    }
}
