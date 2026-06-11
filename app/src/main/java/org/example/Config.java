package org.example;

public final class Config {
    private Config() {}

    // --- Configuración original ---
    public static final String TOKEN_VALIDO = "ICI4344";
    public static final int MAX_HISTORIAL = 15;
    public static final int MAX_MENSAJE = 300;

    // --- Configuración del Cluster ---
    public static int NUM_NODOS = 3;
    public static final String HOST_DEFAULT = "localhost";

    // Puertos base para clientes y peers
    public static final int PUERTO_CLIENTES_BASE = 5000;
    public static final int PUERTO_PEERS_BASE = 6000;

    // Heartbeat
    public static final int HEARTBEAT_INTERVAL_MS = 3000;
    public static final int HEARTBEAT_TIMEOUT_MS = 10000;

    // Elección
    public static final int ELECTION_TIMEOUT_MS = 5000;

    // Token Ring
    public static final int TOKEN_TIMEOUT_MS = 8000;

    // Prueba de carga
    public static final int CARGA_NUM_CLIENTES = 50;
    public static final int CARGA_DURACION_SEGUNDOS = 60;

    // Método helper para obtener el puerto de clientes de un nodo dado su ID
    public static int getPuertoClientes(int nodoId) {
        if (nodoId < 1 || nodoId >= 1000) {
            throw new IllegalArgumentException("Nodo ID inválido: " + nodoId + ". Rango permitido: [1, 999]");
        }
        return PUERTO_CLIENTES_BASE + nodoId;
    }

    // Método helper para obtener el puerto peer de un nodo dado su ID
    public static int getPuertoPeers(int nodoId) {
        if (nodoId < 1 || nodoId >= 1000) {
            throw new IllegalArgumentException("Nodo ID inválido: " + nodoId + ". Rango permitido: [1, 999]");
        }
        return PUERTO_PEERS_BASE + nodoId;
    }
}
