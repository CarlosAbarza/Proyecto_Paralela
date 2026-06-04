package org.example;

public final class Config {
    private Config() {}

    // --- Configuración original ---
    public static final String TOKEN_VALIDO = "ICI4344";
    public static final int MAX_HISTORIAL = 15;
    public static final int MAX_MENSAJE = 300;

    // --- Configuración del Cluster ---
    public static final int NUM_NODOS = 3;
    public static final String HOST_DEFAULT = "localhost";

    // Puertos para clientes (cada servidor escucha clientes en su puerto)
    public static final int PUERTO_CLIENTES_NODO_1 = 5001;
    public static final int PUERTO_CLIENTES_NODO_2 = 5002;
    public static final int PUERTO_CLIENTES_NODO_3 = 5003;

    // Puertos para comunicación peer-to-peer entre servidores
    public static final int PUERTO_PEERS_NODO_1 = 6001;
    public static final int PUERTO_PEERS_NODO_2 = 6002;
    public static final int PUERTO_PEERS_NODO_3 = 6003;

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
        switch (nodoId) {
            case 1: return PUERTO_CLIENTES_NODO_1;
            case 2: return PUERTO_CLIENTES_NODO_2;
            case 3: return PUERTO_CLIENTES_NODO_3;
            default: throw new IllegalArgumentException("Nodo ID inválido: " + nodoId);
        }
    }

    // Método helper para obtener el puerto peer de un nodo dado su ID
    public static int getPuertoPeers(int nodoId) {
        switch (nodoId) {
            case 1: return PUERTO_PEERS_NODO_1;
            case 2: return PUERTO_PEERS_NODO_2;
            case 3: return PUERTO_PEERS_NODO_3;
            default: throw new IllegalArgumentException("Nodo ID inválido: " + nodoId);
        }
    }
}
