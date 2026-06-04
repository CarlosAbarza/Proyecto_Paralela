package org.example.distributed;

import org.example.PaqueteMensaje;
import java.io.*;
import java.net.Socket;

/**
 * Encapsula una conexión TCP con otro nodo servidor (peer).
 * Cada NodoServidor tiene un ConexionPeer por cada otro servidor del cluster.
 */
public class ConexionPeer {
    private final int peerNodoId;
    private final Socket socket;
    private final ObjectOutputStream out;
    private final ObjectInputStream in;
    private volatile boolean conectado;

    /**
     * Constructor que crea nuevos streams (para uso directo con socket recién creado).
     * IMPORTANTE: No usar si ya se crearon streams sobre este socket.
     */
    public ConexionPeer(int peerNodoId, Socket socket) throws IOException {
        this.peerNodoId = peerNodoId;
        this.socket = socket;
        this.out = new ObjectOutputStream(socket.getOutputStream());
        this.out.flush();
        this.in = new ObjectInputStream(socket.getInputStream());
        this.conectado = true;
    }

    /**
     * Constructor que reutiliza streams existentes.
     * Usar cuando ya se realizó un handshake con ObjectOutputStream/ObjectInputStream
     * sobre el mismo socket (evita crear un segundo header de stream que causaría
     * StreamCorruptedException).
     */
    public ConexionPeer(int peerNodoId, Socket socket,
                        ObjectOutputStream out, ObjectInputStream in) {
        this.peerNodoId = peerNodoId;
        this.socket = socket;
        this.out = out;
        this.in = in;
        this.conectado = true;
    }

    /**
     * Envía un mensaje al peer. Thread-safe.
     * Si falla, marca la conexión como desconectada y lanza excepción.
     */
    public synchronized boolean enviar(PaqueteMensaje mensaje) {
        if (!conectado) return false;
        try {
            out.writeObject(mensaje);
            out.flush();
            out.reset(); // evitar memory leak por caché de objetos
            return true;
        } catch (IOException e) {
            conectado = false;
            return false;
        }
    }

    /**
     * Recibe un mensaje del peer. Bloqueante.
     */
    public PaqueteMensaje recibir() throws IOException, ClassNotFoundException {
        Object obj = in.readObject();
        if (obj instanceof PaqueteMensaje) {
            return (PaqueteMensaje) obj;
        }
        throw new ClassNotFoundException("Objeto recibido no es PaqueteMensaje");
    }

    public boolean isConectado() { return conectado; }
    public int getPeerNodoId() { return peerNodoId; }

    public void cerrar() {
        conectado = false;
        try { socket.close(); } catch (IOException e) { /* ignorar */ }
    }
}
