package org.example;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class PaqueteMensaje implements Serializable {
    private static final long serialVersionUID = 4L;

    public enum Tipo {
        // Tipos originales
        TEXTO,
        COMANDO,
        AUTH,
        LOGIN,
        SISTEMA,
        // Tipos para coordinación distribuida (Fase 1+)
        HEARTBEAT,
        ELECTION,
        ELECTION_OK,
        COORDINATOR,
        TOKEN_PASS,
        TOKEN_REQUEST,
        TOKEN_RELEASE,
        REPLICA,
        MEMBERSHIP_UPDATE
    }

    private final String remitente;
    private final String contenido;
    private final LocalDateTime fechaEnvio;
    private final Tipo tipo;
    private volatile int lamportTimestamp;  // Marca lógica de Lamport (volatile para visibilidad entre hilos)
    private volatile int nodoOrigenId;     // ID del nodo servidor que originó/procesó el mensaje

    public PaqueteMensaje(String remitente, String contenido, Tipo tipo) {
        this.remitente = remitente != null ? remitente : "Desconocido";
        this.contenido = contenido != null ? contenido : "";
        this.tipo = tipo != null ? tipo : Tipo.TEXTO;
        this.fechaEnvio = LocalDateTime.now();
        this.lamportTimestamp = 0;
        this.nodoOrigenId = -1;
    }

    // Getters originales
    public String getRemitente() { return remitente; }
    public String getContenido() { return contenido; }
    public Tipo getTipo() { return tipo; }
    public LocalDateTime getFechaEnvio() { return fechaEnvio; }

    // Getters/Setters nuevos
    public int getLamportTimestamp() { return lamportTimestamp; }
    public void setLamportTimestamp(int lamportTimestamp) { this.lamportTimestamp = lamportTimestamp; }
    public int getNodoOrigenId() { return nodoOrigenId; }
    public void setNodoOrigenId(int nodoOrigenId) { this.nodoOrigenId = nodoOrigenId; }

    public String getFechaFormateada() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
        return fechaEnvio.format(formatter);
    }

    @Override
    public String toString() {
        return "[" + getFechaFormateada() + "][L:" + lamportTimestamp + "] " + remitente + ": " + contenido;
    }
}
