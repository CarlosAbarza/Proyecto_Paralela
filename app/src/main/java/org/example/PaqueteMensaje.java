package org.example;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Clase que representa la estructura de datos compleja requerida para el Marshalling.
 * Incluye remitente, contenido y marca de tiempo para orden cronológico.
 */
public class PaqueteMensaje implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String remitente;
    private String contenido;
    private LocalDateTime fechaEnvio;

    public PaqueteMensaje(String remitente, String contenido) {
        this.remitente = remitente;
        this.contenido = contenido;
        this.fechaEnvio = LocalDateTime.now();
    }

    public String getRemitente() { return remitente; }
    public String getContenido() { return contenido; }
    
    public String getFechaFormateada() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
        return fechaEnvio.format(formatter);
    }
}