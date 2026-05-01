package org.example;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class PaqueteMensaje implements Serializable {
    private static final long serialVersionUID = 2L;
    
    public enum Tipo { TEXTO, COMANDO }
    
    private String remitente;
    private String contenido;
    private LocalDateTime fechaEnvio;
    private Tipo tipo; 

    public PaqueteMensaje(String remitente, String contenido, Tipo tipo) {
        this.remitente = remitente;
        this.contenido = contenido;
        this.tipo = tipo;
        this.fechaEnvio = LocalDateTime.now();
    }

    public String getRemitente() { return remitente; }
    public String getContenido() { return contenido; }
    public Tipo getTipo() { return tipo; }
    
    public String getFechaFormateada() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
        return fechaEnvio.format(formatter);
    }
}