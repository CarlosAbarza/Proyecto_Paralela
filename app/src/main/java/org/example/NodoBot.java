package org.example;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NodoBot {
    private static final String NOMBRE_BOT = "🤖 BotCentral";
    
    // Pool de hilos para procesar múltiples comandos en paralelo
    private static final ExecutorService poolComandos = Executors.newFixedThreadPool(10);
    
    // Memoria Thread-Safe para almacenar los comandos que los usuarios le enseñen
    private static final ConcurrentHashMap<String, String> memoriaComandos = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        try {
            Socket socket = new Socket("localhost", 5000);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            System.out.println(NOMBRE_BOT + " conectado (Motor Concurrente y Memoria Dinámica activados)...");

            while (true) {
                try {
                    PaqueteMensaje msj = (PaqueteMensaje) in.readObject();
                    
                    // Solo reacciona a comandos y evita procesar sus propios mensajes
                    if (msj.getTipo() == PaqueteMensaje.Tipo.COMANDO && !msj.getRemitente().equals(NOMBRE_BOT)) {
                        poolComandos.execute(new ProcesadorComando(msj, out));
                    }
                } catch (ClassNotFoundException e) {
                    System.err.println("Error procesando paquete en el Bot.");
                }
            }
        } catch (IOException e) {
            System.err.println("\n[AVISO] El Bot perdió conexión con el servidor principal. Cerrando proceso...");
            System.exit(0);
        }
    }

    // Tarea concurrente delegada al Pool de Hilos
    static class ProcesadorComando implements Runnable {
        private final PaqueteMensaje mensajeOriginal;
        private final ObjectOutputStream out;

        public ProcesadorComando(PaqueteMensaje msj, ObjectOutputStream out) {
            this.mensajeOriginal = msj;
            this.out = out;
        }

        @Override
        public void run() {
            String respuesta = generarRespuesta(mensajeOriginal.getContenido(), mensajeOriginal.getRemitente());
            
            if (respuesta != null) {
                // REGIÓN CRÍTICA: Protegemos el flujo de salida de red
                synchronized (out) {
                    try {
                        out.writeObject(new PaqueteMensaje(NOMBRE_BOT, respuesta, PaqueteMensaje.Tipo.TEXTO));
                        out.flush();
                    } catch (IOException e) {
                        System.err.println("Error al enviar respuesta del bot.");
                    }
                }
            }
        }

        private String generarRespuesta(String comandoCompleto, String usuario) {
            // Dividimos el comando en máximo 3 partes: [instrucción] [clave] [resto del mensaje]
            String[] partes = comandoCompleto.split(" ", 3);
            String instruccion = partes[0].toLowerCase();

            // 1. Lógica de Aprendizaje Dinámico
            if (instruccion.equals("/aprender")) {
                if (partes.length < 3) {
                    return "Uso incorrecto. Formato: /aprender /comando Mensaje de respuesta";
                }
                else { 
                    if (!partes[1].startsWith("/")) {
                        return "La clave del comando debe comenzar con '/'. Ejemplo: /aprender /saludo Hola!";
                    }
                }
                String nuevoComando = partes[1].toLowerCase();
                String respuestaDinamica = partes[2];
                
                memoriaComandos.put(nuevoComando, respuestaDinamica);
                return "Comando '" + nuevoComando + "' aprendido exitosamente gracias a " + usuario + ".";
            }

            // 2. Búsqueda en Memoria Dinámica
            if (memoriaComandos.containsKey(instruccion)) {
                return memoriaComandos.get(instruccion);
            }

            // 3. Comandos Fijos del Sistema
            switch (instruccion) {
                case "/ping":
                    return "Pong! Atendido rápidamente por el hilo: " + Thread.currentThread().getName();
                case "/hora":
                    return "La hora actual es: " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
                case "/pesado":
                    try { Thread.sleep(5000); } catch (InterruptedException ignored) {}
                    return "Proceso pesado de 3 segundos terminado para " + usuario + ".";
                case "/ayuda":
                    return "Fijos: /ping, /hora, /pesado, /aprender, /ayuda. También respondo a los comandos que me enseñen.";
                default:
                    return "Comando desconocido ('" + instruccion + "'). Escribe /ayuda o enséñame algo nuevo con /aprender.";
            }
        }
    }
}