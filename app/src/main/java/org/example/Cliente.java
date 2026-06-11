package org.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class Cliente {
    private static String miNombre;
    private static volatile ObjectOutputStream outStream = null;
    private static final Object lockConexion = new Object();
    private static Thread threadEnvio = null;

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int puertoInicial = args.length > 1 ? Integer.parseInt(args[1]) : -1;
        if (args.length > 2) {
            try {
                int totalNodos = Integer.parseInt(args[2]);
                if (totalNodos < 1 || totalNodos >= 1000) {
                    System.err.println("Error: El número total de nodos debe estar en el rango [1, 999]. Recibido: " + totalNodos);
                    System.exit(1);
                }
                Config.NUM_NODOS = totalNodos;
            } catch (NumberFormatException e) {
                System.err.println("Error: totalNodos debe ser un número entero. Recibido: '" + args[2] + "'");
                System.exit(1);
            }
        }

        int nodoActual = -1;
        if (puertoInicial != -1) {
            for (int i = 1; i <= Config.NUM_NODOS; i++) {
                if (Config.getPuertoClientes(i) == puertoInicial) {
                    nodoActual = i;
                    break;
                }
            }
        } else {
            // Balanceador de carga: elegir un nodo inicial al azar para distribuir las conexiones en el clúster
            nodoActual = new java.util.Random().nextInt(Config.NUM_NODOS) + 1;
            System.out.println("[BALANCEADOR] Seleccionando de forma aleatoria el Nodo " + nodoActual + " para distribuir la carga.");
        }

        BufferedReader teclado = new BufferedReader(new InputStreamReader(System.in));
        boolean primerIntento = true;
        int intentosFallidosConsecutivos = 0;
        int maxIntentos = 2 * Config.NUM_NODOS;

        /*
         * CICLO PRINCIPAL DE CONEXIÓN Y REINTENTOS DEL CLIENTE:
         * - Qué hace: Intenta establecer conexión con uno de los servidores del clúster de forma circular.
         * - Con quién se comunica: Con el NodoServidor correspondiente al puerto de clientes seleccionado.
         * - De qué depende: De la disponibilidad de red, del host y de que al menos un servidor del clúster esté levantado.
         * - Manejo de errores y reconexión: Si ocurre un fallo al conectar o durante la sesión activa (IOException),
         *   se cierra el socket localmente, se incrementa el contador de fallos consecutivos y se duerme el hilo 3 segundos.
         *   Luego, cambia el puerto para apuntar al siguiente nodo del clúster (de forma circular 1 -> 2 -> 3 -> 1)
         *   y vuelve a intentar. Si se superan 'maxIntentos' (2 vueltas completas al clúster sin éxito), se asume que
         *   todo el clúster está caído y el programa finaliza.
         */
        while (true) {
            int puerto;
            if (primerIntento && nodoActual == -1) {
                puerto = puertoInicial;
            } else {
                if (nodoActual == -1) {
                    nodoActual = 1;
                }
                puerto = Config.getPuertoClientes(nodoActual);
            }

            Socket socket = null;
            ObjectInputStream in = null;

            try {
                System.out.println("[CONEXIÓN] Intentando conectar a " + host + ":" + puerto + 
                        (nodoActual != -1 ? " (Nodo " + nodoActual + ")" : "") + "...");
                socket = new Socket(host, puerto);
                
                ObjectOutputStream tempOut = new ObjectOutputStream(socket.getOutputStream());
                tempOut.flush();

                in = new ObjectInputStream(socket.getInputStream());

                enviarAuth(tempOut);

                if (miNombre == null) {
                    miNombre = pedirNombre(teclado);
                }
                enviarLogin(tempOut, miNombre);

                synchronized (lockConexion) {
                    outStream = tempOut;
                }

                System.out.println("Conectado al servidor " + host + ":" + puerto);
                System.out.println("Comandos disponibles: /usuarios, /historial, /ping, /hora, /pesado, /ayuda");
                System.out.println("Escribe /salir para cerrar el cliente.");

                if (threadEnvio == null) {
                    threadEnvio = new Thread(new HiloEnvio(miNombre, teclado));
                    threadEnvio.start();
                }

                primerIntento = false;
                intentosFallidosConsecutivos = 0; // Resetear al conectar exitosamente

                /*
                 * CICLO DE LECTURA DE MENSAJES DEL SERVIDOR:
                 * - Qué hace: Lee de forma bloqueante y continua los mensajes que difunde el servidor.
                 * - Con quién se comunica: Con el NodoServidor al que está conectado.
                 * - De qué depende: De que la conexión TCP establecida con el servidor permanezca activa.
                 * - Manejo de errores: Si falla la deserialización, se muestra un aviso por consola y continúa.
                 *   Si ocurre una IOException (desconexión del servidor), se propaga al try externo para manejar
                 *   el proceso de reconexión.
                 */
                while (true) {
                    try {
                        PaqueteMensaje mensaje = (PaqueteMensaje) in.readObject();

                        String nombreAMostrar = mensaje.getRemitente().equals(miNombre)
                                ? "Tú"
                                : mensaje.getRemitente();

                        System.out.println("[" + mensaje.getFechaFormateada() + "][L:" + mensaje.getLamportTimestamp() + "] "
                                + nombreAMostrar + ": " + mensaje.getContenido());

                    } catch (ClassNotFoundException e) {
                        System.err.println("[ERROR] Error al deserializar el paquete recibido.");
                    }
                }

            } catch (IOException e) {
                System.err.println("[CONEXIÓN] Conexión perdida o fallida con el servidor en " + host + ":" + puerto + ".");
                
                synchronized (lockConexion) {
                    outStream = null;
                }

                try {
                    if (in != null) in.close();
                    if (socket != null) socket.close();
                } catch (IOException ex) {
                    // ignorar
                }

                intentosFallidosConsecutivos++;
                if (intentosFallidosConsecutivos >= maxIntentos) {
                    System.err.println("[CONEXIÓN] Todos los servidores del clúster están caídos tras " + maxIntentos + " intentos (2 vueltas completas). Cerrando cliente...");
                    System.exit(1);
                }

                if (nodoActual != -1) {
                    nodoActual = (nodoActual % Config.NUM_NODOS) + 1;
                } else {
                    nodoActual = 1;
                }

                primerIntento = false;

                System.err.println("[CONEXIÓN] Intentando reconectar al próximo servidor en 3 segundos...");
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private static void enviarAuth(ObjectOutputStream out) throws IOException {
        PaqueteMensaje auth = new PaqueteMensaje(
                "auth",
                Config.TOKEN_VALIDO,
                PaqueteMensaje.Tipo.AUTH
        );

        out.writeObject(auth);
        out.flush();
    }

    private static void enviarLogin(ObjectOutputStream out, String nombre) throws IOException {
        PaqueteMensaje login = new PaqueteMensaje(
                nombre,
                "LOGIN",
                PaqueteMensaje.Tipo.LOGIN
        );

        out.writeObject(login);
        out.flush();
    }

    private static String pedirNombre(BufferedReader teclado) throws IOException {
        while (true) {
            System.out.print("Nombre de usuario: ");
            String nombre = teclado.readLine();

            if (nombre != null && nombre.matches("[a-zA-Z0-9_]{3,20}")) {
                return nombre;
            }

            System.out.println("[ERROR] Nombre inválido. Usa entre 3 y 20 caracteres: letras, números o guion bajo.");
        }
    }

    static class HiloEnvio implements Runnable {
        private final String nombre;
        private final BufferedReader teclado;

        public HiloEnvio(String nombre, BufferedReader teclado) {
            this.nombre = nombre;
            this.teclado = teclado;
        }

        @Override
        public void run() {
            /*
             * CICLO DE LECTURA DE TECLADO Y ENVÍO DE MENSAJES:
             * - Qué hace: Lee de forma continua los comandos y textos escritos por el usuario en la terminal y los envía al servidor.
             * - Con quién se comunica: Con el usuario a través de la entrada estándar, y con el servidor remoto a través de 'outStream'.
             * - De qué depende: De la entrada de la terminal y del estado de 'outStream' (que no sea null).
             * - Manejo de errores: Si ocurre un error de escritura (IOException) o si 'outStream' es nulo debido a una desconexión en proceso,
             *   muestra un mensaje de error y el ciclo continúa esperando que el ciclo de conexión principal restablezca 'outStream'.
             */
            while (true) {
                try {
                    String texto = teclado.readLine();

                    if (texto == null) {
                        continue;
                    }

                    texto = texto.trim();

                    if (texto.isEmpty()) {
                        continue;
                    }

                    if (texto.equalsIgnoreCase("/salir")) {
                        System.out.println("Cerrando cliente...");
                        System.exit(0);
                    }

                    if (texto.length() > Config.MAX_MENSAJE) {
                        System.out.println("[ERROR] Mensaje excede el límite de " + Config.MAX_MENSAJE + " caracteres.");
                        continue;
                    }

                    System.out.print("\033[1A\033[2K");

                    PaqueteMensaje.Tipo tipo = texto.startsWith("/")
                            ? PaqueteMensaje.Tipo.COMANDO
                            : PaqueteMensaje.Tipo.TEXTO;

                    PaqueteMensaje mensaje = new PaqueteMensaje(nombre, texto, tipo);

                    synchronized (lockConexion) {
                        if (outStream != null) {
                            outStream.writeObject(mensaje);
                            outStream.flush();
                        } else {
                            System.err.println("[CONEXIÓN] No estás conectado a ningún servidor. Esperando reconexión...");
                        }
                    }

                } catch (IOException e) {
                    System.err.println("[CONEXIÓN] Error al enviar mensaje (¿servidor caído?).");
                }
            }
        }
    }
}
