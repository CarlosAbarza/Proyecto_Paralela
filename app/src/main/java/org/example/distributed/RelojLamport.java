package org.example.distributed;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementación del reloj lógico de Lamport.
 * Thread-safe mediante AtomicInteger + CAS (compare-and-swap).
 *
 * Uso:
 *   RelojLamport reloj = new RelojLamport();
 *
 *   // Antes de enviar un mensaje:
 *   int marca = reloj.tick();
 *   mensaje.setLamportTimestamp(marca);
 *
 *   // Al recibir un mensaje:
 *   reloj.actualizar(mensaje.getLamportTimestamp());
 */
public class RelojLamport implements IRelojLamport {
    private final AtomicInteger contador;

    public RelojLamport() {
        this.contador = new AtomicInteger(0);
    }

    @Override
    public int tick() {
        return contador.incrementAndGet();
    }

    @Override
    public void actualizar(int timestampRecibido) {
        int actual;
        int nuevo;
        /*
         * CICLO CAS (COMPARE-AND-SWAP) PARA CONCURRENCIA SIN BLOQUEO:
         * - Qué hace: Garantiza la actualización segura y atómica del reloj lógico. Sigue la regla
         *   fundamental de Lamport: al recibir un mensaje con timestamp 'T', el reloj local debe
         *   avanzar a un valor mayor que el máximo entre su valor actual y 'T' (max(local, T) + 1).
         * - Por qué se hace: Para evitar el uso de bloques 'synchronized' pesados, se opta por una
         *   estrategia de concurrencia optimista sin bloqueo (lock-free) usando AtomicInteger.
         * - Con quién se comunica: Opera puramente en memoria local sobre la variable thread-safe 'contador'.
         * - De qué depende: De la instrucción hardware CAS expuesta por 'compareAndSet'.
         * - Manejo de errores: Si otro hilo modifica el contador entre el 'get()' y el 'compareAndSet()',
         *   el reemplazo falla (retorna false) y el ciclo 'do-while' vuelve a intentar transparentemente
         *   con el valor actualizado del contador, previniendo condiciones de carrera.
         */
        do {
            actual = contador.get();
            nuevo = Math.max(actual, timestampRecibido) + 1;
        } while (!contador.compareAndSet(actual, nuevo));
    }

    @Override
    public int getValor() {
        return contador.get();
    }
}
