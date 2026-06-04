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
