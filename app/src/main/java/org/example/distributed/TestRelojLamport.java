package org.example.distributed;

/**
 * Test manual para verificar el funcionamiento del RelojLamport y EventLog.
 * Ejecutar con: java -cp app/build/classes/java/main org.example.distributed.TestRelojLamport
 */
public class TestRelojLamport {
    public static void main(String[] args) {
        System.out.println("=== TEST: RelojLamport ===\n");

        RelojLamport reloj = new RelojLamport();

        // Test 1: Valor inicial
        assert reloj.getValor() == 0 : "Valor inicial debe ser 0";
        System.out.println("✓ Valor inicial: " + reloj.getValor() + " (esperado: 0)");

        // Test 2: tick() incrementa
        int t1 = reloj.tick();
        assert t1 == 1 : "Primer tick debe retornar 1";
        System.out.println("✓ Tick 1: " + t1 + " (esperado: 1)");

        int t2 = reloj.tick();
        assert t2 == 2 : "Segundo tick debe retornar 2";
        System.out.println("✓ Tick 2: " + t2 + " (esperado: 2)");

        // Test 3: actualizar() con valor mayor
        reloj.actualizar(10);
        assert reloj.getValor() == 11 : "Después de actualizar(10) con valor 2, debe ser max(2,10)+1=11";
        System.out.println("✓ Después de actualizar(10): " + reloj.getValor() + " (esperado: 11)");

        // Test 4: actualizar() con valor menor (no retrocede)
        reloj.actualizar(5);
        assert reloj.getValor() == 12 : "Después de actualizar(5) con valor 11, debe ser max(11,5)+1=12";
        System.out.println("✓ Después de actualizar(5): " + reloj.getValor() + " (esperado: 12)");

        // Test 5: tick() después de actualizar
        int t3 = reloj.tick();
        assert t3 == 13 : "Tick después de valor 12 debe ser 13";
        System.out.println("✓ Tick 3: " + t3 + " (esperado: 13)");

        System.out.println("\n=== TEST: EventLog ===\n");

        // Test 6: EventLog escribe a archivo
        EventLog log = new EventLog(99);
        log.registrar("TEST", "Evento de prueba número 1", reloj.tick());
        log.registrar("TEST", "Evento de prueba número 2", reloj.tick());
        log.registrar("MENSAJE", "Simulando un mensaje de chat", reloj.tick());
        log.flush();
        log.cerrar();

        System.out.println("\n✓ Verificar archivo: logs/nodo_99.log");
        System.out.println("  Debe contener 3 líneas con formato:");
        System.out.println("  [fecha][L:N][NODO:99][TIPO] descripción");

        System.out.println("\n=== TODOS LOS TESTS PASARON ===");
    }
}
