package org.example.distributed;

import org.example.Config;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registro de membresía del cluster.
 * Mantiene el estado (ACTIVO/CAIDO/DESCONOCIDO) de cada nodo del cluster.
 */
public class MembresiaCluster {

    public enum EstadoNodo { ACTIVO, CAIDO, DESCONOCIDO }

    private final int nodoLocalId;
    private final Map<Integer, EstadoNodo> estados = new ConcurrentHashMap<>();

    public MembresiaCluster(int nodoLocalId) {
        this.nodoLocalId = nodoLocalId;
        for (int i = 1; i <= Config.NUM_NODOS; i++) {
            if (i == nodoLocalId) {
                estados.put(i, EstadoNodo.ACTIVO);
            } else {
                estados.put(i, EstadoNodo.DESCONOCIDO);
            }
        }
    }

    public void marcarActivo(int nodoId) { estados.put(nodoId, EstadoNodo.ACTIVO); }
    public void marcarCaido(int nodoId) { estados.put(nodoId, EstadoNodo.CAIDO); }
    public EstadoNodo getEstado(int nodoId) {
        return estados.getOrDefault(nodoId, EstadoNodo.DESCONOCIDO);
    }
    public boolean isActivo(int nodoId) { return getEstado(nodoId) == EstadoNodo.ACTIVO; }

    public List<Integer> getNodosActivos() {
        List<Integer> activos = new ArrayList<>();
        for (var entry : estados.entrySet()) {
            if (entry.getValue() == EstadoNodo.ACTIVO) {
                activos.add(entry.getKey());
            }
        }
        return activos;
    }

    public int getNodoLocalId() { return nodoLocalId; }

    @Override
    public String toString() {
        return "Membresía[nodoLocal=" + nodoLocalId + ", estados=" + estados + "]";
    }
}
