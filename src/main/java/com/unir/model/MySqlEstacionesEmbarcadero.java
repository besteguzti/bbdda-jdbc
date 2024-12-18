package com.unir.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class MySqlEstacionesEmbarcadero {
    private int id_localidad;
    private int id_estacion;
    private int id_precios;
    private String rotulo;
    private String tipoVenta;
    private String rem;
    private String horario;
}
