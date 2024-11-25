package com.unir.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class MySqlEstaciones {
    private int id_localidad;
    private int id_estacion;
    private String direccion;
    private String margen;
    private String rotulo;
    private String tipoVenta;
    private String rem;
    private String horario;
    private String tipoServicio;
}
