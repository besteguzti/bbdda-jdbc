package com.unir.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class MySqlLocalidades {
    private int id_localidad;
    private int id_municipio;
    private String localidad;
    private String codigoPostal;
    private String longitud;
    private String latitud;
}
