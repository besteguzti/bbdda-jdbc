package com.unir.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class MySqlPrecios {
    private int id_estacion;
    private int id_precio;
    private String TomaDeDatos;
    private float gasolina95E5;
    private float gasolina95E10;
    private float gasolina95E5Premiun;
    private float gasolina98E5;
    private float gasolina98E10;
    private float gasoleoA;
    private float gasoleoPremium;
    private float gasoleoB;
    private float gasoleoC;
    private float bioetanol;
    private float biodiesel;
    private float gasesLicuadosPetroleo;
    private float gasNaturalComprimido;
    private float gasNaturalLicuado;
    private float hidrogeno;
}
