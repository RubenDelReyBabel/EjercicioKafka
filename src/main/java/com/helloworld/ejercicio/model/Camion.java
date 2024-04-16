package com.helloworld.ejercicio.model;

import java.util.Random;

public class Camion {

    private String id;
    private String matricula;
    private float km;
    private int velocidad;

    public Camion(String id, String matricula) {
        this.id = id;
        this.matricula = matricula;
        this.km = new Random().nextFloat(10, 20);
    }

    public String getId() {
        return id;
    }

    public String getMatricula(){
        return matricula;
    }

    public int getVelocidad(){
        return velocidad;
    }

    public float getKm(){
        return km;
    }

    public float getTimestamp(){
        return System.currentTimeMillis();
    }

    public void avanzar(){
        velocidad = new Random().nextInt(0, 120);
        km += velocidad / 3600 * 10;
    }
}