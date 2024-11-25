package com.unir.app.write;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import com.unir.config.MySqlConnector;
import lombok.extern.slf4j.Slf4j;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class MySqlWriteEstaciones {

    private static final String DATABASE = "laboratorio1";

    public static void main(String[] args) {

        try (Connection connection = new MySqlConnector("localhost", DATABASE).getConnection()) {

            log.warn("Recuerda que el fichero CSV debe estar en la raíz del proyecto, en la carpeta {}",
                    System.getProperty("user.dir"));
            log.info("Conexión establecida con la base de datos MySQL");

            // Leemos los datos del fichero CSV
            List<com.unir.model.MySqlEstaciones> estaciones = readData();

            // Introducimos los datos en la base de datos
            intake(connection, estaciones);

        } catch (Exception e) {
            log.error("Error al tratar con la base de datos", e);
        }
    }

    private static List<com.unir.model.MySqlEstaciones> readData() {

        try (CSVReader reader = new CSVReaderBuilder(
                new FileReader("estaciones.csv"))
                .withCSVParser(
                        new CSVParserBuilder()
                                .withSeparator(';')
                                .build())
                .build()) {

            List<com.unir.model.MySqlEstaciones> estaciones = new LinkedList<>();
            reader.skip(1);
            String[] nextLine;

            while ((nextLine = reader.readNext()) != null) {
                com.unir.model.MySqlEstaciones estacion = new com.unir.model.MySqlEstaciones(
                        Integer.parseInt(nextLine[0]), // id_estacion
                        Integer.parseInt(nextLine[1]), // id_localidad
                        nextLine[2],                  // direccion
                        nextLine[3],                  // margen
                        nextLine[4],                  // rotulo
                        nextLine[5],                  // tipoVenta
                        nextLine[6],                  // rem
                        nextLine[7],                  // horario
                        nextLine[8]);                 // tipoServicio
                estaciones.add(estacion);
            }
            return estaciones;
        } catch (IOException | CsvValidationException e) {
            log.error("Error al leer el fichero CSV", e);
            throw new RuntimeException(e);
        }
    }

    private static void intake(Connection connection, List<com.unir.model.MySqlEstaciones> estaciones) throws SQLException {

        String selectSql = "SELECT COUNT(*) FROM estaciones WHERE id_estacion = ?";
        String insertSql = "INSERT INTO estaciones (id_estacion, id_localidad, direccion, margen, rotulo, tipoVenta, rem, horario, tipoServicio) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String updateSql = "UPDATE estaciones SET id_localidad = ?, direccion = ?, margen = ?, rotulo = ?, tipoVenta = ?, rem = ?, horario = ?, tipoServicio = ? WHERE id_estacion = ?";
        int batchSize = 5;
        int count = 0;

        try (PreparedStatement selectStatement = connection.prepareStatement(selectSql);
             PreparedStatement insertStatement = connection.prepareStatement(insertSql);
             PreparedStatement updateStatement = connection.prepareStatement(updateSql)) {

            connection.setAutoCommit(false);

            for (com.unir.model.MySqlEstaciones estacion : estaciones) {
                selectStatement.setInt(1, estacion.getId_estacion());
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    resultSet.next();
                    int rowCount = resultSet.getInt(1);

                    if (rowCount > 0) {
                        fillUpdateStatement(updateStatement, estacion);
                        updateStatement.addBatch();
                    } else {
                        fillInsertStatement(insertStatement, estacion);
                        insertStatement.addBatch();
                    }
                }

                if (++count % batchSize == 0) {
                    updateStatement.executeBatch();
                    insertStatement.executeBatch();
                }
            }

            insertStatement.executeBatch();
            updateStatement.executeBatch();
            connection.commit();
            connection.setAutoCommit(true);
        }
    }

    private static void fillInsertStatement(PreparedStatement statement, com.unir.model.MySqlEstaciones estacion) throws SQLException {
        statement.setInt(1, estacion.getId_estacion());
        statement.setInt(2, estacion.getId_localidad());
        statement.setString(3, estacion.getDireccion());
        statement.setString(4, estacion.getMargen());
        statement.setString(5, estacion.getRotulo());
        statement.setString(6, estacion.getTipoVenta());
        statement.setString(7, estacion.getRem());
        statement.setString(8, estacion.getHorario());
        statement.setString(9, estacion.getTipoServicio());
    }

    private static void fillUpdateStatement(PreparedStatement statement, com.unir.model.MySqlEstaciones estacion) throws SQLException {
        statement.setInt(1, estacion.getId_localidad());
        statement.setString(2, estacion.getDireccion());
        statement.setString(3, estacion.getMargen());
        statement.setString(4, estacion.getRotulo());
        statement.setString(5, estacion.getTipoVenta());
        statement.setString(6, estacion.getRem());
        statement.setString(7, estacion.getHorario());
        statement.setString(8, estacion.getTipoServicio());
        statement.setInt(9, estacion.getId_estacion());
    }
}

