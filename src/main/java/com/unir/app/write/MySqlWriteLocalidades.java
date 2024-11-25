package com.unir.app.write;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import com.unir.config.MySqlConnector;
import com.unir.model.MySqlEstaciones;
import com.unir.model.MySqlLocalidades;
import lombok.extern.slf4j.Slf4j;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class MySqlWriteLocalidades {

    private static final String DATABASE = "laboratorio1";

    public static void main(String[] args) {

        try (Connection connection = new MySqlConnector("localhost", DATABASE).getConnection()) {

            log.warn("Recuerda que el fichero CSV debe estar en la raíz del proyecto, en la carpeta {}",
                    System.getProperty("user.dir"));
            log.info("Conexión establecida con la base de datos MySQL");

            // Leemos los datos del fichero CSV
            List<com.unir.model.MySqlLocalidades> localidades = readData();

            // Introducimos los datos en la base de datos
            intake(connection, localidades);

        } catch (Exception e) {
            log.error("Error al tratar con la base de datos", e);
        }
    }

    private static List<com.unir.model.MySqlLocalidades> readData() {

        try (CSVReader reader = new CSVReaderBuilder(
                new FileReader("localidades.csv"))
                .withCSVParser(
                        new CSVParserBuilder()
                                .withSeparator(';')
                                .build())
                .build()) {

            List<com.unir.model.MySqlLocalidades> localidades = new LinkedList<>();
            reader.skip(1);
            String[] nextLine;

            while ((nextLine = reader.readNext()) != null) {
                MySqlLocalidades localidad = new MySqlLocalidades(
                        Integer.parseInt(nextLine[0]), // id_localidad
                        Integer.parseInt(nextLine[1]), // id_municipio
                        nextLine[2],                    // localidad
                        nextLine[3],                  // codigoPostal
                        nextLine[4],                  // longitud
                        nextLine[5]);                  // latitud

                localidades.add(localidad);
            }
            return localidades;
        } catch (IOException | CsvValidationException e) {
            log.error("Error al leer el fichero CSV", e);
            throw new RuntimeException(e);
        }
    }

    private static void intake(Connection connection, List<MySqlLocalidades> localidades) throws SQLException {

        String selectSql = "SELECT COUNT(*) FROM localidades WHERE id_localidad = ?";
        String insertSql = "INSERT INTO localidades (id_localidad, id_municipio, localidad, codigoPostal, longitud, latitud)"
                + "VALUES (?, ?, ?, ?, ?, ?)";
        String updateSql = "UPDATE localidades SET id_localidad = ?, id_municipio = ?, localidad = ?, codigoPostal = ?, longitud = ?, latitud = ?";
        int batchSize = 5;
        int count = 0;

        try (PreparedStatement selectStatement = connection.prepareStatement(selectSql);
             PreparedStatement insertStatement = connection.prepareStatement(insertSql);
             PreparedStatement updateStatement = connection.prepareStatement(updateSql)) {

            connection.setAutoCommit(false);

            for (MySqlLocalidades localidad : localidades) {
                selectStatement.setInt(1, localidad.getId_localidad());
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    resultSet.next();
                    int rowCount = resultSet.getInt(1);

                    if (rowCount > 0) {
                        fillUpdateStatement(updateStatement, localidad);
                        updateStatement.addBatch();
                    } else {
                        fillInsertStatement(insertStatement, localidad);
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

    private static void fillInsertStatement(PreparedStatement statement, MySqlLocalidades localidad) throws SQLException {
        statement.setInt(1, localidad.getId_localidad());
        statement.setInt(2, localidad.getId_municipio());
        statement.setString(3, localidad.getLocalidad());
        statement.setString(4, localidad.getCodigoPostal());
        statement.setString(5, localidad.getLongitud());
        statement.setString(6, localidad.getLatitud());

    }

    private static void fillUpdateStatement(PreparedStatement statement, MySqlLocalidades localidad) throws SQLException {
        statement.setInt(1, localidad.getId_localidad());
        statement.setInt(2, localidad.getId_municipio());
        statement.setString(3, localidad.getLocalidad());
        statement.setString(4, localidad.getCodigoPostal());
        statement.setString(5, localidad.getLongitud());
        statement.setString(6, localidad.getLatitud());
    }
}

