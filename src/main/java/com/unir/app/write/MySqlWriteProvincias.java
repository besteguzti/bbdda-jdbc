package com.unir.app.write;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import com.unir.config.MySqlConnector;
import com.unir.model.MySqlMunicipios;
import com.unir.model.MySqlProvincias;
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
public class MySqlWriteProvincias {

    private static final String DATABASE = "laboratorio1";

    public static void main(String[] args) {

        try (Connection connection = new MySqlConnector("localhost", DATABASE).getConnection()) {

            log.warn("Recuerda que el fichero CSV debe estar en la raíz del proyecto, en la carpeta {}",
                    System.getProperty("user.dir"));
            log.info("Conexión establecida con la base de datos MySQL");

            // Leemos los datos del fichero CSV
            List<MySqlProvincias> provincias = readData();

            // Introducimos los datos en la base de datos
            intake(connection, provincias);

        } catch (Exception e) {
            log.error("Error al tratar con la base de datos", e);
        }
    }

    private static List<MySqlProvincias> readData() {

        try (CSVReader reader = new CSVReaderBuilder(
                new FileReader("provincias.csv"))
                .withCSVParser(
                        new CSVParserBuilder()
                                .withSeparator(';')
                                .build())
                .build()) {

            List<MySqlProvincias> provincias = new LinkedList<>();
            reader.skip(1);
            String[] nextLine;

            while ((nextLine = reader.readNext()) != null) {
                MySqlProvincias provincia = new MySqlProvincias(
                        Integer.parseInt(nextLine[0]), // id_provincia
                        nextLine[1]);                    // provincia


                provincias.add(provincia);
            }
            return provincias;
        } catch (IOException | CsvValidationException e) {
            log.error("Error al leer el fichero CSV", e);
            throw new RuntimeException(e);
        }
    }

    private static void intake(Connection connection, List<MySqlProvincias> provincias) throws SQLException {

        String selectSql = "SELECT COUNT(*) FROM provincias WHERE id_provincia = ?";
        String insertSql = "INSERT INTO provincias (id_provincia, provincia)"
                + "VALUES (?, ?)";
        String updateSql = "UPDATE provincias SET id_provincia = ?, provincia = ?";
        int batchSize = 5;
        int count = 0;

        try (PreparedStatement selectStatement = connection.prepareStatement(selectSql);
             PreparedStatement insertStatement = connection.prepareStatement(insertSql);
             PreparedStatement updateStatement = connection.prepareStatement(updateSql)) {

            connection.setAutoCommit(false);

            for (MySqlProvincias provincia : provincias) {
                selectStatement.setInt(1, provincia.getId_provincia());
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    resultSet.next();
                    int rowCount = resultSet.getInt(1);

                    if (rowCount > 0) {
                        fillUpdateStatement(updateStatement, provincia);
                        updateStatement.addBatch();
                    } else {
                        fillInsertStatement(insertStatement, provincia);
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

    private static void fillInsertStatement(PreparedStatement statement, MySqlProvincias provincia) throws SQLException {
        statement.setInt(1, provincia.getId_provincia());
        statement.setString(2, provincia.getProvincia());

    }

    private static void fillUpdateStatement(PreparedStatement statement, MySqlProvincias provincia) throws SQLException {
        statement.setInt(1, provincia.getId_provincia());
        statement.setString(2, provincia.getProvincia());
    }
}

