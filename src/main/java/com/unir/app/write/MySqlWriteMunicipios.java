package com.unir.app.write;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import com.unir.config.MySqlConnector;
import com.unir.model.MySqlLocalidades;
import com.unir.model.MySqlMunicipios;
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
public class MySqlWriteMunicipios {

    private static final String DATABASE = "laboratorio1";

    public static void main(String[] args) {

        try (Connection connection = new MySqlConnector("localhost", DATABASE).getConnection()) {

            log.warn("Recuerda que el fichero CSV debe estar en la raíz del proyecto, en la carpeta {}",
                    System.getProperty("user.dir"));
            log.info("Conexión establecida con la base de datos MySQL");

            // Leemos los datos del fichero CSV
            List<MySqlMunicipios> municipios = readData();

            // Introducimos los datos en la base de datos
            intake(connection, municipios);

        } catch (Exception e) {
            log.error("Error al tratar con la base de datos", e);
        }
    }

    private static List<MySqlMunicipios> readData() {

        try (CSVReader reader = new CSVReaderBuilder(
                new FileReader("municipios.csv"))
                .withCSVParser(
                        new CSVParserBuilder()
                                .withSeparator(';')
                                .build())
                .build()) {

            List<MySqlMunicipios> municipios = new LinkedList<>();
            reader.skip(1);
            String[] nextLine;

            while ((nextLine = reader.readNext()) != null) {
                MySqlMunicipios municipio = new MySqlMunicipios(
                        Integer.parseInt(nextLine[0]), // id_municipio
                        Integer.parseInt(nextLine[1]), // id_provincia
                        nextLine[2]);                    // municipio


                municipios.add(municipio);
            }
            return municipios;
        } catch (IOException | CsvValidationException e) {
            log.error("Error al leer el fichero CSV", e);
            throw new RuntimeException(e);
        }
    }

    private static void intake(Connection connection, List<MySqlMunicipios> municipios) throws SQLException {

        String selectSql = "SELECT COUNT(*) FROM municipios WHERE id_municipio = ?";
        String insertSql = "INSERT INTO municipios (id_municipio, id_provincia, municipio)"
                + "VALUES (?, ?, ?)";
        String updateSql = "UPDATE municipios SET id_municipio = ?, id_provincia = ?, municipio = ?";
        int batchSize = 5;
        int count = 0;

        try (PreparedStatement selectStatement = connection.prepareStatement(selectSql);
             PreparedStatement insertStatement = connection.prepareStatement(insertSql);
             PreparedStatement updateStatement = connection.prepareStatement(updateSql)) {

            connection.setAutoCommit(false);

            for (MySqlMunicipios municipio : municipios) {
                selectStatement.setInt(1, municipio.getId_municipio());
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    resultSet.next();
                    int rowCount = resultSet.getInt(1);

                    if (rowCount > 0) {
                        fillUpdateStatement(updateStatement, municipio);
                        updateStatement.addBatch();
                    } else {
                        fillInsertStatement(insertStatement, municipio);
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

    private static void fillInsertStatement(PreparedStatement statement, MySqlMunicipios municipio) throws SQLException {
        statement.setInt(1, municipio.getId_municipio());
        statement.setInt(2, municipio.getId_provincia());
        statement.setString(3, municipio.getMunicipio());

    }

    private static void fillUpdateStatement(PreparedStatement statement, MySqlMunicipios municipio) throws SQLException {
        statement.setInt(1, municipio.getId_municipio());
        statement.setInt(2, municipio.getId_provincia());
        statement.setString(3, municipio.getMunicipio());
    }
}

