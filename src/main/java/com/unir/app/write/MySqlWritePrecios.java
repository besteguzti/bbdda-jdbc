package com.unir.app.write;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import com.unir.config.MySqlConnector;
import com.unir.model.MySqlPrecios;
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
public class MySqlWritePrecios {

    private static final String DATABASE = "laboratorio1";

    public static void main(String[] args) {

        try (Connection connection = new MySqlConnector("localhost", DATABASE).getConnection()) {

            log.warn("Recuerda que el fichero CSV debe estar en la raíz del proyecto, en la carpeta {}",
                    System.getProperty("user.dir"));
            log.info("Conexión establecida con la base de datos MySQL");

            // Leemos los datos del fichero CSV
            List<MySqlPrecios> precios = readData();

            // Introducimos los datos en la base de datos
            intake(connection, precios);

        } catch (Exception e) {
            log.error("Error al tratar con la base de datos", e);
        }
    }

    private static List<MySqlPrecios> readData() {

        try (CSVReader reader = new CSVReaderBuilder(
                new FileReader("precios.csv"))
                .withCSVParser(
                        new CSVParserBuilder()
                                .withSeparator(';')
                                .build())
                .build()) {

            List<MySqlPrecios> precios = new LinkedList<>();
            reader.skip(1);
            String[] nextLine;

            while ((nextLine = reader.readNext()) != null) {
                MySqlPrecios precio = new MySqlPrecios(
                        Integer.parseInt(nextLine[0]), // id_precio
                        Integer.parseInt(nextLine[1]), // id_provincia
                        nextLine[2],
                       Float.parseFloat(nextLine[3]),
                        Float.parseFloat(nextLine[4]),
                        Float.parseFloat(nextLine[5]),
                        Float.parseFloat(nextLine[6]),
                        Float.parseFloat(nextLine[7]),
                        Float.parseFloat(nextLine[8]),
                        Float.parseFloat(nextLine[9]),
                        Float.parseFloat(nextLine[10]),
                        Float.parseFloat(nextLine[11]),
                        Float.parseFloat(nextLine[12]),
                        Float.parseFloat(nextLine[13]),
                        Float.parseFloat(nextLine[14]),
                        Float.parseFloat(nextLine[15]),
                        Float.parseFloat(nextLine[16]),
                        Float.parseFloat(nextLine[17])
                        );




                precios.add(precio);
            }
            return precios;
        } catch (IOException | CsvValidationException e) {
            log.error("Error al leer el fichero CSV", e);
            throw new RuntimeException(e);
        }
    }

    private static void intake(Connection connection, List<MySqlPrecios> precios) throws SQLException {

        String selectSql = "SELECT COUNT(*) FROM precios WHERE id_precio = ?";
        String insertSql = "INSERT INTO precios (id_precio, id_estacion, TomaDeDatos, gasolina95E5, gasolina95E10, gasolina95E5Premiun, gasolina98E5, gasolina98E10, gasoleoA, gasoleoPremium, gasoleoB, gasoleoC, bioetanol, biodiesel, gasesLicuadosPetroleo, gasNaturalComprimido, gasNaturalLicuado, hidrogeno)"
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String updateSql = "UPDATE precios SET id_precio = ?, id_estacion = ?, TomaDeDatos = ?, gasolina95E5 = ?, gasolina95E10 = ?, gasolina95E5Premiun = ?, gasolina98E5 = ?, gasolina98E10 = ?, gasoleoA = ?, gasoleoPremium = ?, gasoleoB = ?, gasoleoC = ?, bioetanol = ?, biodiesel = ?, gasesLicuadosPetroleo = ?, gasNaturalComprimido = ?, gasNaturalLicuado = ?, hidrogeno = ?";
        int batchSize = 5;
        int count = 0;

        try (PreparedStatement selectStatement = connection.prepareStatement(selectSql);
             PreparedStatement insertStatement = connection.prepareStatement(insertSql);
             PreparedStatement updateStatement = connection.prepareStatement(updateSql)) {

            connection.setAutoCommit(false);

            for (MySqlPrecios precio : precios) {
                selectStatement.setInt(1, precio.getId_precio());
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    resultSet.next();
                    int rowCount = resultSet.getInt(1);

                    if (rowCount > 0) {
                        fillUpdateStatement(updateStatement, precio);
                        updateStatement.addBatch();
                    } else {
                        fillInsertStatement(insertStatement, precio);
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

    private static void fillInsertStatement(PreparedStatement statement, MySqlPrecios precio) throws SQLException {
        statement.setInt(1, precio.getId_precio());
        statement.setInt(2, precio.getId_estacion());
        statement.setString(3, precio.getTomaDeDatos());
        statement.setFloat(4, precio.getGasolina95E5());
        statement.setFloat(5, precio.getGasolina95E10());
        statement.setFloat(6, precio.getGasolina95E5Premiun());
        statement.setFloat(7, precio.getGasolina98E5());
        statement.setFloat(8, precio.getGasolina98E10());
        statement.setFloat(9, precio.getGasoleoA());
        statement.setFloat(10, precio.getGasoleoPremium());
        statement.setFloat(11, precio.getGasoleoB());
        statement.setFloat(12, precio.getGasoleoC());
        statement.setFloat(13, precio.getBioetanol());
        statement.setFloat(14, precio.getBiodiesel());
        statement.setFloat(15, precio.getGasesLicuadosPetroleo());
        statement.setFloat(16, precio.getGasNaturalComprimido());
        statement.setFloat(17, precio.getGasNaturalComprimido());
        statement.setFloat(18, precio.getHidrogeno());
    }

    private static void fillUpdateStatement(PreparedStatement statement, MySqlPrecios precio) throws SQLException {
        statement.setInt(1, precio.getId_precio());
        statement.setInt(2, precio.getId_estacion());
        statement.setString(3, precio.getTomaDeDatos());
        statement.setFloat(4, precio.getGasolina95E5());
        statement.setFloat(5, precio.getGasolina95E10());
        statement.setFloat(6, precio.getGasolina95E5Premiun());
        statement.setFloat(7, precio.getGasolina98E5());
        statement.setFloat(8, precio.getGasolina98E10());
        statement.setFloat(9, precio.getGasoleoA());
        statement.setFloat(10, precio.getGasoleoPremium());
        statement.setFloat(11, precio.getGasoleoB());
        statement.setFloat(12, precio.getGasoleoC());
        statement.setFloat(13, precio.getBioetanol());
        statement.setFloat(14, precio.getBiodiesel());
        statement.setFloat(15, precio.getGasesLicuadosPetroleo());
        statement.setFloat(16, precio.getGasNaturalComprimido());
        statement.setFloat(17, precio.getGasNaturalComprimido());
        statement.setFloat(18, precio.getHidrogeno());
    }
}

