package com.unir.app.write;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import com.unir.config.MySqlConnector;
import com.unir.model.MySqlEmployee;
import lombok.extern.slf4j.Slf4j;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;

/**
 * La version para Oracle seria muy similar a esta, cambiando únicamente el Driver y los datos de sentencias.
 * La tabla de Oracle contiene muchas restricciones y triggers. Por simplicidad, usamos MySQL en este caso.
 */
@Slf4j
public class MySqlApplication {

    private static final String DATABASE = "laboratorio1";

    public static void main(String[] args) {

        //Creamos conexion. No es necesario indicar puerto en host si usamos el default, 1521
        //Try-with-resources. Se cierra la conexión automáticamente al salir del bloque try
        try(Connection connection = new MySqlConnector("localhost", DATABASE).getConnection()) {

            log.warn("Recuerda que el fichero csv debe estar en la raíz del proyecto, es decir, en la carpeta {}"
                    , System.getProperty("user.dir"));
            log.info("Conexión establecida con la base de datos MySQL");

            // Leemos los datos del fichero CSV
            List<MySqlEmployee> employees = readData();

            // Introducimos los datos en la base de datos
            intake(connection, employees);

        } catch (Exception e) {
            log.error("Error al tratar con la base de datos", e);
        }
    }

    /**
     * Lee los datos del fichero CSV y los devuelve en una lista de empleados.
     * El fichero CSV debe estar en la raíz del proyecto.
     *
     * @return - Lista de empleados
     */
    private static List<MySqlEmployee> readData() {

        // Try-with-resources. Se cierra el reader automáticamente al salir del bloque try
        // CSVReader nos permite leer el fichero CSV linea a linea
        try (CSVReader reader = new CSVReaderBuilder(
                new FileReader("unirEmployees.csv"))
                .withCSVParser(
                        new CSVParserBuilder()
                                .withSeparator(',')
                                .build())
                .build()) {

            // Creamos la lista de empleados y el formato de fecha
            List<MySqlEmployee> employees = new LinkedList<>();
            SimpleDateFormat format = new SimpleDateFormat("yyy-MM-dd");

            // Saltamos la primera linea, que contiene los nombres de las columnas del CSV
            reader.skip(1);
            String[] nextLine;

            // Leemos el fichero linea a linea
            while((nextLine = reader.readNext()) != null) {

                // Creamos el empleado y lo añadimos a la lista
                MySqlEmployee employee = new MySqlEmployee(
                        Integer.parseInt(nextLine[0]),
                        nextLine[1],
                        nextLine[2],
                        nextLine[3],
                        new Date(format.parse(nextLine[4]).getTime()),
                        new Date(format.parse(nextLine[5]).getTime())
                );
                employees.add(employee);
            }
            return employees;
        } catch (IOException e) {
            log.error("Error al leer el fichero CSV", e);
            throw new RuntimeException(e);
        } catch (CsvValidationException | ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Introduce los datos en la base de datos.
     * Si el empleado ya existe, se actualiza.
     * Si no existe, se inserta.
     *
     * Toma como referencia el campo emp_no para determinar si el empleado existe o no.
     * @param connection - Conexión a la base de datos
     * @param employees - Lista de empleados
     * @throws SQLException - Error al ejecutar la consulta
     */
    private static void intake(Connection connection, List<MySqlEmployee> employees) throws SQLException {

        String selectSql = "SELECT COUNT(*) FROM employees WHERE emp_no = ?";
        String insertSql = "INSERT INTO employees (emp_no, first_name, last_name, gender, hire_date, birth_date) "
                + "VALUES (?, ?, ?, ?, ?, ?)";
        String updateSql = "UPDATE employees SET first_name = ?, last_name = ?, gender = ?, hire_date = ?, birth_date = ? WHERE emp_no = ?";
        int lote = 5;
        int contador = 0;

        // Preparamos las consultas, una unica vez para poder reutilizarlas en el batch
        PreparedStatement insertStatement = connection.prepareStatement(insertSql);
        PreparedStatement updateStatement = connection.prepareStatement(updateSql);

        // Desactivamos el autocommit para poder ejecutar el batch y hacer commit al final
        connection.setAutoCommit(false);

        for (MySqlEmployee employee : employees) {

            // Comprobamos si el empleado existe
            PreparedStatement selectStatement = connection.prepareStatement(selectSql);
            selectStatement.setInt(1, employee.getEmployeeId()); // Código del empleado
            ResultSet resultSet = selectStatement.executeQuery();
            resultSet.next(); // Nos movemos a la primera fila
            int rowCount = resultSet.getInt(1);

            // Si existe, actualizamos. Si no, insertamos
            if(rowCount > 0) {
                fillUpdateStatement(updateStatement, employee);
                updateStatement.addBatch();
            } else {
                fillInsertStatement(insertStatement, employee);
                insertStatement.addBatch();
            }

            // Ejecutamos el batch cada lote de registros
            if (++contador % lote == 0) {
                updateStatement.executeBatch();
                insertStatement.executeBatch();
            }
        }

        // Ejecutamos el batch final
        insertStatement.executeBatch();
        updateStatement.executeBatch();

        /**
         * Para probar en modo DEBUG
         * Hasta que no se hace commit, los cambios no se reflejan en la base de datos
         * Es decir, si alguien consulta la base de datos antes de que se ejecute connection.commit(), no verá los cambios
         * Haz la prueba. Modifica el archivo CSV e incluye un nuevo empleado. Copia la ultima linea y cambia el nombre del empleado (pon algo que sea unico). Pon emp_no 99
         * Pon un breakpoint en connection.commit() y ejecuta el programa en modo debug.
         * Abre DataGrip u otro cliente de base de datos y ejecuta la consulta SELECT * FROM employees. Verás que el nuevo empleado no aparece.
         *
         * Descomenta el siguiente codigo para probarlo.
         * Veras que, tras ejecutarse los batch, el empleado con emp_no 99 si existe en esta conexion contra la DB.
         * Sin embargo, si ejecutas la consulta SELECT * FROM employees en DataGrip, no verás a ese empleado aun.
         */
        //PreparedStatement selectStatement = connection.prepareStatement(selectSql);
        //selectStatement.setInt(1, 99); // Código del empleado
        //ResultSet resultSet = selectStatement.executeQuery();
        //resultSet.next(); // Nos movemos a la primera fila
        //int rowCount = resultSet.getInt(1);
        //log.debug("El empleado con emp_no 99 existe en esta conexion contra la DB? {}", rowCount > 0);


        // Hacemos commit y volvemos a activar el autocommit
        connection.commit();
        connection.setAutoCommit(true);
    }

    /**
     * Rellena los parámetros de un PreparedStatement para una consulta INSERT.
     *
     * @param statement - PreparedStatement
     * @param employee - Empleado
     * @throws SQLException - Error al rellenar los parámetros
     */
    private static void fillInsertStatement(PreparedStatement statement, MySqlEmployee employee) throws SQLException {
        statement.setInt(1, employee.getEmployeeId());
        statement.setString(2, employee.getFirstName());
        statement.setString(3, employee.getLastName());
        statement.setString(4, employee.getGender());
        statement.setDate(5, employee.getHireDate());
        statement.setDate(6, employee.getBirthDate());

    }

    /**
     * Rellena los parámetros de un PreparedStatement para una consulta UPDATE.
     *
     * @param statement - PreparedStatement
     * @param employee - Empleado
     * @throws SQLException - Error al rellenar los parámetros
     */
    private static void fillUpdateStatement(PreparedStatement statement, MySqlEmployee employee) throws SQLException {
        statement.setString(1, employee.getFirstName());
        statement.setString(2, employee.getLastName());
        statement.setString(3, employee.getGender());
        statement.setDate(4, employee.getHireDate());
        statement.setDate(5, employee.getBirthDate());
        statement.setInt(6, employee.getEmployeeId());
    }

    /**
     * Devuelve el último id de una columna de una tabla.
     * Util para obtener el siguiente id a insertar.
     *
     * @param connection - Conexión a la base de datos
     * @param table - Nombre de la tabla
     * @param fieldName - Nombre de la columna
     * @return - Último id de la columna
     * @throws SQLException - Error al ejecutar la consulta
     */
    private static int lastId(Connection connection, String table, String fieldName) throws SQLException {
        String selectSql = "SELECT MAX(?) FROM ?";
        PreparedStatement selectStatement = connection.prepareStatement(selectSql);
        selectStatement.setString(1, fieldName);
        selectStatement.setString(2, table);
        ResultSet resultSet = selectStatement.executeQuery();
        resultSet.next(); // Nos movemos a la primera fila
        return resultSet.getInt(1);
    }
}
