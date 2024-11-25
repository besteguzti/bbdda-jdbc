package com.unir.app.read;

import com.unir.config.MySqlConnector;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;

@Slf4j
public class MySqlApplication {

    private static final String DATABASE = "employees";

    public static void main(String[] args) {

        //Creamos conexion. No es necesario indicar puerto en host si usamos el default, 1521
        //Try-with-resources. Se cierra la conexión automáticamente al salir del bloque try
        try(Connection connection = new MySqlConnector("localhost", DATABASE).getConnection()) {

            log.info("Conexión establecida con la base de datos MySQL");

            //selectAllEmployeesOfDepartment(connection, "d001");
            //selectAllEmployeesOfDepartment(connection, "d002");
            //employeesGender(connection);
            //mejorSalarioDpto(connection,"d004");
            //mejor2SalarioDpto(connection,"d004");
            numeroEmpleadosContratados(connection,11);

        } catch (Exception e) {
            log.error("Error al tratar con la base de datos", e);
        }
    }

    /**
     * Ejemplo de consulta a la base de datos usando Statement.
     * Statement es la forma más básica de ejecutar consultas a la base de datos.
     * Es la más insegura, ya que no se protege de ataques de inyección SQL.
     * No obstante es útil para sentencias DDL.
     * @param connection
     * @throws SQLException
     */
    private static void selectAllEmployees(Connection connection) throws SQLException {
        Statement selectEmployees = connection.createStatement();
        ResultSet employees = selectEmployees.executeQuery("select * from employees");

        while (employees.next()) {
            log.debug("Employee: {} {}",
                    employees.getString("first_name"),
                    employees.getString("last_name"));
        }
    }

    /**
     * Ejemplo de consulta a la base de datos usando PreparedStatement.
     * PreparedStatement es la forma más segura de ejecutar consultas a la base de datos.
     * Se protege de ataques de inyección SQL.
     * Es útil para sentencias DML.
     * @param connection
     * @throws SQLException
     */
    private static void selectAllEmployeesOfDepartment(Connection connection, String department) throws SQLException {
        PreparedStatement selectEmployees = connection.prepareStatement("select count(*) as 'Total'\n" +
                "from employees emp\n" +
                "inner join dept_emp dep_rel on emp.emp_no = dep_rel.emp_no\n" +
                "inner join departments dep on dep_rel.dept_no = dep.dept_no\n" +
                "where dep_rel.dept_no = ?;\n");
        selectEmployees.setString(1, department);
        ResultSet employees = selectEmployees.executeQuery();

        while (employees.next()) {
            log.debug("Empleados del departamento {}: {}",
                    department,
                    employees.getString("Total"));
        }
    }

    private static void employeesGender(Connection connection) throws SQLException {
        PreparedStatement employeesG = connection.prepareStatement("Select gender, count(*) as Numero from employees group by gender order by Numero DESC");
        ResultSet employees = employeesG.executeQuery();
        while (employees.next()) {
            log.debug("Gender {}: Total: {}",
            employees.getString("gender"),
            employees.getInt("Numero"));
        }
    }


    private static void mejorSalarioDpto(Connection connection,String department) throws SQLException {
        PreparedStatement employeesG = connection.prepareStatement("SELECT\n" +
                "    empleados.first_name AS nombre,\n" +
                "    empleados.last_name AS apellido,\n" +
                "    salario.salary AS salario,\n" +
                "    departamento.dept_no AS departamento\n" +
                "FROM employees.employees AS empleados\n" +
                "JOIN employees.salaries AS salario ON empleados.emp_no = salario.emp_no\n" +
                "JOIN employees.dept_emp AS departamento ON empleados.emp_no = departamento.emp_no\n" +
                "WHERE departamento.dept_no = ?\n" +
                "ORDER BY salario.salary DESC\n" +
                "LIMIT 1;");

        employeesG.setString(1, department);
        ResultSet employees = employeesG.executeQuery();

        while (employees.next()) {
            log.debug("nombre: {}, apellido:{}, salario: {}",
                    employees.getString("nombre"),
                    employees.getString("apellido"),
                    employees.getInt("salario"));
        }
    }

    private static void mejor2SalarioDpto(Connection connection,String department) throws SQLException {
        PreparedStatement employeesG = connection.prepareStatement("SELECT\n" +
                "    empleados.first_name AS nombre,\n" +
                "    empleados.last_name AS apellido,\n" +
                "    salario.salary AS salario,\n" +
                "    departamento.dept_no AS departamento\n" +
                "FROM employees.employees AS empleados\n" +
                "JOIN employees.salaries AS salario ON empleados.emp_no = salario.emp_no\n" +
                "JOIN employees.dept_emp AS departamento ON empleados.emp_no = departamento.emp_no\n" +
                "WHERE departamento.dept_no = ?\n" +
                "ORDER BY salario.salary DESC\n" +
                "LIMIT 1\n" +
                "OFFSET 1;");
        employeesG.setString(1, department);
        ResultSet employees = employeesG.executeQuery();

        while (employees.next()) {
            log.debug("nombre: {}, apellido:{}, salario: {}",
                    employees.getString("nombre"),
                    employees.getString("apellido"),
                    employees.getInt("salario"));
        }
    }

    private static void numeroEmpleadosContratados(Connection connection,int month) throws SQLException {
        PreparedStatement employeesG = connection.prepareStatement("SELECT COUNT(*) AS total_empleados\n" +
                "FROM employees.employees\n" +
                "WHERE MONTH(hire_date) = ?;");
        employeesG.setInt(1, month);
        ResultSet employees = employeesG.executeQuery();

        while (employees.next()) {
            log.debug("Total de empleados contratados en el mes {}: {}",
                    month,
                    employees.getInt("total_empleados"));
        }

    }


}
