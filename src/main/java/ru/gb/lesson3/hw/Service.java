package ru.gb.lesson3.hw;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class Service {
    private List<Person> people;
    private List<Department> departments;


    /**
     * 7. Загружаем данные из таблиц Person и Department в списки соответствующих им объектов
     */
    public void extractDataFromDB(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {

            departments = new ArrayList<>();
            ResultSet resultSet = statement.executeQuery("SELECT id, name FROM Department");
            while (resultSet.next()) {
                long id = resultSet.getLong("id");
                String name = resultSet.getString("name");
                departments.add(new Department(id, name));
            }

            people = new ArrayList<>();
            resultSet = statement.executeQuery("SELECT id, name, age, department_id FROM Person");

            while (resultSet.next()) {
                long id = resultSet.getLong("id");
                String name = resultSet.getString("name");
                int age = resultSet.getInt("age");
                long department_id = resultSet.getLong("department_id");
                Department department = getDepartment(department_id); //getDepartmentById(resultSet.getLong("department_id"));
                people.add(new Person(id, name, age, department));
            }
        } catch (UnsupportedOperationException e) {
            System.err.println("Во время создания таблицы произошла ошибка: " + e.getMessage());
            throw e;
        }
    }

    private Department getDepartment(long departmentId) {
        return departments.stream()
                .filter(it -> it.getId() == departmentId).findFirst().orElse(null);
    }

    /**
     * 7.4 Метод, который загружает Department по Идентификатору person
     */
    public Department getPersonDepartmentName(long personId) throws SQLException {
        return people.stream()
                .filter(it -> it.getId() == personId)
                .map(Person::getDepartment)
                .findFirst()
                .orElse(null);

    }

    /**
     * 7.5 Метод, который загружает Map<Person, Department,
     */
    public Map<Person, Department> getPersonDepartments() throws SQLException {
        return people.stream().collect(Collectors.toMap(x -> x, Person::getDepartment));
    }

    /**
     * 7.6 Метод, который загружает Map<Department, List<Person>>, в которой маппинг department.name -> <person.name>
     */
    public Map<Department, List<Person>> getDepartmentPersons() throws SQLException {
        return people.stream()
                .collect(Collectors.groupingBy(Person::getDepartment));
    }


}