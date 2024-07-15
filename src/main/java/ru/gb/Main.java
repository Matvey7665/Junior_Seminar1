package ru.gb;

import ru.gb.lesson3.hw.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class Main {

  // Jetbrains Intellij IDEA

  // .zip

  public static void main(String[] args){

    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test")) {
      // п.1
      createTable(connection);
      insertData(connection);

      // пп 1-2. Создаем таблицу Department, добавляем primary key department_id в Person. заполняем их
      createDepartment(connection);
      insertDepartmentData(connection);
      addForeignKey(connection);
      setDepartmentId(connection);
      selectData(connection);

      // пп 4-6 задания
      System.out.printf("\nВывод департамента по id сотрудника:\nid: %s, department: %s\n", 2,
              getPersonDepartmentName(connection, 2));
      System.out.println("\nСписок сотрудников c указанием департамента (Map<String, String>):\n" +
              mapToString(getPersonDepartments(connection)));
      System.out.println("\nСписок сотрудников по департаментам (Map<String, List<String>>):\n" +
              mapToString(getDepartmentPersons(connection)));

      // п.7. Созданы одноименные классы-обертки над таблицами: Department и Person
      // и методы, соответствующие пп 4-6 в классе Service, возвращающие объекты

      Service service = new Service();
      service.extractDataFromDB(connection);

      System.out.printf("\nВывод департамента по id сотрудника:\nid: %s, department: %s\n", 4,
              service.getPersonDepartmentName(4));
      System.out.println("\nСписок сотрудников c указанием департамента (Map<Person, Department>):\n" +
              mapToString(service.getPersonDepartments()));
      System.out.println("\nСписок сотрудников по департаментам (Map<Department, List<Person>>):\n" +
              mapToString(service.getDepartmentPersons()));


    } catch (SQLException e) {
      System.err.println("Во время подключения произошла ошибка: " + e.getMessage());
    }
  }

  /**
   * 2. Создаем таблицу Department (id bigint primary key, name varchar(128) not null)
   */
  private static void createDepartment(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute("""
                    CREATE TABLE Department (
                      id BIGINT PRIMARY KEY,
                      name VARCHAR(128) NOT NULL
                    )""");
    } catch (SQLException e) {
      System.err.println("Во время создания таблицы произошла ошибка: " + e.getMessage());
      throw e;
    }
  }

  /**
   * 2.1 Заполняем поля таблицы
   */
  private static void insertDepartmentData(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      StringBuilder sb = new StringBuilder("""
                    INSERT INTO Department (id, name)
                      VALUES""");
      for (int i = 0; i < 4; i++) {
        sb.append(String.format("\n(%s, 'Department #%s')", i + 1, i + 1))
                .append(i < 3 ? ", " : "");
      }
      statement.executeUpdate(sb.toString());
    } catch (SQLException e) {
      System.err.println("Во время добавления записей произошла ошибка: " + e.getMessage());
      throw e;
    }
  }

  /**
   * 3. Добавляем в таблицу Person поле department_id типа bigint (внешний ключ)
   */
  private static void addForeignKey(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate("""
                    ALTER TABLE Person
                        ADD department_id BIGINT;
                    ALTER TABLE Person
                        ADD FOREIGN KEY (department_id)
                            REFERENCES Department(id)
                    """);
    } catch (SQLException e) { /* ,
     */
      System.err.println("При добавления внешнего ключа возникла ошибка");
      throw e;
    }
  }

  /**
   * 3.1 Заполняем случайными значениями поле department_id
   */
  private static void setDepartmentId(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < 11; i++) {
        sb.append("UPDATE Person SET department_id = ")
                .append(ThreadLocalRandom.current().nextInt(4) + 1)
                .append(" WHERE id = ").append(i + 1)
                .append(i < 10 ? ";\n" : "\n");
      }
      statement.executeUpdate(sb.toString());
    } catch (Exception e) {
      System.out.println("Обновление поля не удалось");
      throw e;
    }
  }


  /**
   * 4. Метод, который загружает Имя department по Идентификатору person
   */
  private static String getPersonDepartmentName(Connection connection, long personId) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement("""
                SELECT d.name
                    FROM Person p LEFT JOIN Department d
                        ON p.department_id = d.id
                    WHERE p.id = ?
                """)) {
      statement.setLong(1, personId);
      ResultSet resultSet = statement.executeQuery();
      if (resultSet.next())
        return resultSet.getString("name");
      throw new NullPointerException("Нет данных по id = " + personId);
    } catch (UnsupportedOperationException e) {
      System.err.println(e.getMessage());
      throw e;
    }

  }

  /**
   * 5. Метод, который загружает Map<String, String>,
   */
  private static Map<String, String> getPersonDepartments(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("""
                        SELECT p.name, d.name depart_name
                        FROM Person p LEFT JOIN Department d
                            ON p.department_id = d.id
                    """);

      Map<String, String> map = new HashMap<>();
      while (resultSet.next()) {
        String key = resultSet.getString("name");
        String value = resultSet.getString("depart_name");
        map.put(key, value);
      }

      return map;

    } catch (Exception e) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * 6. ** Метод, который загружает Map<String, List<String>>, в которой маппинг department.name -> <person.name>
   */
  private static Map<String, List<String>> getDepartmentPersons(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("""
                        SELECT p.name person_name, d.name depart_name
                        FROM Department d LEFT JOIN Person p
                            ON p.department_id = d.id
                    """);

      Map<String, List<String>> map = new HashMap<>();
      while (resultSet.next()) {
        String value = resultSet.getString("person_name");
        String key = resultSet.getString("depart_name");
        map.computeIfAbsent(key, v -> new ArrayList<>()).add(value);
      }
      return map;
    } catch (UnsupportedOperationException e) {
      System.err.println("Ошибка выполнения операции");
      throw e;
    }
  }


  /**
   * 1. Создаем таблицу Person (скопируем код с семниара)
   */
  private static void createTable(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute("""
                    create table person (
                      id bigint primary key,
                      name varchar(256),
                      age integer,
                      active boolean
                    )
                    """);
    } catch (SQLException e) {
      System.err.println("Во время создания таблицы произошла ошибка: " + e.getMessage());
      throw e;
    }
  }

  /**
   * 1.1. Заполняем поля таблицы
   */
  private static void insertData(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      StringBuilder insertQuery = new StringBuilder("insert into person(id, name, age, active) values\n");
      for (int i = 1; i <= 10; i++) {
        int age = ThreadLocalRandom.current().nextInt(20, 60);
        boolean active = ThreadLocalRandom.current().nextBoolean();
        insertQuery.append(String.format("(%s, '%s', %s, %s)", i, "Person #" + i, age, active));

        if (i != 10) {
          insertQuery.append(",\n");
        }
      }

      int insertCount = statement.executeUpdate(insertQuery.toString());
      System.out.println("Вставлено строк: " + insertCount);
    }
  }


  private static void selectData(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("""
                    select p.id, p.name, p.age, d.name depart_name
                    from person p left join department d
                    on p.department_id = d.id
                    """);

      while (resultSet.next()) {
        long id = resultSet.getLong("id");
        String name = resultSet.getString("name");
        int age = resultSet.getInt("age");
        String depart_name = resultSet.getString("depart_name");
        System.out.println("Найдена строка: [id = " + id + ", name = " + name
                + ", age = " + age + ", department = " + depart_name + "]");
      }
    }
  }

  private static <K, V> String mapToString(Map<K, V> map) {
    return map.entrySet().stream()
            .map(e -> e.getKey() + " " + e.getValue())
            .collect(Collectors.joining("\n"));
  }
}
