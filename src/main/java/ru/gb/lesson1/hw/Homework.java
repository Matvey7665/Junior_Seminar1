package ru.gb.lesson1.hw;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Homework {

  private static class Department {
    private String name;

    // TODO: геттеры, сеттеры
  }

  private static class Person {
    private String name;
    private int age;
    private double salary;
    private Department depart;

    // TODO: геттеры, сеттеры
  }

  /**
   * Найти самого молодого сотрудника
   */
  static Optional<Person> findMostYoungestPerson(List<Person> people) {
    // FIXME: ваша реализация здесь
    throw new UnsupportedOperationException();
  }

  /**
   * Найти департамент, в котором работает сотрудник с самой большой зарплатой
   */
  static Optional<Department> findMostExpensiveDepartment(List<Person> people) {
    // FIXME: ваша реализация здесь
    throw new UnsupportedOperationException();
  }

  /**
   * Сгруппировать сотрудников по департаментам
   */
  static Map<Department, List<Person>> groupByDepartment(List<Person> people) {
    // FIXME: ваша реализация здесь
    throw new UnsupportedOperationException();
  }

  /**
   * Сгруппировать сотрудников по названиям департаментов
   */
  static Map<String, List<Person>> groupByDepartmentName(List<Person> people) {
    // FIXME: ваша реализация здесь
    throw new UnsupportedOperationException();
  }

  /**
   * В каждом департаменте найти самого старшего сотрудника
   */
  static Map<String, Person> getDepartmentOldestPerson(List<Person> people) {
    // FIXME: ваша реализация здесь
    throw new UnsupportedOperationException();
  }

  /**
   * *Найти сотрудников с минимальными зарплатами в своем отделе
   * (прим. можно реализовать в два запроса)
   */
  static List<Person> cheapPersonsInDepartment(List<Person> people) {
    // FIXME: ваша реализация здесь
    throw new UnsupportedOperationException();
  }

}
