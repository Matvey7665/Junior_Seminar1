package ru.gb.lesson3.hw;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Person {
    private long id;
    private String name;
    private int age;
    private Department department;

    @Override
    public String toString() {
        return "name:" + name +
                ", age:" + age;
    }
}