package ru.gb.lesson2.anno.lib;

import ru.gb.lesson2.anno.AnnotationsMain;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Random {

  int min() default 0;

  int max() default 100;

//  String s();
//
//  Gender gender();
//
//  Class<?> cl();
//
//  String[] ss();


  // все примитивы int, long, ...
  // String
  // любой enum
  // Class
  // массив всего, что выше

}