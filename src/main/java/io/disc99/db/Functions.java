package io.disc99.db;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collector;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

public class Functions {
    public static <T, R> Collector<T, ?, R> toListAnd(Function<List<T>, R> finisher) {
        return collectingAndThen(toList(), finisher);
    }
}
