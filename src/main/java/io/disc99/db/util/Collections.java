package io.disc99.db.util;

import java.util.ArrayList;
import java.util.List;

public class Collections {
    public static <E> List<E> concat(List<E> left, List<E> right) {
        List<E> newList = new ArrayList<E>(left);
        newList.addAll(right);
        return newList;
    }
}
