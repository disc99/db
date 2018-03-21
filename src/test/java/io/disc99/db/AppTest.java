package io.disc99.db;

import io.disc99.db.App.*;
import org.junit.Test;

public class AppTest {
    @Test
    public void test() {
        //商品テーブル
        Table shohin = Table.create("shohin",
                new String[]{"shohin_id", "shohin_name", "kubun_id", "price"});

        shohin.insert(1, "りんご", 1, 300)
                .insert(2, "みかん", 1, 130)
                .insert(3, "キャベツ", 2, 200)
                .insert(4, "わかめ", null, 250)//区分がnull
                .insert(5, "しいたけ", 3, 180);//該当区分なし

        //区分テーブル
        Table kubun = Table.create("kubun",
                new String[]{"kubun_id", "kubun_name"});

        kubun.insert(1, "くだもの")
                .insert(2, "野菜");

        System.out.println(shohin);//テーブル内容
        System.out.println(Query.from("shohin"));//クエリー経由でテーブル内容
        System.out.println(Query.from("shohin").select("shohin_name", "price"));//射影
        System.out.println(Query.from("shohin").lessThan("price", 250));//フィルター
        System.out.println(Query.from("shohin").leftJoin("kubun", "kubun_id"));//結合
        System.out.println(Query.from("shohin").leftJoin("kubun", "kubun_id")//全部入り
                .lessThan("price", 200).select("shohin_name", "kubun_name", "price"));

    }
}
