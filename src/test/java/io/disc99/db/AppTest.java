package io.disc99.db;

import io.disc99.db.App.*;
import org.assertj.core.api.Assertions;
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
                .insert(4, "さんま", 3, 220)
                .insert(5, "わかめ", null, 250) //区分がnull
                .insert(6, "しいたけ", 4, 180) //該当区分なし
                .insert(7, "ドリアン", 1, null);
        //区分テーブル
        Table kubun = Table.create("kubun",
                new String[]{"kubun_id", "kubun_name"});
        kubun.insert(1, "くだもの")
                .insert(2, "野菜")
                .insert(3, "魚");

//        System.out.println(shohin);//テーブル内容
        Assertions.assertThat(Query.from("shohin").toString()).isEqualTo("|shohin_id|shohin_name|kubun_id|price|\n|1|りんご|1|300|\n|2|みかん|1|130|\n|3|キャベツ|2|200|\n|4|さんま|3|220|\n|5|わかめ|null|250|\n|6|しいたけ|4|180|\n|7|ドリアン|1|null|\n");//クエリー経由でテーブル内容
        Assertions.assertThat(Query.from("shohin").select("shohin_name", "price").toString()).isEqualTo("|shohin_name|price|\n|りんご|300|\n|みかん|130|\n|キャベツ|200|\n|さんま|220|\n|わかめ|250|\n|しいたけ|180|\n|ドリアン|null|\n");//射影
        Assertions.assertThat(Query.from("shohin").lessThan("price", 250).toString()).isEqualTo("|shohin_id|shohin_name|kubun_id|price|\n|2|みかん|1|130|\n|3|キャベツ|2|200|\n|4|さんま|3|220|\n|6|しいたけ|4|180|\n");//フィルター
        Assertions.assertThat(Query.from("shohin").leftJoin("kubun", "kubun_id").toString()).isEqualTo("|shohin_id|shohin_name|kubun_id|price|kubun_id|kubun_name|\n|1|りんご|1|300|1|くだもの|\n|2|みかん|1|130|1|くだもの|\n|3|キャベツ|2|200|2|野菜|\n|4|さんま|3|220|3|魚|\n|5|わかめ|null|250|null|null|\n|6|しいたけ|4|180|null|null|\n|7|ドリアン|1|null|1|くだもの|\n");//結合
        Assertions.assertThat(Query.from("shohin").leftJoin("kubun", "kubun_id")//全部入り
                .lessThan("price", 200).select("shohin_name", "kubun_name", "price").toString()).isEqualTo("|shohin_name|kubun_name|price|\n|みかん|くだもの|130|\n|しいたけ|null|180|\n");
        Assertions.assertThat(Query //サブクエリー
                .from(Query.from("shohin").lessThan("price", 250))
                .leftJoin(Query.from("kubun").lessThan("kubun_id", 3), "kubun_id").toString()).isEqualTo("|shohin_id|shohin_name|kubun_id|price|kubun_id|kubun_name|\n|2|みかん|1|130|1|くだもの|\n|3|キャベツ|2|200|2|野菜|\n|4|さんま|3|220|null|null|\n|6|しいたけ|4|180|null|null|\n");
        Assertions.assertThat(Query //一致比較
                .from("shohin")
                .leftJoin("kubun", "kubun_id")
                .equalsTo("shohin_id", new IntegerValue(2))
                .select("shohin_id", "shohin_name", "kubun_name", "price").toString()).isEqualTo("|shohin_id|shohin_name|kubun_name|price|\n|2|みかん|くだもの|130|\n");
        Assertions.assertThat(Query //安い順に並べ替え
                .from("shohin")
                .orderBy("price").toString()).isEqualTo("|shohin.shohin_id|shohin.shohin_name|shohin.kubun_id|shohin.price|\n|2|みかん|1|130|\n|6|しいたけ|4|180|\n|3|キャベツ|2|200|\n|4|さんま|3|220|\n|5|わかめ|null|250|\n|1|りんご|1|300|\n|7|ドリアン|1|null|\n");
        Assertions.assertThat(Query //高い順に並べ替え
                .from("shohin")
                .orderBy("price", false).toString()).isEqualTo("|shohin.shohin_id|shohin.shohin_name|shohin.kubun_id|shohin.price|\n|1|りんご|1|300|\n|5|わかめ|null|250|\n|4|さんま|3|220|\n|3|キャベツ|2|200|\n|6|しいたけ|4|180|\n|2|みかん|1|130|\n|7|ドリアン|1|null|\n");
        Assertions.assertThat(Query //集計
                .from(Query
                        .from("shohin")
                        .groupBy("kubun_id", new Count("shohin_name"), new Average("price")))
                .leftJoin("kubun", "kubun_id")
                .select("kubun_id", "kubun_name", "count", "average").toString()).isEqualTo("|kubun_id|kubun_name|count|average|\n|1|くだもの|3|215.0|\n|2|野菜|1|200.0|\n|3|魚|1|220.0|\n|4|null|1|180.0|\n");

    }

}
