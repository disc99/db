package kis.basicdb;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Relational Database
 *
 */
public class App
{
    public static void main( String[] args )
    {
        //商品テーブル
        Table shohin = Table.create("shohin",
                new String[]{"shohin_id", "shohin_name", "kubun_id", "price"});
        shohin.insert(1, "りんご", 1, 300)
                .insert(2, "みかん", 1, 130)
                .insert(3, "キャベツ", 2, 200)
                .insert(4, "さんま", 3, 220)
                .insert(5, "わかめ", null, 250)//区分がnull
                .insert(6, "しいたけ", 4, 180)//該当区分なし
                .insert(7, "ドリアン", 1, null);
        //区分テーブル
        Table kubun = Table.create("kubun",
                new String[]{"kubun_id", "kubun_name"});
        kubun.insert(1, "くだもの")
                .insert(2, "野菜")
                .insert(3, "魚");

        System.out.println(shohin);//テーブル内容
        System.out.println(Query.from("shohin"));//クエリー経由でテーブル内容
        System.out.println(Query.from("shohin").select("shohin_name", "price"));//射影
        System.out.println(Query.from("shohin").lessThan("price", 250));//フィルター
        System.out.println(Query.from("shohin").leftJoin("kubun", "kubun_id"));//結合
        System.out.println(Query.from("shohin").leftJoin("kubun", "kubun_id")//全部入り
                .lessThan("price", 200).select("shohin_name", "kubun_name", "price"));
        System.out.println(Query //サブクエリー
                .from(Query.from("shohin").lessThan("price", 250))
                .leftJoin(Query.from("kubun").lessThan("kubun_id", 3), "kubun_id"));
        System.out.println(Query //一致比較
                .from("shohin")
                .leftJoin("kubun", "kubun_id")
                .equalsTo("shohin_id", 2)
                .select("shohin_id", "shohin_name", "kubun_name", "price"));
        System.out.println(Query //安い順に並べ替え
                .from("shohin")
                .orderBy("price"));
        System.out.println(Query //高い順に並べ替え
                .from("shohin")
                .orderBy("price", false));
        System.out.println(Query //集計
                .from(Query
                        .from("shohin")
                        .groupBy("kubun_id", new Count("shohin_name"), new Average("price")))
                .leftJoin("kubun", "kubun_id")
                .select("kubun_id", "kubun_name", "count", "average"));
    }

    /** テーブル一覧 */
    private static Map<String, Table> tables = new HashMap<>();

    /** クエリーの起点 */
    public static class Query{
        /**
         * クエリーのベースになるテーブルを選択
         * @param tableName テーブル名
         * @return
         */
        public static QueryObj from(String tableName){
            return from(tables.get(tableName));
        }
        /**
         * クエリーのベースになるリレーションを選択
         * @param tableName テーブル名
         * @return
         */
        public static QueryObj from(QueryObj relation){
            return relation;
        }
    }

    /** クエリーとして利用できるメソッド(補完時に不要なメソッドを出さないため) */
    public interface QueryObj{
        /**
         * left join
         * @param tableName 右側テーブル名
         * @param matchingField 値を結合する基準になる属性名
         * @return
         */
        QueryObj leftJoin(String tableName, String matchingField);
        QueryObj leftJoin(QueryObj relation, String matchingField);

        /**
         * 指定した値よりも小さいタプルを抽出
         * @param columnName 比較するフィールド名
         * @param value 比較する値
         * @return
         */
        QueryObj lessThan(String columnName, Integer value);

        /**
         * 指定した値に一致するタプルを抽出
         * @param columnName 比較するフィールド名
         * @param value 比較する値
         * @return
         */
        QueryObj equalsTo(String columnName, Object value);

        /**
         * 射影
         * @param columnNames 抽出するフィールド名
         * @return
         */
        QueryObj select(String... columnNames);


        /**
         * 昇順に並べ替え
         * @param columnName 並べ替えるフィールド名
         * @return
         */
        QueryObj orderBy(String columnName);

        /**
         * 並べ替え
         * @param columnName 並べ替えるフィールド名
         * @param asc 昇順のばあいtrue
         * @return
         */
        QueryObj orderBy(String columnName, boolean asc);

        /**
         * グループ化
         * @param columnName グループ化するフィールド名
         * @param aggregations 集計関数
         * @return
         */
        QueryObj groupBy(String columnName, Aggregation... aggregations);
    }

    /** 集計関数の基底 */
    public static abstract class Aggregation{
        String columnName;
        public Aggregation(String columnName) {
            this.columnName = columnName;
        }
        /** 関数名 */
        public abstract String getName();
        /** データ追加 */
        public abstract void addData(Object value);
        /** 結果取得 */
        public abstract Object getResult();
        /** リセット */
        public abstract void reset();
    }

    /** 集計用カウント関数 */
    public static class Count extends Aggregation{
        int counter = 0;

        @Override
        public String getName() {
            return "count";
        }
        public Count(String columnName) {
            super(columnName);
        }
        @Override
        public void addData(Object value) {
            ++counter;
        }
        @Override
        public Object getResult() {
            return counter;
        }
        @Override
        public void reset() {
            counter = 0;
        }
    }

    /** 集計用平均関数 */
    public static class Average extends Aggregation{
        int counter = 0;
        int total = 0;
        public Average(String columnName) {
            super(columnName);
        }

        @Override
        public String getName() {
            return "average";
        }

        @Override
        public void addData(Object value) {
            ++counter;
            total += (Integer)value;
        }

        @Override
        public Object getResult() {
            return (double)total / counter;
        }

        @Override
        public void reset() {
            counter = 0;
            total = 0;
        }
    }

    /** リレーション */
    public static class Relation implements QueryObj{
        /** 属性 */
        List<Column> columns;
        /** データ */
        List<Taple> taples;

        public Relation(List<Column> columns, List<Taple> taples) {
            this.columns = columns;
            this.taples = taples;
        }

        /**
         * 射影
         */
        @Override
        public QueryObj select(String... columnNames){
            List<Integer> indexes = new ArrayList<>();
            List<Column> newColumns = new ArrayList<>();
            for(String n : columnNames){
                newColumns.add(new Column(n));
                //属性を探す
                int idx = findColumn(n);
                indexes.add(idx);
            }
            //データの投影
            List<Taple> newTaples = new ArrayList<>();
            for(Taple tp : taples){
                List<Object> values = new ArrayList<>();
                for(int idx : indexes){
                    if(idx < tp.values.size()){
                        values.add(tp.values.get(idx));
                    }else{
                        values.add(null);
                    }
                }
                newTaples.add(new Taple(values));
            }

            return new Relation(newColumns, newTaples);
        }

        /**
         * left join
         */
        @Override
        public QueryObj leftJoin(String tableName, String matchingField){
            //テーブル取得
            Table tbl = tables.get(tableName);
            return leftJoin(tbl, matchingField);
        }
        @Override
        public QueryObj leftJoin(QueryObj relation, String matchingField){
            Relation tbl = (Relation) relation;
            //属性の作成
            List<Column> newColumns = new ArrayList<>(columns);
            newColumns.addAll(tbl.columns);

            //値の作成
            List<Taple> newTaples = new ArrayList<>();
            int leftColumnIdx = findColumn(matchingField);
            int rightColumnIdx = tbl.findColumn(matchingField);
            if(leftColumnIdx >= columns.size() || rightColumnIdx >= tbl.columns.size()){
                //該当フィールドがない場合は値の結合をしない
                return new Relation(newColumns, Collections.EMPTY_LIST);
            }
            //結合処理
            for(Taple tp : taples){
                //元のテーブルのデータ
                Taple ntpl = new Taple(new ArrayList<>(tp.values));
                //足りないフィールドを埋める
                while(ntpl.values.size() < columns.size()){
                    ntpl.values.add(null);
                }
                //結合対象のフィールドを探す
                Object leftValue = ntpl.values.get(leftColumnIdx);
                //結合するタプルを抽出
                Relation leftRel = (Relation)relation.equalsTo(matchingField, leftValue);
                //一致するタプルがあれば結合
                if(!leftRel.taples.isEmpty()){
                    for(Object v : leftRel.taples.get(0).values){//今回は、タプルの対応は一対一まで
                        ntpl.values.add(v);
                    }
                }

                //足りないフィールドを埋める
                while(ntpl.values.size() < newColumns.size()){
                    ntpl.values.add(null);
                }

                newTaples.add(ntpl);
            }
            return new Relation(newColumns, newTaples);
        }

        /**
         * 指定した値よりも小さいタプルを抽出
         */
        @Override
        public QueryObj lessThan(String columnName, Integer value){
            if(value == null){
                return new Relation(columns, Collections.EMPTY_LIST);
            }
            int idx = findColumn(columnName);
            if(idx >= columns.size()){
                return new Relation(columns, Collections.EMPTY_LIST);
            }
            List<Taple> newTaples = new ArrayList<>();
            for(Taple tp : taples){
                try{
                    if((Integer)tp.values.get(idx) < value){
                        newTaples.add(tp);
                    }
                }catch(NullPointerException | ArrayIndexOutOfBoundsException ex){}
            }
            return new Relation(columns, newTaples);
        }

        /**
         * 指定した値に一致するタプルを抽出
         */
        @Override
        public QueryObj equalsTo(String columnName, Object value) {
            if(value == null){
                return new Relation(columns, Collections.EMPTY_LIST);
            }
            int idx = findColumn(columnName);
            if(idx >= columns.size()){
                return new Relation(columns, Collections.EMPTY_LIST);
            }
            List<Taple> newTaples = new ArrayList<>();
            for(Taple tp : taples){
                if(value.equals(tp.values.get(idx))){
                    newTaples.add(tp);
                }
            }
            return new Relation(columns, newTaples);
        }

        @Override
        public QueryObj orderBy(String columnName) {
            return orderBy(columnName, true);
        }

        @Override
        public QueryObj orderBy(String columnName, final boolean asc) {
            final int idx = findColumn(columnName);
            if(idx >= columns.size()){
                return this;
            }

            List<Taple> newTaple = new ArrayList<>(taples);
            Collections.sort(newTaple, new Comparator<Taple>(){
                @Override
                public int compare(Taple o1, Taple o2) {
                    Object v1 = o1.values.size() > idx ? o1.values.get(idx) : null;
                    Object v2 = o2.values.size() > idx ? o2.values.get(idx) : null;
                    if(v1 == null){
                        if(v2 == null){
                            return 0;
                        }else{
                            return 1;
                        }
                    }
                    if(v2 == null){
                        return -1;
                    }
                    return ((Comparable)v1).compareTo(v2) * (asc ? 1 : -1);
                }
            });

            return new Relation(columns, newTaple);
        }

        @Override
        public QueryObj groupBy(String columnName, Aggregation... aggregations) {
            //列名を作成
            List<Column> newColumns = new ArrayList<>();
            newColumns.add(new Column(columnName));
            List<Integer> colIndexes = new ArrayList<>();
            for(Aggregation agg : aggregations){
                newColumns.add(new Column(agg.getName()));
                colIndexes.add(findColumn(agg.columnName));
            }

            //集計行を取得
            int idx = findColumn(columnName);
            if(idx >= columns.size()){
                return new Relation(newColumns, Collections.EMPTY_LIST);
            }

            //あらかじめソート
            Relation sorted = (Relation) orderBy(columnName);

            Object current = null;
            List<Taple> newTaples = new ArrayList<>();
            for(Taple tp : sorted.taples){
                //集計フィールド取得
                if(tp.values.size() <= idx) continue;
                Object v = tp.values.get(idx);
                if(v == null) continue;

                if(!v.equals(current)){
                    if(current != null){
                        //集計行を追加
                        List<Object> values = new ArrayList<>();
                        values.add(current);
                        for(Aggregation agg : aggregations){
                            values.add(agg.getResult());
                        }
                        newTaples.add(new Taple(values));
                    }
                    current = v;
                    for(Aggregation agg : aggregations){
                        agg.reset();
                    }
                }
                //集計
                for(int i = 0; i < aggregations.length; ++i){
                    int aidx = colIndexes.get(i);
                    if(tp.values.size() <= aidx) continue;
                    Object cv = tp.values.get(aidx);
                    if(cv == null) continue;

                    Aggregation ag = aggregations[i];
                    ag.addData(cv);
                }
            }
            if(current != null){
                //集計行を追加
                List<Object> values = new ArrayList<>();
                values.add(current);
                for(Aggregation agg : aggregations){
                    values.add(agg.getResult());
                }
                newTaples.add(new Taple(values));
            }

            return new Relation(newColumns, newTaples);
        }

        /**
         * 属性を探す
         * @param name 属性名
         * @return 属性のインデックス。みつからなかったときは属性数(インデックスからあふれる)を返す
         */
        public int findColumn(String name){
            for(int i = 0; i < columns.size(); ++i){
                if(columns.get(i).name.equals(name)){
                    return i;
                }
            }
            return columns.size();
        }

        @Override
        public String toString() {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            //フィールド名
            for(Column cl : columns){
                pw.print("|");
                if(cl.parent != null && !cl.parent.isEmpty()){
                    pw.print(cl.parent + ".");
                }
                pw.print(cl.name);
            }
            pw.println("|");
            //データ
            for(Taple t : taples){
                for(Object v : t.values){
                    pw.print("|");
                    pw.print(v);
                }
                pw.println("|");
            }

            return sw.toString();
        }
    }

    /** テーブル */
    public static class Table extends Relation{
        /** テーブル名 */
        String name;

        /** テーブル生成 */
        public static Table create(String name, String[] columnNames){
            List<Column> columns = new ArrayList<>();
            for(String n : columnNames){
                columns.add(new Column(name, n));
            }
            Table t = new Table(name, columns);
            tables.put(name, t);
            return t;
        }

        /**
         * テーブルオブジェクトの生成
         * @param name テーブル名
         * @param columns 属性
         */
        private Table(String name, List<Column> columns) {
            super(columns, new ArrayList<Taple>());
            this.name = name;
        }

        /**
         * タプルの追加
         * @param values 値
         * @return
         */
        public Table insert(Object... values){
            taples.add(new Taple(Arrays.asList(values)));
            return this;
        }
    }
    /** タプル */
    public static class Taple{
        /** 保持する値 */
        List<Object> values;

        public Taple(List<Object> values) {
            this.values = values;
        }

    }

    /** 属性 */
    public static class Column{
        /** テーブルなどの名前 */
        String parent;
        /** 属性名 */
        String name;

        public Column(String parent, String name) {
            this.parent = parent;
            this.name = name;
        }

        public Column(String name) {
            this("", name);
        }
    }
}