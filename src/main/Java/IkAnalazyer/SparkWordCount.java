package IkAnalazyer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SparkWordCount implements Serializable {
    private String doc = "企业名单";
    private boolean isSelectFile = false;
    private int wordLength = 0; //0 为所有单词长度

    //conf 和 sc变量 必须声明为静态类或者在静态方法如main方法中调用
    private static SparkConf conf = null;
    private static JavaSparkContext sc = null;

    private void initSpark() {

        /**
         * 1，创建SparkConf对象，设置spark应用程序的配置信息
         *
         */
        conf = new SparkConf().setAppName(SparkWordCount.class.getSimpleName()).setMaster("local[*]");

        /**
         * 2,创建sparkcontext对象，JavaSparkContext
         */

        sc = new JavaSparkContext(conf);
    }

    SparkWordCount(String doc, boolean isSelectFile, int wordLength) {
        this.doc = doc;
        this.isSelectFile = isSelectFile;
        this.wordLength = wordLength;

        initSpark();
    }

    SparkWordCount() {

        initSpark();
    }

    private List<String> getSplitWords(String line) {
        List<String> words = new ArrayList<String>();
        if (line == null || line.trim().length() == 0) {
            return words;
        }

        try {
            InputStream is = new ByteArrayInputStream(line.getBytes("UTF-8"));
            IKSegmenter seg = new IKSegmenter(new InputStreamReader(is), false);

            Lexeme lex = seg.next();

            while (lex != null) {
                String word = lex.getLexemeText();
                if (wordLength == 0 || word.length() == wordLength) {
                    words.add(word);
                }

                lex = seg.next();
            }

        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return words;
    }


    public JavaPairRDD<String, Integer> wordcount() {
        /**
         * textfile方法读取hdfs上的文本文件
         */

        JavaRDD<String> lines = null;

        if (isSelectFile) {
            lines = sc.textFile(doc);
        } else {
            lines = sc.textFile("/Users/haoxing/IdeaProjects/enterpriselabel/src/main/Java/text/" + doc + ".txt");
        }

        //@1读取数据后对数据进行切分处理先判断credit_code的长度是否等于12，取不等于12的数据，拿到数据的
        /** JavaRDD<String> map = lines.filter(new Function<String, Boolean>() {
        @Override public Boolean call(String s) throws Exception {
        String[] split1 = s.split(",");
        String[] split2 = split1[0].split(":");
        boolean b = split2[1].length() == 18 && (split2[1].startsWith("91")||split2[1].startsWith("92")||split2[1].startsWith("93"));
        return b;
        }
        }).map(new Function<String, String>() {
        @Override public String call(String s) throws Exception {
        String s1 = s.split(",")[1].split(":")[1];
        return s1;
        }
        });
         */

        JavaRDD<String> map = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String[] split = s.split(",");
                boolean b = split.length == 2 && split[0].length() == 18 && (split[0].substring(0, 2) != "91" || split[0].substring(0, 2) != "92" || split[0].substring(0, 2) != "93");
                return b;
            }
        }).map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                String s1 = s.split(",")[1];
                return s1;
            }
        });

        /**
         *
         *4.对文本进行拆分，将文本拆分为多个单词，
         * 反回每一行的每一个单词
         * 用中文分词，调用分词方法后的list结果
         */

        JavaRDD<String> words = map.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return getSplitWords(s).listIterator();
            }
        });

        /**
         * 5.将每一个单词的初始数量都标记为一个
         */
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        /**
         *6.计算每个单词出现的次数
         */

        JavaPairRDD<String, Integer> wordCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        return wordCount;
    }

    /**
     * 排序
     */
    public JavaPairRDD<String, Integer> sortByValue(JavaPairRDD<String, Integer> wordCount, boolean isAsc) {
        //把key和value互换载进行sortbykey就ok
        JavaPairRDD<Integer, String> pairs2 = wordCount.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> s) throws Exception {
                return new Tuple2<Integer, String>(s._2, s._1);
            }
        });

        //降序
        JavaPairRDD<Integer, String> pairs3 = pairs2.sortByKey(isAsc);

        //再次交换key和value

        JavaPairRDD<String, Integer> Count = pairs3.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return new Tuple2<String, Integer>(integerStringTuple2._2, integerStringTuple2._1);
            }
        });

        return Count;
    }

    public void closeSpark(JavaPairRDD<String, Integer> wordCount) {
        /**
         * 7、使用foreach这个action算子提交Spark应用程序
         * 在Spark中，每个应用程序都需要transformation算子计算，最终由action算子触发作业提交
         */
        wordCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1 + ":" + wordCount._2);
            }
        });

        sc.close();
    }

    public static void main(String[] args) {
        SparkWordCount app = new SparkWordCount();
        JavaPairRDD<String, Integer> wordCount = app.wordcount();
        JavaPairRDD<String, Integer> sorted = app.sortByValue(wordCount, false).repartition(1);
        app.closeSpark(sorted);

    }

}
