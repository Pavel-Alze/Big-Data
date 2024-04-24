import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

public class LeftJoin {
#
# Пусть у нас есть две таблицы формата (id,name) и (name,car)
# Нам нужно их объединить по полю `name` -> LEFT JOIN ON table1.name = table2.name;
# Первый мапер обрабатывае первую таблицу, а второй - вторую
#
    public static class JoinFirstMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

	        ArrayList<Text> fields = new ArrayList<Text>();
            StringTokenizer st = new StringTokenizer(value.toString(), ",");

            while (st.hasMoreTokens()) {
                Text word = new Text();
                word.set(st.nextToken());
                fields.add(word);
            }
#
# Из первой таблицы мы отправляем ключом поле `name`, то есть второе поле из таблицы
# и второй элемент из списка 'fields' | индекс == 1
# а остальные элементы отправляются, как значение, но с добавлением метки таблицы (+":1")
#
            context.write(fields.get(1),new Text(fields.get(0).toString()+":1"));
        }
    }

    public static class JoinSecondMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

	        ArrayList<Text> fields = new ArrayList<Text>();
            StringTokenizer st = new StringTokenizer(value.toString(), ",");

            while (st.hasMoreTokens()) {
                Text word = new Text();
                word.set(st.nextToken());
                fields.add(word);
            }
#
# Из второй таблицы мы отправляем ключом поле `name`, то есть первое поле из таблицы
# и первый элемент из списка 'fields' | индекс == 0
# а остальные элементы отправляются, как значение, но с добавлением метки таблицы (+":2")
#
            context.write(fields.get(0),new Text(fields.get(1).toString()+":2"));
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        ArrayList<String> bagOne = new ArrayList<>();
	        ArrayList<String> bagTwo = new ArrayList<>();
#
# Перебираем все пришедшие значения для join_key и распределяем по спискам(корзинам)
#
            for (Text v : values){
                String[] tmp = v.toString().split(":");
		        if(tmp[1].equals("1")){
		            bagOne.add(tmp[0]);
		        }else{
		            bagTwo.add(tmp[0]);
		        }
            }
#
# Проверяем что у нас есть хоть один не пустой список
# Так как у нас LEFT JOIN, то по выбранному join_key надо отобразить все поля из первой таблицы таблиц
# и присоединённые к ним из второй таблицы
# Если от второй таблицы нет данных, то пишем null
# Если есть данные от всех таблиц, то делаем для каждого элемента из первого списка 
# пишем каждый элемент второго => Декартово произведение
#
            if(!bagOne.isEmpty()){
		        if(bagTwo.isEmpty()){
		            for(String s : bagOne){
			            context.write(key, new Text(s+",null"));
		            }
		        }else{
		            for(String s1 : bagOne){
			            for(String s2 : bagTwo){
			                context.write(key, new Text(s1+","+s2));
			            }
		            }
		        }
            }
        }
    }
}
