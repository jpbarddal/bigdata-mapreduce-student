package advanced.customwritable;

import basic.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AverageTemperature {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "forestfire-estudante");

        // Registrar as classes
        j.setJarByClass(AverageTemperature.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);
        j.setCombinerClass(CombinerForAverage.class);

        // Definir os tipos de saida (MAP e REDUCE)
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FireAvgTempWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        // Cadastrar os arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * 1o Parametro: tipo da chave de entrada
     * 2o Parametro: tipo do valor de entrada
     *
     * ----------------------------------------
     *
     * 3o Parametro: tipo da chave de saida
     * 4o Parametro: tipo do valor de saida
     */
    public static class MapForAverage extends Mapper<LongWritable, Text,
            Text, FireAvgTempWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // Funcao de map eh executada por bloco, e na realidade eh executada por LINHA

            // Obter o conteudo da linha
            String linha = value.toString();

            // A partir da linha, obter apenas a coluna 8
            String[] colunas = linha.split(",");
            float temperatura = Float.parseFloat(colunas[8]);

            // Emitir <chave, valor>, sendo que a chave deve ser comum
            //                        e o valor deve ser composto (soma = vlr, n=1)
            con.write(new Text("auxiliar"), new FireAvgTempWritable(1, temperatura));

            // COMENTARIO IMPORTANTE!!!
            // usando sempre a mesma chave para garantir que todos os resultados
            // originados de maps diferentes cheguem no mesmo reduce

        }
    }


    // Recebe como entrada <chave, lista de valores>
    // Chave = "auxiliar"
    // Lista de Valores = Lista que contempla diferentes FireAvgTempWritables (n, soma)
    // Objetivo do reduce:
    // Somar todas as somas
    // Somas todos os Ns
    // Dividir a soma das somas pela soma dos Ns
    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable,
            Text, FloatWritable> {

        // (n=1, soma= temperatura)
        // numero de linhas do arquivo

        // Funcao de reduce
        public void reduce(Text word, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {

            // Somando Ns e Somas
            int somaN = 0;
            float somaSomas = 0.0f;

            for(FireAvgTempWritable obj : values){
                somaN += obj.getN();
                somaSomas += obj.getSoma();
            }

            // calculando a media
            float media = somaSomas / somaN;

            // emitir o resultado final (media = X)
            con.write(new Text("media = "), new FloatWritable(media));


        }
    }

    public static class CombinerForAverage extends Reducer<Text, FireAvgTempWritable, Text, FireAvgTempWritable>{
        public void reduce(Text key, Iterable<FireAvgTempWritable> valores, Context con)
                throws IOException, InterruptedException {
            // aglutinar os valores em um objeto unico (n = soma dos ns vistos no map,
            //                                          temperatura = soma das temperaturas vistas no map)
            int somaNs = 0;
            float somaTemps = 0.0f;

            for(FireAvgTempWritable obj : valores){
                somaNs += obj.getN();
                somaTemps += obj.getSoma();
            }

            // emitir <chave, (somaNs, somaTemps)>
            con.write(key, new FireAvgTempWritable(somaNs, somaTemps));

        }
    }

}
