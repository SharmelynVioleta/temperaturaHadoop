import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TempCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {


    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int suma = 0;
        int contador = 0;

        for (IntWritable valor : values) {
            suma += valor.get();
            contador += 1;
        }

        int promedio = suma / contador;
        context.write(key, new IntWritable(promedio));
    }
}
