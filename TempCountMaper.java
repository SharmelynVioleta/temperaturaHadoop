import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TempCountMaper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String[] valores = value.toString().split("\\n");

        for (String fila : valores) {
            String[] items = fila.toString().split("\\t");
           
	   int temperatura = Integer.parseInt(items[1]);
	    String fecha = items[0];
	    
	    IntWritable t = new IntWritable(temperatura);
            Text f = new Text(fecha);
            context.write(f, t);
        }
    }
}
