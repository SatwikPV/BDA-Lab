import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
public class JoinReducer extends MapReduceBase implements Reducer&lt;TextPair, Text,
Text,
Text > {
@Override
public void reduce (TextPair key, Iterator < Text&gt; values, OutputCollector < Text, Text >
output, Reporter reporter)
throws IOException
{
Text nodeId = new Text(values.next());
while (values.hasNext()) {
Text node = values.next();
Text outValue = new Text(nodeId.toString() + &quot;\t\t&quot; + node.toString());
output.collect(key.getFirst(), outValue);
}
}
}