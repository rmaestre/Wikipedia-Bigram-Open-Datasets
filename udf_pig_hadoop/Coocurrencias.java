package myudfs;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Coocurrencias extends EvalFunc<DataBag> {
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();

    public DataBag exec(Tuple input) throws IOException
    {
        try {
            // Get the input
            String str = (String) input.get(0);
            // Stop words to match
            String sw1 = (String) input.get(1);
            String sw2 = (String) input.get(2);
            // Check if input is not null or empty
            if (str != null && str.length() > 0){
                // Input String to lower case
                str = str.toLowerCase(); 
                // Match  COOCURRENCES pattern
                Pattern pattern = Pattern.compile(String.format("(%s %s)|(%s \\w+ %s)|(%s)|(%s)" , sw1 , sw2 , sw1 , sw2 , sw1 , sw2));
                Matcher matcher = pattern.matcher(str);
                // Data structure for the output
                DataBag output = mBagFactory.newDefaultBag();
                // Add all matches
                while (matcher.find()) {
                    output.add(mTupleFactory.newTuple(String.format("%s" , matcher.group(0).toString())));
                }
                // Return the output
                return output;
            }
        } catch (ExecException ee) {
            // error handling goes here
        }
		return null;
    }
}
