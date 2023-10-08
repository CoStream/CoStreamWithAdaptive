package experiment.component;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple4;

public class TpcFilterFor_L1 implements FilterFunction<Tuple4<String, String, String, String>> {
    @Override
    public boolean filter(Tuple4<String, String, String, String> value) throws Exception {
        String[] splitValueString = value.f3.split("\\|");
        String shipMode = splitValueString[14];
        double quantity = Double.parseDouble(splitValueString[4]);
        //True for values that should be retained, false for values to be filtered out.
        if (shipMode.equals("TRUCK") && quantity > 45) {
            return true;
        } else {
            return false;
        }
    }
}
