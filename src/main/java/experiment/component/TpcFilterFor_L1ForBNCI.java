package experiment.component;

import org.apache.flink.api.common.functions.FilterFunction;

public class TpcFilterFor_L1ForBNCI implements FilterFunction<String> {
    @Override
    public boolean filter(String value) throws Exception {
        String[] splitValueString = value.split("\\|");
        String shipMode = splitValueString[14];
        double quantity = Double.parseDouble(splitValueString[4]);
        //True for values that should be retained, false for values to be filtered out.
        if (shipMode.equals("TRUCK") && quantity > 48) {
            return true;
        } else {
            return false;
        }
    }
}
