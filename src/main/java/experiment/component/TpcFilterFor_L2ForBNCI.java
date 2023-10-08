package experiment.component;

import org.apache.flink.api.common.functions.FilterFunction;

//L2.shipinstruct=’NONE’
public class TpcFilterFor_L2ForBNCI implements FilterFunction<String> {
    @Override
    public boolean filter(String value) throws Exception {
        String[] splitValueString = value.split("\\|");
        String shipInstruct = splitValueString[13];
        //True for values that should be retained, false for values to be filtered out.
        if (shipInstruct.equals("NONE")) {
            return true;
        } else {
            return false;
        }
    }
}
