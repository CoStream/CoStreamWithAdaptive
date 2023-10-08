package coModel.experiment;

import common.GeneralParameters;
import experiment.ExperimentExecutorForSyntheticAndEqual;

public class CoModelExpForSyntDatasets {
    public static void main(String[] args) throws Exception {
//        ParseExperimentParametersTool.parseExpParameters(args);
        ExperimentExecutorForSyntheticAndEqual experiment = new ExperimentExecutorForSyntheticAndEqual(GeneralParameters.CO_MODEL, args);
        experiment.startExperiment();
        System.out.println("CoModel针对合成数据集的实验启动成功");
    }
}
