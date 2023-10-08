package coModel.experiment;

import coModel.CoModelParameters;
import common.GeneralParameters;
import experiment.ExperimentExecutorForCorrect;

public class CoModelExpForCorrect {
    public static void main(String[] args) throws Exception {
        ExperimentExecutorForCorrect experiment = new ExperimentExecutorForCorrect(GeneralParameters.CO_MODEL, CoModelParameters.TOTAL_NUM_OF_ROUTER);
        experiment.startExperiment();
        System.out.println("CoModel正确性验证实验启动成功");
    }
}
