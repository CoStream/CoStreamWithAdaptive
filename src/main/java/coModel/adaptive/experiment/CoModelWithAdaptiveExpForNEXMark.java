package coModel.adaptive.experiment;



public class CoModelWithAdaptiveExpForNEXMark {
    public static void main(String[] args) throws Exception {
        ExperimentWithAdaptiveExecutorForNEXMark experiment = new ExperimentWithAdaptiveExecutorForNEXMark("CoModelWithAdaptive", args);
        experiment.startExperiment();
        System.out.println("CoModel测试，采用堆外内存，且采用非迁移的方式实现复制数调整");
    }
}
