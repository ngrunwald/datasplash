package datasplash.pipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;

public class PipelineWithOptions extends Pipeline {

    private final PipelineOptions pipelineOptions;

    private PipelineWithOptions(PipelineOptions options) {
        super(options);
        pipelineOptions = options;
    }

    public PipelineOptions getPipelineOptions() {
        return pipelineOptions;
    }

    public static PipelineWithOptions create (PipelineOptions options) {
        return new PipelineWithOptions(options);
    }
}
