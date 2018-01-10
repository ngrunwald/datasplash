package datasplash.fns;

import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.io.FileBasedSink.OutputFileHints;

import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.util.Map;
import clojure.lang.IFn;
import clojure.java.api.Clojure;

public final class FileNamePolicy extends FileBasedSink.FilenamePolicy {

    private static final long serialVersionUID = 0;
    private final IFn windowedFn;
    private final IFn unwindowedFn;
    private final ResourceId prefix;
    private final String raw_prefix;

        public FileNamePolicy (Map<String, Object> params_map) {
      super();
      windowedFn = (IFn) params_map.get("windowed-fn");
      unwindowedFn = (IFn) params_map.get("unwindowed-fn");
      raw_prefix = (String) params_map.get("prefix");
      prefix = FileBasedSink.convertToFileResourceIfPossible(raw_prefix);
    }

    public ResourceId windowedFilename(int shardNumber, int numShards,BoundedWindow window, PaneInfo paneInfo, OutputFileHints outputFileHints) {
        String fileName = (String) windowedFn.invoke(shardNumber, numShards, window, outputFileHints.getSuggestedFilenameSuffix());
        return prefix.getCurrentDirectory().resolve(fileName, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
    }

    public ResourceId unwindowedFilename(int shardNumber, int numShards, OutputFileHints outputFileHints) {
        String fileName = (String) unwindowedFn.invoke(shardNumber, numShards, outputFileHints.getSuggestedFilenameSuffix());
        return prefix.getCurrentDirectory().resolve(fileName, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
    }
}

