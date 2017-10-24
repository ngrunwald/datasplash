package datasplash.fns;

import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy.Context;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy.WindowedContext;

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

    public FileNamePolicy(Map<String, IFn> fns_map) {
      super();
      windowedFn = fns_map.get("windowed-fn");
      unwindowedFn = fns_map.get("unwindowed-fn");
    }

    public ResourceId windowedFilename(ResourceId outputDirectory, FileBasedSink.FilenamePolicy.WindowedContext c, String extension) {
      String fileName = (String) windowedFn.invoke(c, extension);
      return outputDirectory.resolve(fileName, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
    }

    public ResourceId unwindowedFilename(ResourceId outputDirectory, FileBasedSink.FilenamePolicy.Context c, String extension) {
      String fileName = (String) unwindowedFn.invoke(c, extension);
      return outputDirectory.resolve(fileName, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
    }
}

