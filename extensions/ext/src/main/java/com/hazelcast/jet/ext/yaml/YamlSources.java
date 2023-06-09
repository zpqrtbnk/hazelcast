package com.hazelcast.jet.ext.yaml;

import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.logging.ILogger;

public class YamlSources implements JobBuilderExtension {

    @Override
    public void register(JobBuilder jobBuilder) {

        jobBuilder.registerSource("map-journal", YamlSources::sourceMapJournal);
    }

    private static Object sourceMapJournal(Pipeline pipelineContext, String name, YamlMapping properties, ILogger logger) throws JobBuilderException {

        String mapName = YamlUtils.getProperty(properties, "map-name");
        String initialPositionString = YamlUtils.getProperty(properties, "journal-initial-position");
        //JournalInitialPosition initialPosition = getProperty(properties, "journal-initial-position");
        JournalInitialPosition initialPosition = JournalInitialPosition.valueOf(initialPositionString);
        logger.fine("  map-name: " + mapName);
        logger.fine("  initial-position: " + initialPosition);
        StreamSource source = Sources.mapJournal(mapName, initialPosition);
        if (source == null) logger.fine("MEH source");
        if (pipelineContext == null) logger.fine("MEH pipelineContext");
        return pipelineContext.readFrom(source).withIngestionTimestamps();
    }
}
