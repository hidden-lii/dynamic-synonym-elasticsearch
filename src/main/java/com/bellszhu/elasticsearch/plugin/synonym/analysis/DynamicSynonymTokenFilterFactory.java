package com.bellszhu.elasticsearch.plugin.synonym.analysis;


import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisModule;

import java.io.IOException;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author bellszhu
 */
public class DynamicSynonymTokenFilterFactory extends
        AbstractTokenFilterFactory {

    /**
     * Static id generator
     */
    private static final AtomicInteger id = new AtomicInteger(1);
    public static Logger logger = ESLoggerFactory.getLogger("dynamic-synonym");
    private static ScheduledExecutorService pool = Executors.newScheduledThreadPool(1,
            new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r);
                    thread.setName("monitor-synonym-Thread-" + id.getAndAdd(1));
                    return thread;
                }
            });
    private final String location;
    private final boolean ignoreCase;
    private final boolean expand;
    private final String format;
    private final int interval;
    private volatile ScheduledFuture<?> scheduledFuture;
    private SynonymMap synonymMap;
    private Map<DynamicSynonymFilter, Integer> dynamicSynonymFilters = new WeakHashMap<DynamicSynonymFilter, Integer>();

    public DynamicSynonymTokenFilterFactory(
            IndexSettings indexSettings,
            Environment env,
            String name,
            Settings settings,
            AnalysisRegistry analysisRegistry
    ) throws IOException {

        super(indexSettings, name, settings);


        this.location = settings.get("synonyms_path");
        if (this.location == null) {
            throw new IllegalArgumentException(
                    "dynamic synonym requires `synonyms_path` to be configured");
        }

        this.interval = settings.getAsInt("interval", 60);
        this.ignoreCase = settings.getAsBoolean("ignore_case", false);
        this.expand = settings.getAsBoolean("expand", true);
        this.format = settings.get("format", "");

        String tokenizerName = settings.get("tokenizer", "whitespace");


        AnalysisModule.AnalysisProvider<TokenizerFactory> tokenizerFactoryFactory =
                analysisRegistry.getTokenizerProvider(tokenizerName, indexSettings);
        if (tokenizerFactoryFactory == null) {
            throw new IllegalArgumentException("failed to find tokenizer [" + tokenizerName + "] for synonym token filter");
        }
        final TokenizerFactory tokenizerFactory = tokenizerFactoryFactory.get(indexSettings, env, tokenizerName,

                AnalysisRegistry.getSettingsFromIndexSettings(indexSettings, AnalysisRegistry.INDEX_ANALYSIS_TOKENIZER + "." + tokenizerName));


        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = tokenizerFactory == null ? new WhitespaceTokenizer() : tokenizerFactory.create();
                TokenStream stream = ignoreCase ? new LowerCaseFilter(tokenizer) : tokenizer;
                return new TokenStreamComponents(tokenizer, stream);
            }
        };

        SynonymFile synonymFile;
        if (location.startsWith("http://") || location.startsWith("https://")) {
            synonymFile = new RemoteSynonymFile(env, analyzer, expand, format,
                    location);
        } else {
            synonymFile = new LocalSynonymFile(env, analyzer, expand, format,
                    location);
        }
        synonymMap = synonymFile.reloadSynonymMap();

        scheduledFuture = pool.scheduleAtFixedRate(new Monitor(synonymFile),
                interval, interval, TimeUnit.SECONDS);

    }


    @Override
    public TokenStream create(TokenStream tokenStream) {
        DynamicSynonymFilter dynamicSynonymFilter = new DynamicSynonymFilter(
                tokenStream, synonymMap, ignoreCase);
        dynamicSynonymFilters.put(dynamicSynonymFilter, 1);

        // fst is null means no synonyms
        return synonymMap.fst == null ? tokenStream : dynamicSynonymFilter;
    }

    public class Monitor implements Runnable {

        private SynonymFile synonymFile;

        public Monitor(SynonymFile synonymFile) {
            this.synonymFile = synonymFile;
        }

        @Override
        public void run() {
            if (synonymFile.isNeedReloadSynonymMap()) {
                synonymMap = synonymFile.reloadSynonymMap();
                for (DynamicSynonymFilter dynamicSynonymFilter : dynamicSynonymFilters
                        .keySet()) {
                    dynamicSynonymFilter.update(synonymMap);
                    logger.info("success reload synonym");
                }
            }
        }
    }

}