package com.bellszhu.elasticsearch.plugin.synonym.analysis;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.util.*;
import org.apache.lucene.util.fst.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.*;

import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * @author bellszhu
 */
public class DynamicSynonymTokenFilterFactory extends AbstractTokenFilterFactory {

    private static final Logger logger = LogManager.getLogger("dynamic-synonym");

    /**
     * Static id generator
     */
    private static final AtomicInteger id = new AtomicInteger(1);
    private static final ScheduledExecutorService pool = Executors.newScheduledThreadPool(1, r -> {
        Thread thread = new Thread(r);
        thread.setName("monitor-synonym-Thread-" + id.getAndAdd(1));
        return thread;
    });
    private volatile ScheduledFuture<?> scheduledFuture;

    private final String location;
    private final boolean expand;
    private final boolean lenient;
    private final String format;
    private final int interval;
    private final boolean multiple_files;
    private final boolean incrementable;
    protected SynonymMap synonymMap;
    protected Map<AbsSynonymFilter, Integer> dynamicSynonymFilters = new WeakHashMap<>();
    protected final Environment environment;
    protected final AnalysisMode analysisMode;

    public DynamicSynonymTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);

        this.location = settings.get("synonyms_path");
        if (this.location == null) {
            throw new IllegalArgumentException("dynamic synonym requires `synonyms_path` to be configured");
        }
        if (settings.get("ignore_case") != null) {
        }

        this.interval = settings.getAsInt("interval", 60);
        this.expand = settings.getAsBoolean("expand", true);
        this.lenient = settings.getAsBoolean("lenient", false);
        // 开启多查询模式,根据header中的offset分多次查询
        this.multiple_files = settings.getAsBoolean("multiple_files", false);
        // 开启增量更新模式
        this.incrementable = settings.getAsBoolean("incrementable", false);
        this.format = settings.get("format", "");
//        settings.getAsBoolean("updateable", false) ? AnalysisMode.SEARCH_TIME :
        this.analysisMode = AnalysisMode.ALL;
        this.environment = env;
    }

    @Override
    public AnalysisMode getAnalysisMode() {
        return this.analysisMode;
    }


    @Override
    public TokenStream create(TokenStream tokenStream) {
        throw new IllegalStateException(
                "Call getChainAwareTokenFilterFactory to specialize this factory for an analysis chain first");
    }

    public TokenFilterFactory getChainAwareTokenFilterFactory(
            TokenizerFactory tokenizer,
            List<CharFilterFactory> charFilters,
            List<TokenFilterFactory> previousTokenFilters,
            Function<String, TokenFilterFactory> allFilters
    ) {
        final Analyzer analyzer = buildSynonymAnalyzer(tokenizer, charFilters, previousTokenFilters, allFilters);
        synonymMap = buildSynonyms(analyzer);
        final String name = name();
        return new TokenFilterFactory() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                // fst is null means no synonyms
                if (synonymMap.fst == null) {
                    return tokenStream;
                }
                DynamicSynonymFilter dynamicSynonymFilter = new DynamicSynonymFilter(tokenStream, synonymMap, false);
                dynamicSynonymFilters.put(dynamicSynonymFilter, 1);

                return dynamicSynonymFilter;
            }

            @Override
            public TokenFilterFactory getSynonymFilter() {
                // In order to allow chained synonym filters, we return IDENTITY here to
                // ensure that synonyms don't get applied to the synonym map itself,
                // which doesn't support stacked input tokens
                return IDENTITY_FILTER;
            }

            @Override
            public AnalysisMode getAnalysisMode() {
                return analysisMode;
            }
        };
    }

    Analyzer buildSynonymAnalyzer(
            TokenizerFactory tokenizer,
            List<CharFilterFactory> charFilters,
            List<TokenFilterFactory> tokenFilters,
            Function<String, TokenFilterFactory> allFilters
    ) {
        return new CustomAnalyzer(
                tokenizer,
                charFilters.toArray(new CharFilterFactory[0]),
                tokenFilters.stream().map(TokenFilterFactory::getSynonymFilter).toArray(TokenFilterFactory[]::new)
        );
    }

    SynonymMap buildSynonyms(Analyzer analyzer) {
        try {
            SynonymMap currentMap = getSynonymFile(analyzer).reloadSynonymMap();
            if (synonymMap == null || !incrementable) {
                return currentMap;
            }
            // 增量同步
            BytesRefHash mergedWords = new BytesRefHash();
            for (int i = 0; i < synonymMap.words.size(); ++i) {
                BytesRef word = synonymMap.words.get(i, new BytesRef());
                mergedWords.add(word);
            }
            for (int i = 0; i < currentMap.words.size(); ++i) {
                BytesRef word = currentMap.words.get(i, new BytesRef());
                mergedWords.add(word);
            }

            Outputs<BytesRef> outputs = ByteSequenceOutputs.getSingleton();
            Builder<BytesRef> builder = new Builder<>(FST.INPUT_TYPE.BYTE4, outputs);
            for (int i = 0; i < mergedWords.size(); ++i) {
                BytesRef word = mergedWords.get(i, new BytesRef());
                builder.add(Util.toIntsRef(word, new IntsRefBuilder()), outputs.getNoOutput());
            }

            int fixedMaxHorizontalContext = Math.max(synonymMap.maxHorizontalContext, currentMap.maxHorizontalContext);
            return new SynonymMap(builder.finish(), mergedWords, fixedMaxHorizontalContext);
        } catch (Exception e) {
            logger.error("failed to build synonyms", e);
            throw new IllegalArgumentException("failed to build synonyms", e);
        }
    }

    SynonymFile getSynonymFile(Analyzer analyzer) {
        try {
            SynonymFile synonymFile;
            if (location.startsWith("http://") || location.startsWith("https://")) {
                synonymFile = new RemoteSynonymFile(environment, analyzer, expand, lenient, format, location, multiple_files);
            } else {
                synonymFile = new LocalSynonymFile(environment, analyzer, expand, lenient, format, location);
            }
            if (scheduledFuture == null) {
                scheduledFuture = pool.scheduleAtFixedRate(new Monitor(synonymFile), interval, interval, TimeUnit.SECONDS);
            }
            return synonymFile;
        } catch (Exception e) {
            logger.error("failed to get synonyms: " + location, e);
            throw new IllegalArgumentException("failed to get synonyms : " + location, e);
        }
    }

    public class Monitor implements Runnable {

        private final SynonymFile synonymFile;

        Monitor(SynonymFile synonymFile) {
            this.synonymFile = synonymFile;
        }

        @Override
        public void run() {
            if (synonymFile.isNeedReloadSynonymMap()) {
                synonymMap = synonymFile.reloadSynonymMap();
                for (AbsSynonymFilter dynamicSynonymFilter : dynamicSynonymFilters.keySet()) {
                    dynamicSynonymFilter.update(synonymMap);
                    logger.debug("success reload synonym");
                }
            }
        }
    }

}
