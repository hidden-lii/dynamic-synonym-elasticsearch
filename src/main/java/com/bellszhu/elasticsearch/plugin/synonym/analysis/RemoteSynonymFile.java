package com.bellszhu.elasticsearch.plugin.synonym.analysis;

import com.ibm.icu.text.CharsetDetector;
import com.ibm.icu.text.CharsetMatch;
import org.apache.http.Header;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;
import org.elasticsearch.analysis.common.ESSolrSynonymParser;
import org.elasticsearch.analysis.common.ESWordnetSynonymParser;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.env.Environment;

import java.io.*;
import java.nio.charset.Charset;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.ParseException;
import java.util.Objects;
import java.util.Optional;

/**
 * @author bellszhu
 */
public class RemoteSynonymFile implements SynonymFile {

    private static final String LAST_MODIFIED_HEADER = "Last-Modified";
    private static final String ETAG_HEADER = "ETag";
    private static final String INCREMENTAL = "Incremental";
    private static final String OFFSET = "Offset";

    private static final Logger logger = LogManager.getLogger("dynamic-synonym");

    private final CloseableHttpClient httpclient;

    private final String format;

    private final boolean expand;

    private final boolean lenient;

    private final Analyzer analyzer;

    private final Environment env;

    /**
     * Remote URL address
     */
    private final String location;

    private final boolean enableCallback;

    private final boolean multiple_files;

    private String lastModified;

    private String eTags;

    private boolean isIncremental = false;

    private final boolean incrementable;

    private Long offset;

    RemoteSynonymFile(
            Environment env, Analyzer analyzer, boolean expand, boolean lenient,
            String format, String location, boolean multiple_files, boolean callback, boolean incrementable
    ) {
        this.analyzer = analyzer;
        this.expand = expand;
        this.lenient = lenient;
        this.format = format;
        this.env = env;
        this.location = location;
        this.enableCallback = callback;
        this.multiple_files = multiple_files;
        this.incrementable = incrementable;

        this.httpclient = AccessController.doPrivileged((PrivilegedAction<CloseableHttpClient>) HttpClients::createDefault);

        isNeedReloadSynonymMap();
    }

    static SynonymMap.Builder getSynonymParser(
            Reader rulesReader, String format, boolean expand, boolean lenient, Analyzer analyzer
    ) throws IOException, ParseException {
        SynonymMap.Builder parser;
        if ("wordnet".equalsIgnoreCase(format)) {
            parser = new ESWordnetSynonymParser(true, expand, lenient, analyzer);
            ((ESWordnetSynonymParser) parser).parse(rulesReader);
        } else {
            parser = new ESSolrSynonymParser(true, expand, lenient, analyzer);
            ((ESSolrSynonymParser) parser).parse(rulesReader);
        }
        return parser;
    }

    @Override
    public SynonymMap reloadSynonymMap(SynonymMap now) {
        Reader rulesReader = null;
        try {
            logger.debug("start reload remote synonym from {}.", location);
            rulesReader = getReader();
            SynonymMap.Builder parser = getSynonymParser(rulesReader, format, expand, lenient, analyzer);
            SynonymMap current = parser.build();
            if (now != null && isIncremental && incrementable) {
                SynonymMap merged = merge(current, now);
                if (merged != null) {
                    current = merged;
                }
            }
            return current;
        } catch (Exception e) {
            logger.error("reload remote synonym {} error!", location, e);
            throw new IllegalArgumentException(
                    "could not reload remote synonyms file to build synonyms",
                    e);
        } finally {
            if (rulesReader != null) {
                try {
                    rulesReader.close();
                } catch (Exception e) {
                    logger.error("failed to close rulesReader", e);
                }
            }
        }
    }

    private SynonymMap merge(SynonymMap current, SynonymMap last) {
        try {
            SynonymMap.Builder mergedBuilder = new SynonymMap.Builder(true);
            StopWatch stopWatch = new StopWatch("merge synonym map");
            stopWatch.start("merge last");
            mergeSynonymMaps(mergedBuilder, last);
            stopWatch.stop();
            stopWatch.start("merge current");
            mergeSynonymMaps(mergedBuilder, current);
            stopWatch.stop();
            logger.info(stopWatch.prettyPrint());

            return mergedBuilder.build();
        } catch (Exception e) {
            logger.error("failed to merge synonym map, abort", e);
        }
        return null;
    }

    private void mergeSynonymMaps(SynonymMap.Builder targetBuilder, SynonymMap sourceMap) {
        try (Analyzer analyzer = createAnalyzer(sourceMap)) {
            FST<BytesRef> fst = sourceMap.fst;
            BytesRefFSTEnum<BytesRef> fstEnum = new BytesRefFSTEnum<>(fst);
            BytesRefFSTEnum.InputOutput<BytesRef> current;

            while ((current = fstEnum.next()) != null) {
                for (int i = 0; i < current.output.length; ++i) {
                    byte index = current.output.bytes[i];
                    BytesRef bytesRef = sourceMap.words.get(index, new BytesRef());
                    CharsRef charsRef = bytesRefToCharsRef(bytesRef);
                    processWord(targetBuilder, charsRef.toString(), analyzer);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to merge synonym map", e);
        }
    }

    private void processWord(SynonymMap.Builder targetBuilder, String word, Analyzer analyzer) {
        try (TokenStream tokenStream = analyzer.tokenStream("field", word)) {
            tokenStream.reset();
            CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
            while (tokenStream.incrementToken()) {
                String synonym = charTermAttribute.toString();
                if (!word.equals(synonym)) {
                    targetBuilder.add(new CharsRef(word), new CharsRef(synonym), true);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error processing word: " + word, e);
        }
    }

    private static Analyzer createAnalyzer(SynonymMap synonymMap) {
        return new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer source = new WhitespaceTokenizer();
                TokenStream tokenStream = new SynonymGraphFilter(source, synonymMap, true);
                return new TokenStreamComponents(source, tokenStream);
            }
        };
    }

    public static CharsRef bytesRefToCharsRef(BytesRef bytesRef) {
        // 使用CharsetDetector检测字节编码
        CharsetDetector charsetDetector = new CharsetDetector();
        charsetDetector.setText(bytesRef.bytes);
        CharsetMatch charsetMatch = charsetDetector.detect();

        // 将字节转换为字符串，使用检测到的编码
        String detectedEncoding = charsetMatch.getName();
        Charset charset = Charset.forName(detectedEncoding);
        String decodedString = new String(bytesRef.bytes, bytesRef.offset, bytesRef.length, charset);
        // 将字符串转换为CharsRef

        return new CharsRef(decodedString);
    }

    private CloseableHttpResponse executeHttpRequest(HttpUriRequest httpUriRequest) {
        return AccessController.doPrivileged((PrivilegedAction<CloseableHttpResponse>) () -> {
            try {
                return httpclient.execute(httpUriRequest);
            } catch (IOException e) {
                logger.error("Unable to execute HTTP request.", e);
            }
            return null;
        });
    }

    /**
     * Download custom terms from a remote server
     */
    public Reader getReader() {
        Reader reader;
        HttpGet get = new HttpGet(location);
        if (offset != null) {
            get.setHeader(OFFSET, offset.toString());
        }
        get.setConfig(RequestConfig.custom()
                .setConnectionRequestTimeout(10 * 1000)
                .setConnectTimeout(10 * 1000).setSocketTimeout(60 * 1000)
                .build());
        BufferedReader br = null;
        CloseableHttpResponse response = null;
        try {
            // todo multiple_files
            response = executeHttpRequest(get);
            if (response.getStatusLine().getStatusCode() == 200) {
                Header offset = response.getLastHeader(OFFSET);
                if (offset != null) {
                    this.offset = Long.parseLong(offset.getValue());
                }
                String charset = "UTF-8"; // 获取编码，默认为utf-8
                if (response.getEntity().getContentType().getValue().contains("charset=")) {
                    String contentType = response.getEntity().getContentType().getValue();
                    charset = contentType.substring(contentType.lastIndexOf('=') + 1);
                }

                br = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), charset));
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) {
                    logger.debug("reload remote synonym: {}", line);
                    sb.append(line).append(System.getProperty("line.separator"));
                }
                logger.info("fetched: " + sb);
                reader = new StringReader(sb.toString());
            } else {
                reader = new StringReader("");
            }
        } catch (Exception e) {
            logger.error("get remote synonym reader {} error!", location, e);
            reader = new StringReader("");
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException e) {
                logger.error("failed to close bufferedReader", e);
            }
            try {
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                logger.error("failed to close http response", e);
            }
        }
        return reader;
    }

    @Override
    public boolean isNeedReloadSynonymMap() {
        HttpHead head = AccessController.doPrivileged((PrivilegedAction<HttpHead>) () -> new HttpHead(location));
        head.setConfig(RequestConfig.custom()
                .setConnectionRequestTimeout(10 * 1000)
                .setConnectTimeout(10 * 1000).setSocketTimeout(15 * 1000)
                .build());
        // 设置请求头
        if (lastModified != null) {
            head.setHeader("If-Modified-Since", lastModified);
        }
        if (eTags != null) {
            head.setHeader("If-None-Match", eTags);
        }
        head.setHeader("LAST_ACTION", isIncremental ? "increase" : "update");
        if (offset != null) {
            head.setHeader(OFFSET, offset.toString());
        }
        boolean isReload = false;
        CloseableHttpResponse response = null;
        try {
            response = executeHttpRequest(head);
            if (response.getStatusLine().getStatusCode() == 200) { // 返回200 才做操作
                Header eTag = response.getLastHeader(ETAG_HEADER);
                Header lastModified = response.getLastHeader(LAST_MODIFIED_HEADER);
                if (eTag == null || lastModified == null) {
                    isReload = true;
                } else if (!lastModified.getValue().equalsIgnoreCase(this.lastModified) || !eTag.getValue().equalsIgnoreCase(eTags)) {
                    this.lastModified = lastModified.getValue();
                    this.eTags = eTag.getValue();
                    isReload = true;
                }
                Header incremental = response.getLastHeader(INCREMENTAL);
                this.isIncremental = incremental != null && incremental.getValue().equalsIgnoreCase("true");
                Header offset = response.getLastHeader(OFFSET);
                if (offset != null) {
                    long currentOffset = Long.parseLong(offset.getValue());
                    isReload = !Objects.equals(this.offset, currentOffset);
                }
            } else {
                logger.info("remote synonym {} return bad code {}", location, response.getStatusLine().getStatusCode());
            }
        } finally {
            try {
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                logger.error("failed to close http response", e);
            }
        }
        if (enableCallback) {
            sendCallback(isReload);
        }
        return isReload;
    }

    private void sendCallback(boolean isReload) {
        HttpHead head = AccessController.doPrivileged((PrivilegedAction<HttpHead>) () -> new HttpHead(location + "/callback"));
        head.setConfig(RequestConfig.custom()
                .setConnectionRequestTimeout(10 * 1000)
                .setConnectTimeout(10 * 1000).setSocketTimeout(15 * 1000)
                .build());
        head.setHeader("isReload", String.valueOf(isReload));
        head.setHeader(OFFSET, Optional.ofNullable(offset).orElse(0L).toString());
        CloseableHttpResponse response = null;
        try {
            response = executeHttpRequest(head);
            if (response.getStatusLine().getStatusCode() != 200) {
                logger.warn("callback return bad code {}", response.getStatusLine().getStatusCode());
            }
        } finally {
            try {
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                logger.error("failed to close http response", e);
            }
        }
    }
}
