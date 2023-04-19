/**
 *
 */
package com.bellszhu.elasticsearch.plugin.synonym.analysis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.ParseException;

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
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.elasticsearch.analysis.common.ESSolrSynonymParser;
import org.elasticsearch.analysis.common.ESWordnetSynonymParser;
import org.elasticsearch.env.Environment;

/**
 * @author bellszhu
 */
public class RemoteSynonymFile implements SynonymFile {

    private static final String LAST_MODIFIED_HEADER = "Last-Modified";
    private static final String ETAG_HEADER = "ETag";

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

    private final boolean multiple_files;

    private String lastModified;

    private String eTags;

    RemoteSynonymFile(
            Environment env, Analyzer analyzer, boolean expand, boolean lenient,
            String format, String location, boolean multiple_files
    ) {
        this.analyzer = analyzer;
        this.expand = expand;
        this.lenient = lenient;
        this.format = format;
        this.env = env;
        this.location = location;
        this.multiple_files = multiple_files;

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
    public SynonymMap reloadSynonymMap() {
        Reader rulesReader = null;
        try {
            logger.debug("start reload remote synonym from {}.", location);
            rulesReader = getReader();
            SynonymMap.Builder parser = getSynonymParser(rulesReader, format, expand, lenient, analyzer);
            return parser.build();
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
        get.setConfig(RequestConfig.custom()
                .setConnectionRequestTimeout(10 * 1000)
                .setConnectTimeout(10 * 1000).setSocketTimeout(60 * 1000)
                .build());
        BufferedReader br = null;
        CloseableHttpResponse response = null;
        try {
            response = executeHttpRequest(get);
            if (response.getStatusLine().getStatusCode() == 200) {
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
        sendCallback(isReload);
        return isReload;
    }

    private void sendCallback(boolean isReload) {
        HttpHead head = AccessController.doPrivileged((PrivilegedAction<HttpHead>) () -> new HttpHead(location + "/callback"));
        head.setConfig(RequestConfig.custom()
                .setConnectionRequestTimeout(10 * 1000)
                .setConnectTimeout(10 * 1000).setSocketTimeout(15 * 1000)
                .build());
        head.setHeader("isReload", String.valueOf(isReload));
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
