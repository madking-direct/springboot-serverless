package madking.spring.serverless;

import com.amazonaws.serverless.exceptions.ContainerInitializationException;
import com.amazonaws.serverless.proxy.internal.testutils.AwsProxyRequestBuilder;
import com.amazonaws.serverless.proxy.model.AwsProxyRequest;
import com.amazonaws.serverless.proxy.model.AwsProxyResponse;
import com.amazonaws.serverless.proxy.spring.SpringBootLambdaContainerHandler;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class StreamLambdaHandler implements RequestStreamHandler {
    private static SpringBootLambdaContainerHandler<AwsProxyRequest, AwsProxyResponse> handler;
    private static Logger logger = LoggerFactory.getLogger(StreamLambdaHandler.class);

    private static ObjectMapper objectMapper = new ObjectMapper();
    private static boolean redirect = true;

    static {
        try {
            handler = SpringBootLambdaContainerHandler.getAwsProxyHandler(Application.class);
            // For applications that take longer than 10 seconds to start, use the async builder:
            // long startTime = Instant.now().toEpochMilli();
            // handler = new SpringBootProxyHandlerBuilder()
            //                    .defaultProxy()
            //                    .asyncInit(startTime)
            //                    .springBootApplication(Application.class)
            //                    .buildAndInitialize();
        } catch (ContainerInitializationException e) {
            // if we fail here. We re-throw the exception to force another cold start
            e.printStackTrace();
            throw new RuntimeException("Could not initialize Spring Boot application", e);
        }
    }

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context)
            throws IOException {

        inputStream = proxyRedirect(inputStream);
        handler.proxyStream(inputStream, outputStream, context);
    }

    private InputStream proxyRedirect(InputStream inputStream) throws IOException {

        if (!redirect)
            return inputStream;

        String buffer = IOUtils.toString(inputStream, StandardCharsets.UTF_8.name());
        logger.info("handleRequest - {}", buffer);
        JsonNode jsonNode = objectMapper.readTree(buffer);
        String bucket = jsonNode.path("Records").path(0).path("s3").path("bucket").get("name").asText();
        String key = jsonNode.path("Records").path(0).path("s3").path("object").get("key").asText();

        return new AwsProxyRequestBuilder("/" + bucket + "/" + key, HttpMethod.GET)
                .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON)
                .header("Content-Type", "UTF-8")
                .buildStream();
    }
}