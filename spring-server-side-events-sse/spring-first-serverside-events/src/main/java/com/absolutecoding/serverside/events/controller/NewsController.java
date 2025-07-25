package com.absolutecoding.serverside.events.controller;

import com.absolutecoding.serverside.events.model.News;
import com.absolutecoding.serverside.events.model.Article;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Gatherers;

@Controller
@RequestMapping("/news")
public class NewsController {

    @Value("${app.use.java.22}")
    private boolean useJava22;

    @Value("${app.news.file.name}")
    private String newsFile;

    @GetMapping(value = "/reactive/emitter", produces = "text/event-stream")
    public Flux<ServerSentEvent<List<Article>>> news() throws IOException {
        System.out.println("Java version used: " + useJava22);
        final String json = readJsonFile();
        final News news = getNews(json);

        return Flux.fromIterable(news.articles())
                .window(3)
                .delayElements(Duration.ofSeconds(1))
                .flatMap(Flux::collectList)
                .map(articles -> ServerSentEvent.<List<Article>>builder()
                        .event("news")
                        .data(articles)
                        .build());

    }

    private final ExecutorService nonBlockingService = Executors.newCachedThreadPool();

    @GetMapping(value = "/traditional/emitter")
    public SseEmitter newsEmitter() throws IOException {
        System.out.println("Java version used: " + useJava22);
        SseEmitter emitter = new SseEmitter();
        final String json = readJsonFile();
        final News news = getNews(json);

        nonBlockingService.execute(() -> {
            try {
                if(useJava22) {
                    news.articles().stream()
                            .gather(Gatherers.windowFixed(3))
                            .forEach(articles -> {
                                try {
                                    emitter.send(articles);
                                    Thread.sleep(1000);
                                } catch (Exception e) {
                                    emitter.completeWithError(e);
                                }

                            });
                    emitter.complete();
                } else {
                    news.articles().stream()
                            .collect(Collectors.collectingAndThen(Collectors.toList(), list -> {
                                List<List<Article>> batches = new ArrayList<>();
                                for (int i = 0; i < list.size(); i += 3) {
                                    int end = Math.min(i + 3, list.size());
                                    batches.add(list.subList(i, end));
                                }
                                return batches;
                            }))
                            .forEach(articles -> {
                                try {
                                    emitter.send(articles); // Send each batch
                                    Thread.sleep(1000);     // Simulate delay
                                } catch (Exception e) {
                                    emitter.completeWithError(e);
                                }
                            });
                }
            } catch (Exception ex) {
                emitter.completeWithError(ex);
            }
        });
        return emitter;
    }

    private String readJsonFile() throws IOException {
        Resource resource = new ClassPathResource(newsFile);
        Path path = resource.getFile().toPath();
        return Files.readString(path);
    }

    private News getNews(String newsStr) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();

        /*
         * Ignore all the other properties which we do not require coming news.jsn file but not require in the News model class
         */
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return objectMapper.readValue(newsStr, News.class);
    }
}
