package com.tterrag.discorio;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;

import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

@Slf4j
public class ChatReader {
    
    @Value
    public static class FactorioMessage {
        
        String username;
        String message;
    }
    
    private static final Pattern CHAT_MSG = Pattern.compile(
              "(?<date>\\d{4}-\\d{2}-\\d{2})\\s"
            + "(?<time>\\d{2}:\\d{2}:\\d{2})\\s"
            + "\\[(?<type>CHAT|SHOUT)\\]\\s"
            + "(?!<server>)(?<user>\\S+)\\s*"
            + "(?:\\[(?<team>[^\\]]+)\\])?\\s*"
            + "(?:\\(shout\\))?:\\s*"
            + "(?<message>.+)$"
    );

    private final String fileName;
        
    public ChatReader(String fileName) {
        this.fileName = fileName;
    }

    public Flux<FactorioMessage> start() {
        EmitterProcessor<FactorioMessage> processor = EmitterProcessor.create(false);
        FluxSink<FactorioMessage> sink = processor.sink();
        Tailer tailer = new Tailer(new File(fileName), new TailerListenerAdapter() {
            @Override
            public void handle(String line) {
                Matcher m = CHAT_MSG.matcher(line.trim());
                log.info("Saw input");
                if (m.matches()) {
                    String type = m.group("type");
                    log.info("Input matched, type: {}", type);
                    if (type.equals("SHOUT") || m.group("team") == null) {
                        log.info("Visible chat, sending to discord...");
                        sink.next(new FactorioMessage(m.group("user"), m.group("message")));
                    }
                }
            }
          
            @Override
            public void handle(Exception ex) {
                sink.error(ex);
            }
        }, 1000, true);
        
        Thread t = new Thread(tailer, "Factorio chat reader");
        t.setDaemon(true);
        t.start();
        
        return processor;
    }
}
