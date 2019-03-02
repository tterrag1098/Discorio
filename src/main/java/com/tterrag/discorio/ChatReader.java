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
        boolean action;
    }
    
    private static final String TIMESTAMP_REGEX = "(?<date>\\d{4}-\\d{2}-\\d{2})\\s(?<time>\\d{2}:\\d{2}:\\d{2})";
    
    private static final Pattern CHAT_MSG = Pattern.compile(
            TIMESTAMP_REGEX + "\\s"
            + "\\[(?<type>CHAT|SHOUT)\\]\\s"
            + "(?!<server>)(?<user>\\S+)\\s*"
            + "(?:\\[(?<team>[^\\]]+)\\])?\\s*"
            + "(?:\\(shout\\))?:\\s*"
            + "(?<message>.+)$"
    );
    
    private static final Pattern JOIN_LEAVE_MSG = Pattern.compile(    
            TIMESTAMP_REGEX + "\\s"
            + "\\[(?<type>JOIN|LEAVE)\\]\\s"
            + "(?<user>\\S+)\\s"
            + "(?<message>.+)$"
    );

    private final String fileName;
    
    private Thread thread;
        
    public ChatReader(String fileName) {
        this.fileName = fileName;
    }

    public Flux<FactorioMessage> start() {
        EmitterProcessor<FactorioMessage> processor = EmitterProcessor.create(false);
        FluxSink<FactorioMessage> sink = processor.sink();
        restart(new TailerListenerAdapter() {
            @Override
            public void handle(String line) {
                line = line.trim();
                Matcher m = CHAT_MSG.matcher(line);
                if (m.matches()) {
                    String type = m.group("type");
                    if (type.equals("SHOUT") || m.group("team") == null) {
                        sink.next(new FactorioMessage(m.group("user"), m.group("message"), false));
                    }
                    return;
                }
                m = JOIN_LEAVE_MSG.matcher(line);
                if (m.matches()) {
                    sink.next(new FactorioMessage(m.group("user"), m.group("message"), true));
                }
            }
          
            @Override
            public void handle(Exception ex) {
                sink.error(ex);
                restart(this);
            }
        });
        
        return processor;
    }

    protected void restart(TailerListenerAdapter adapter) {
        if (thread != null) {
            if (thread.isAlive()) { 
                thread.interrupt();
            }
        }
        Tailer tailer = new Tailer(new File(fileName), adapter, 1000, true);
        thread = new Thread(tailer, "Factorio chat reader");
        thread.setDaemon(true);
        thread.start();
    }
}
