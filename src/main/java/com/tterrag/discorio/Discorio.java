package com.tterrag.discorio;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.tterrag.discorio.ChatReader.FactorioMessage;

import discord4j.core.DiscordClient;
import discord4j.core.DiscordClientBuilder;
import discord4j.core.event.domain.message.MessageCreateEvent;
import discord4j.core.object.entity.Channel;
import discord4j.core.object.entity.TextChannel;
import discord4j.core.object.util.Snowflake;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Slf4j
public class Discorio {
    
    @ToString
    private static class Arguments {
        @Parameter(names = { "-a", "--auth" }, description = "The Discord app key to authenticate with.", required = true)
        private String authKey;
        
        @Parameter(names = { "-f", "--file" }, description = "The path to the server log file.", required = true)
        private String fileName;
        
        @Parameter(names = { "-o", "--output" }, description = "The location of the server input pipe.", required = true)
        private String outputFile;
    }
    
    private static Arguments args;
    
    public static void main(String[] argv) {
        
        args = new Arguments();
        JCommander.newBuilder().addObject(args).build().parse(argv);
        
        System.out.println(args);
        
        Hooks.onOperatorDebug();

        DiscordClient client = new DiscordClientBuilder(args.authKey).build();
                
        Mono<Void> messageListener = client.getEventDispatcher().on(MessageCreateEvent.class)
                .log()
                .filter(e -> e.getMessage().getAuthor().map(u -> !u.isBot()).orElse(true))
                .filter(e -> e.getMessage().getContent().isPresent())
                .filterWhen(e -> e.getMessage().getChannel().map(Channel::getId).map(s -> s.asLong() == 205168854240985088L))
                .flatMap(Discorio::sendToFactorio)
                .then();

        ChatReader reader = new ChatReader(args.fileName);
        
        @SuppressWarnings("null") 
        Mono<Void> chatListener = reader.start()
                .onErrorResume(t -> Mono.just(new FactorioMessage("ERROR", t.toString(), false)).then(Mono.error(t)))
                                                            // TODO take channel as command/arg
                .transform(flatZipWith(client.getChannelById(Snowflake.of(205168854240985088L)).cast(TextChannel.class).cache().repeat(), 
                         (m, c) -> c.createMessage(m.isAction() ? ("*" + m.getUsername() + " " + m.getMessage() + "*") : "<" + m.getUsername() + "> " + m.getMessage())))
                .subscribeOn(Schedulers.elastic(), false)
                .then();
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> client.logout().block()));
        
        Mono.zip(client.login(),  messageListener, chatListener).block();
    }

    private static Mono<Void> sendToFactorio(MessageCreateEvent evt) {
        return Mono.fromCallable(() -> {
            try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(args.outputFile))) {
                out.write(("<" + evt.getMember().get().getUsername() + "> " + evt.getMessage().getContent().get() + "\n").getBytes());
            }
            return true;
        }).then().onErrorResume(t -> evt.getMessage().getChannel().flatMap(c -> c.createMessage("Error sending to factorio: " + t)).then());
    }

    @SuppressWarnings("null")
    public static <A, B, C> Function<Flux<A>, Flux<C>> flatZipWith(Flux<? extends B> b, BiFunction<A, B, Publisher<C>> combinator) {
        return in -> in.zipWith(b, combinator).flatMap(Function.identity());
    }
}
