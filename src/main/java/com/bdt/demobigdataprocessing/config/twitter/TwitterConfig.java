package com.bdt.demobigdataprocessing.config.twitter;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.MessageChannel;
import twitter4j.Status;
import twitter4j.TwitterStream;

import java.util.Arrays;

@Slf4j
@Configuration
public class TwitterConfig {

  @Bean
  TwitterStreamFactory twitterStreamFactory() {
    return new TwitterStreamFactory();
  }

  @Bean
  TwitterStream twitterStream(TwitterStreamFactory twitterStreamFactory) {
    return twitterStreamFactory.createInstance();
  }

  @Bean
  MessageChannel outputChannel() {
    return MessageChannels.direct().get();
  }

  @Bean
  TwitterMessageProducer twitterMessageProducer(
      TwitterStream twitterStream, MessageChannel outputChannel) {

    TwitterMessageProducer twitterMessageProducer =
        new TwitterMessageProducer(twitterStream, outputChannel);

    twitterMessageProducer.setTerms(Arrays.asList("java", "microservices", "spring"));

    return twitterMessageProducer;
  }

  @Bean
  IntegrationFlow twitterFlow(MessageChannel outputChannel, KafkaTemplate<String, String> kafkaClient) {
    return IntegrationFlows.from(outputChannel)
        .transform(Status::getText)
        .handle(m -> {
          String msg = m.getPayload().toString();
          kafkaClient.send("quickstart-events", msg);
          log.info(msg);
        })
        .get();
  }

}
