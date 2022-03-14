package dev.pulsarfunction.dataproducer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.apache.pulsar.client.api.*;
import java.net.URL;
import java.util.UUID;
import ai.djl.Application;
import ai.djl.ModelException;
import ai.djl.engine.Engine;
import ai.djl.inference.Predictor;
import ai.djl.modality.nlp.qa.QAInput;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.training.util.ProgressBar;
import ai.djl.translate.TranslateException;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SpringBootApplication
public class DataproducerApplication implements CommandLineRunner { 

        private static Logger LOG = LoggerFactory.getLogger(DataproducerApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(DataproducerApplication.class, args);
	}

        public static String predict(String question, String paragraph) throws IOException, TranslateException, ModelException {
        question = "When did BBC Japan start broadcasting?";
        paragraph =
                "BBC Japan was a general entertainment Channel. "
                        + "Which operated between December 2004 and April 2006. "
                        + "It ceased operations after its Japanese distributor folded.";

        QAInput input = new QAInput(question, paragraph);
        LOG.error("Paragraph: {}", input.getParagraph());
        LOG.error("Question: {}", input.getQuestion());

        Criteria<QAInput, String> criteria =
                Criteria.builder()
                        .optApplication(Application.NLP.QUESTION_ANSWER)
                        .setTypes(QAInput.class, String.class)
                        .optFilter("backbone", "bert")
                        .optEngine(Engine.getDefaultEngineName())
                        .optProgress(new ProgressBar())
                        .build();

        try (ZooModel<QAInput, String> model = criteria.loadModel()) {
            try (Predictor<QAInput, String> predictor = model.newPredictor()) {
                return predictor.predict(input);
            }
        }
    }

    @Override
    public void run(String... args) {
        for (int i = 0; i < args.length; ++i) {
            LOG.error("args[{}]: {}", i, args[i]);
        }

	try {

	UUID uuidKey = UUID.randomUUID();
        String pulsarKey = uuidKey.toString();
        String OS = System.getProperty("os.name").toLowerCase();

	PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar://pulsar1:6650")
        .build();
	Producer<String> stringProducer = client.newProducer(Schema.STRING)
        .topic("persistent://public/default/springboottest1")
        .create();

 	String answer = DataproducerApplication.predict("question","paragraphs");
        LOG.error("answer:" + answer);

	MessageId msgId = stringProducer.newMessage().key(pulsarKey).value("Message:" + OS + ":" + pulsarKey).send();

	LOG.error(msgId + "," + OS + "," + pulsarKey);
	stringProducer.close();
	client.close();

        LOG.error("Available processors (cores): " +
                    Runtime.getRuntime().availableProcessors());

        LOG.error("Free memory (bytes): " +
                    Runtime.getRuntime().freeMemory());

            /* This will return Long.MAX_VALUE if there is no preset limit */
            long maxMemory = Runtime.getRuntime().maxMemory();

            /* Maximum amount of memory the JVM will attempt to use */
            LOG.error("Maximum memory (bytes): " +
                    (maxMemory == Long.MAX_VALUE ? "no limit" : maxMemory));

            /* Total memory currently available to the JVM */
            LOG.error("Total memory available to JVM (bytes): " +
                    Runtime.getRuntime().totalMemory());

	    }
	    catch(Throwable t) {
		    t.printStackTrace();
	    }
    }
}
